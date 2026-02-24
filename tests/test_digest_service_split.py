import os

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.db import Base
from app.repositories import NewsRepository
from app.services.digest_service import DigestService


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "BOT_TOKEN": "x",
            "CHANNEL_ID": "@c",
            "ADMIN_USER_IDS": "1",
        }
    )


def _item(index: int, payload_size: int) -> str:
    text = "x" * payload_size
    return f"{index}. Новость {index}: {text} <a href='https://example.com/{index}'>Источник</a>"


def test_split_body_long_text_with_multiple_links() -> None:
    settings = _settings()
    service = DigestService(settings)
    service.TELEGRAM_MAX_CHARS = 260

    body = "\n\n".join(_item(i, 120) for i in range(1, 6))

    chunks = service._split_body(body)

    assert len(chunks) >= 3
    for chunk in chunks:
        assert len(chunk) <= service.TELEGRAM_MAX_CHARS
        assert service._has_balanced_anchor_tags(chunk)
        assert "<a href='https://example.com/" in chunk
        assert chunk.count("<a ") == chunk.count("</a>")


def test_split_body_when_single_item_near_limit() -> None:
    settings = _settings()
    service = DigestService(settings)
    service.TELEGRAM_MAX_CHARS = 220

    first = _item(1, 130)
    second = _item(2, 70)
    body = f"{first}\n\n{second}"

    chunks = service._split_body(body)

    assert len(chunks) == 2
    assert chunks[0] == first
    assert chunks[1] == second
    assert all(service._has_balanced_anchor_tags(chunk) for chunk in chunks)


def test_fit_chunk_to_budget_rolls_back_unbalanced_anchor_and_keeps_safe_text() -> None:
    settings = _settings()
    service = DigestService(settings)

    chunk = (
        "Короткое вступление перед ссылкой "
        "<a href='https://example.com/long'>"
        f"{'очень-длинный-текст-' * 40}"
        "</a>"
    )

    result = service._fit_chunk_to_budget(chunk, budget=80)

    assert result
    assert len(result) <= 80
    assert "Короткое вступление" in result
    assert service._has_balanced_anchor_tags(result)


@pytest.mark.asyncio
async def test_send_digest_messages_adds_continuation_headers() -> None:
    settings = _settings()
    service = DigestService(settings)
    service.TELEGRAM_MAX_CHARS = 220

    body = "\n\n".join(_item(i, 80) for i in range(1, 7))

    sent_texts: list[str] = []

    class _FakeBot:
        async def send_message(self, chat_id, text):  # noqa: ANN001
            sent_texts.append(text)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        repo = NewsRepository(session, timezone="UTC")
        result = await service._send_digest_messages(_FakeBot(), "Дайджест дня", body, news_repo=repo, digest_id=None)
    await engine.dispose()

    assert result["status"] == "success"
    assert result["total_chunks"] == len(sent_texts)
    assert result["total_chunks"] >= 2
    assert sent_texts[0].startswith("Дайджест дня\n\n")

    total_chunks = result["total_chunks"]
    for idx, message in enumerate(sent_texts[1:], start=2):
        assert message.startswith(f"Дайджест дня (продолжение {idx}/{total_chunks})\n\n")


@pytest.mark.asyncio
async def test_send_digest_messages_respects_total_limit_with_long_title_and_body() -> None:
    settings = _settings()
    service = DigestService(settings)
    service.TELEGRAM_MESSAGE_MAX = 250
    service.TELEGRAM_MAX_CHARS = 250
    service.DIGEST_TITLE_MAX_CHARS = 200

    long_title = "Очень длинный заголовок " * 30
    body = "\n\n".join(_item(i, 140) for i in range(1, 6))

    sent_texts: list[str] = []

    class _FakeBot:
        async def send_message(self, chat_id, text):  # noqa: ANN001
            sent_texts.append(text)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        repo = NewsRepository(session, timezone="UTC")
        result = await service._send_digest_messages(_FakeBot(), long_title, body, news_repo=repo, digest_id=None)
    await engine.dispose()

    assert result["status"] == "success"
    assert result["total_chunks"] == len(sent_texts)
    assert sent_texts
    for payload in sent_texts:
        assert len(payload) <= service.TELEGRAM_MESSAGE_MAX
        assert service._has_balanced_anchor_tags(payload)

    assert "…" in sent_texts[0].split("\n\n", maxsplit=1)[0]
