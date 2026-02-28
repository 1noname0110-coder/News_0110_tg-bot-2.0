import os
from datetime import date, datetime
from types import SimpleNamespace

import pytest
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.db import Base
from app.models import DailyStats, DeliveryAttempt, PublishedNews, RawNews, Source
from app.repositories import NewsRepository
from app.services.digest_service import DigestService
from app.services.pipeline import EvaluatedNewsItem


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "BOT_TOKEN": "x",
            "CHANNEL_ID": "@c",
            "ADMIN_USER_IDS": "1",
            "TIMEZONE": "UTC",
        }
    )


@pytest.mark.asyncio
@pytest.mark.smoke
async def test_e2e_pipeline_source_collect_filter_summarize_send_stats() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        source = Source(id=1, name="Main RSS", type="rss", url="https://example.com/rss", meta={})
        session.add(source)
        await session.commit()

        service = DigestService(_settings())

        collected_at = datetime(2026, 2, 20, 9, 0, 0)

        async def _fake_collect(_source):  # noqa: ANN001
            return [
                {
                    "source_id": 1,
                    "title": "Strategic partnership",
                    "summary": "accepted",
                    "url": "https://example.com/a",
                    "external_id": "a",
                    "published_at": collected_at,
                },
                {
                    "source_id": 1,
                    "title": "Celebrity gossip",
                    "summary": "reject",
                    "url": "https://example.com/b",
                    "external_id": "b",
                    "published_at": collected_at,
                },
            ]

        service.collector.collect_from_source = _fake_collect

        evaluate_calls = 0

        def _fake_evaluate(title, _summary, **_kwargs):  # noqa: ANN001
            nonlocal evaluate_calls
            evaluate_calls += 1
            if "Strategic" in title:
                return SimpleNamespace(accepted=True, reason="", score=8, topic="strategy", decision_trace=[], is_high_confidence=True)
            return SimpleNamespace(accepted=False, reason="noise", score=-1, topic="other", decision_trace=[], is_high_confidence=False)

        service.filter.evaluate = _fake_evaluate

        async def _fake_build_digest(period, accepted):  # noqa: ANN001
            assert period == "daily"
            assert len(accepted) == 1
            assert isinstance(accepted[0], EvaluatedNewsItem)
            assert accepted[0].filter_result.score == 8
            assert accepted[0].filter_result.topic == "strategy"
            return SimpleNamespace(
                title="Daily digest",
                body="• <a href='https://example.com/a'>Strategic partnership</a>",
                items_count=1,
                source_breakdown={"1": 1},
                topic_breakdown={"strategy": 1},
                quality_metrics={},
            )

        service.summarizer.build_digest = _fake_build_digest

        sent_messages: list[str] = []

        class _Bot:
            async def send_message(self, chat_id, text):  # noqa: ANN001
                assert chat_id == "@c"
                sent_messages.append(text)
                return None

        await service.collect_and_store(session)

        start_dt = datetime(2026, 2, 20, 0, 0, 0)
        end_dt = datetime(2026, 2, 21, 0, 0, 0)
        await service._publish_period(bot=_Bot(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)

        repo = NewsRepository(session, timezone="UTC")
        stats = await repo.compute_daily_stats(date(2026, 2, 20))

        raws = list((await session.execute(select(RawNews).order_by(RawNews.external_id.asc()))).scalars().all())
        published = list((await session.execute(select(PublishedNews).order_by(PublishedNews.id.asc()))).scalars().all())

        assert len(raws) == 2
        assert evaluate_calls == 2
        assert len(sent_messages) == 1
        assert len(published) == 1
        assert published[0].items_count == 1
        assert published[0].quality_metrics["fetched_from_db"] == 2
        assert published[0].quality_metrics["accepted_total"] == 1
        assert published[0].quality_metrics["rejected_total"] == 1
        assert published[0].quality_metrics["rejection_reasons"] == {"noise": 1}
        assert stats.published_count == 0
        assert stats.rejected_count == 0
        assert stats.source_usage == {"1": 2}

    await engine.dispose()


@pytest.mark.asyncio
async def test_send_digest_messages_fallback_for_retry_after_forbidden_and_bad_request() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        service = DigestService(_settings())
        service.SEND_RETRY_ATTEMPTS = 3
        service.SEND_RETRY_DELAY_SECONDS = 0

        async def _no_sleep(_seconds: int) -> None:
            return None

        import app.services.digest_service as digest_module

        original_sleep = digest_module.asyncio.sleep
        digest_module.asyncio.sleep = _no_sleep

        class _Bot:
            def __init__(self):
                self.calls = 0

            async def send_message(self, chat_id, text):  # noqa: ANN001
                self.calls += 1
                if self.calls == 1:
                    raise TelegramRetryAfter(method="sendMessage", message="flood", retry_after=0)
                if self.calls == 2:
                    raise TelegramForbiddenError(method="sendMessage", message="forbidden")
                raise TelegramBadRequest(method="sendMessage", message="bad request")

        try:
            repo = NewsRepository(session, timezone="UTC")
            result = await service._send_digest_messages(_Bot(), "digest", "x " * 6000, news_repo=repo, digest_id=300)
        finally:
            digest_module.asyncio.sleep = original_sleep

        attempts = list((await session.execute(select(DeliveryAttempt).order_by(DeliveryAttempt.id.asc()))).scalars().all())

        assert result["status"] == "failed"
        assert result["total_chunks"] >= 2
        assert any(a.status == "retry" and a.error_type == "TelegramRetryAfter" for a in attempts)
        assert any(a.error_type == "TelegramForbiddenError" and a.status == "failed" for a in attempts)
        assert any(a.error_type == "TelegramBadRequest" and a.status == "failed" for a in attempts)

    await engine.dispose()


@pytest.mark.asyncio
async def test_publish_period_idempotency_blocks_second_publication_for_same_period() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        session.add(Source(id=1, name="Src", type="rss", url="https://example.com/rss", meta={}))
        session.add(
            RawNews(
                source_id=1,
                title="r1",
                summary="s",
                url="https://example.com/1",
                external_id="r1",
                published_at=datetime(2026, 1, 1, 10, 0, 0),
            )
        )
        await session.commit()

        service = DigestService(_settings())
        service.filter.evaluate = lambda _title, _summary, **_kwargs: SimpleNamespace(accepted=True, reason="", score=8, topic="general", decision_trace=[], is_high_confidence=True)

        async def _fake_build_digest(_period, _accepted):  # noqa: ANN001
            return SimpleNamespace(
                title="digest",
                body="body",
                items_count=1,
                source_breakdown={"1": 1},
                topic_breakdown={"general": 1},
                quality_metrics={},
            )

        sends = 0

        async def _fake_send(_bot, _title, _body, **_kwargs):  # noqa: ANN001
            nonlocal sends
            sends += 1
            return {"status": "success", "total_chunks": 1, "sent_chunks": 1, "failed_chunks": []}

        service.summarizer.build_digest = _fake_build_digest
        service._send_digest_messages = _fake_send

        start_dt = datetime(2026, 1, 1, 0, 0, 0)
        end_dt = datetime(2026, 1, 2, 0, 0, 0)

        repo = NewsRepository(session, timezone="UTC")
        assert await repo.is_period_already_published("daily", start_dt, end_dt) is False

        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)
        assert await repo.is_period_already_published("daily", start_dt, end_dt) is True

        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)

        published = list((await session.execute(select(PublishedNews).order_by(PublishedNews.id.asc()))).scalars().all())

        assert sends == 1
        assert len(published) == 1

    await engine.dispose()


def test_split_body_keeps_html_links_balanced_in_long_messages() -> None:
    service = DigestService(_settings())
    service.TELEGRAM_MAX_CHARS = 120

    link_line = "• <a href='https://example.com/very/long/path'>Очень длинная ссылка</a> с пояснением"
    body = "\n".join([link_line for _ in range(20)])

    chunks = service._split_body(body)

    assert len(chunks) > 1
    assert all("<a" in chunk for chunk in chunks)
    assert all(service._has_balanced_anchor_tags(chunk) for chunk in chunks)
    assert all(len(chunk) <= service.TELEGRAM_MAX_CHARS for chunk in chunks)
