import os
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.db import Base
from app.models import RawNews, Source
from app.services.digest_service import DigestService


def _settings() -> Settings:
    return Settings.model_validate({"BOT_TOKEN": "x", "CHANNEL_ID": "@c", "ADMIN_USER_IDS": "1", "TIMEZONE": "UTC"})


@pytest.mark.smoke
@pytest.mark.asyncio
async def test_smoke_publish_period_success() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        session.add(Source(id=1, name="smoke", type="rss", url="https://example.com/rss", meta={}))
        session.add(
            RawNews(
                source_id=1,
                title="headline",
                summary="summary",
                url="https://example.com/1",
                external_id="smoke-1",
                published_at=datetime(2026, 1, 1, 10, 0, 0),
            )
        )
        await session.commit()

        service = DigestService(_settings())
        service.filter.evaluate = lambda _title, _summary: SimpleNamespace(accepted=True, reason="")

        async def _build_digest(_period, _accepted):  # noqa: ANN001
            return SimpleNamespace(
                title="digest",
                body="body",
                items_count=1,
                source_breakdown={"1": 1},
                topic_breakdown={"general": 1},
                quality_metrics={},
            )

        async def _send(*_args, **_kwargs):  # noqa: ANN001
            return {"status": "success", "total_chunks": 1, "sent_chunks": 1, "failed_chunks": []}

        service.summarizer.build_digest = _build_digest
        service._send_digest_messages = _send

        await service._publish_period(
            bot=object(),
            session=session,
            period_type="daily",
            start_dt=datetime(2026, 1, 1, 0, 0, 0),
            end_dt=datetime(2026, 1, 2, 0, 0, 0),
        )

    await engine.dispose()


@pytest.mark.smoke
def test_smoke_chunking_balanced_links() -> None:
    service = DigestService(_settings())
    service.TELEGRAM_MAX_CHARS = 100
    body = "\n".join(["• <a href='https://example.com'>link</a> detail" for _ in range(20)])
    chunks = service._split_body(body)
    assert len(chunks) > 1
    assert all(service._has_balanced_anchor_tags(chunk) for chunk in chunks)
