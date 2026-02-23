import os
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.db import Base
from app.models import PublishedNews, RawNews, RejectedNews
from app.services.digest_service import DigestService


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "BOT_TOKEN": "x",
            "CHANNEL_ID": "@c",
            "ADMIN_USER_IDS": "1",
        }
    )


@pytest.mark.asyncio
async def test_publish_period_does_not_duplicate_rejected_news_on_rerun() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        session.add_all(
            [
                RawNews(
                    source_id=1,
                    title="r1",
                    summary="s",
                    url="https://example.com/1",
                    external_id="r1",
                    published_at=datetime(2026, 1, 1, 10, 0, 0),
                ),
                RawNews(
                    source_id=1,
                    title="r2",
                    summary="s",
                    url="https://example.com/2",
                    external_id="r2",
                    published_at=datetime(2026, 1, 1, 11, 0, 0),
                ),
            ]
        )
        await session.commit()

        service = DigestService(_settings())
        service.filter.evaluate = lambda _title, _summary: SimpleNamespace(accepted=False, reason="noise")
        async def _fake_build_digest(_period, _accepted):  # noqa: ANN001
            return SimpleNamespace(
                title="digest",
                body="body",
                items_count=0,
                source_breakdown={},
                topic_breakdown={},
                quality_metrics={},
            )

        async def _fake_send_digest_messages(_bot, _title, _body):  # noqa: ANN001
            return {"status": "success", "total_chunks": 1, "sent_chunks": 1, "failed_chunks": []}

        service.summarizer.build_digest = _fake_build_digest
        service._send_digest_messages = _fake_send_digest_messages

        start_dt = datetime(2026, 1, 1, 0, 0, 0)
        end_dt = datetime(2026, 1, 2, 0, 0, 0)

        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)
        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)

        rejected = list((await session.execute(select(RejectedNews).order_by(RejectedNews.raw_news_id.asc()))).scalars().all())

        assert [row.raw_news_id for row in rejected] == [1, 2]
        assert [row.reason for row in rejected] == ["noise", "noise"]

    await engine.dispose()


@pytest.mark.asyncio
async def test_publish_period_skips_duplicate_period_unless_manual_republish() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
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
        service.filter.evaluate = lambda _title, _summary: SimpleNamespace(accepted=True, reason="")

        sent_payloads: list[tuple[str, str]] = []

        async def _fake_build_digest(_period, _accepted):  # noqa: ANN001
            return SimpleNamespace(
                title="digest",
                body="body",
                items_count=1,
                source_breakdown={"1": 1},
                topic_breakdown={"general": 1},
                quality_metrics={},
            )

        async def _fake_send_digest_messages(_bot, _title, _body):  # noqa: ANN001
            sent_payloads.append((_title, _body))
            return {"status": "success", "total_chunks": 1, "sent_chunks": 1, "failed_chunks": []}

        service.summarizer.build_digest = _fake_build_digest
        service._send_digest_messages = _fake_send_digest_messages

        start_dt = datetime(2026, 1, 1, 0, 0, 0)
        end_dt = datetime(2026, 1, 2, 0, 0, 0)

        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)
        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)

        published = list((await session.execute(select(PublishedNews).order_by(PublishedNews.id.asc()))).scalars().all())

        assert len(sent_payloads) == 1
        assert len(published) == 1
        assert published[0].period_type == "daily"
        assert published[0].period_start == start_dt
        assert published[0].period_end == end_dt

        await service.republish_period(
            bot=object(),
            session=session,
            period_type="daily",
            start_dt=start_dt,
            end_dt=end_dt,
        )

        republished = list((await session.execute(select(PublishedNews).order_by(PublishedNews.id.asc()))).scalars().all())

        assert len(sent_payloads) == 2
        assert len(republished) == 2

    await engine.dispose()
