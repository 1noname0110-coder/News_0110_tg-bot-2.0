import asyncio
import os
from unittest.mock import AsyncMock, MagicMock

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from datetime import datetime

from app.db import Base
from app.models import PublishedNews, RawNews, RejectedNews
from app.repositories import NewsRepository, SourceRepository


def test_source_create_returns_none_on_duplicate_name() -> None:
    session = MagicMock()
    session.add = MagicMock()
    session.commit = AsyncMock(side_effect=IntegrityError("insert", {"name": "dup"}, Exception("uniq")))
    session.rollback = AsyncMock()

    repo = SourceRepository(session)
    result = asyncio.run(repo.create(source_type="rss", name="dup", url="https://example.com"))

    assert result is None
    session.rollback.assert_awaited_once()


def test_source_create_returns_none_on_invalid_source_type() -> None:
    session = MagicMock()
    session.add = MagicMock()
    session.commit = AsyncMock()

    repo = SourceRepository(session)
    result = asyncio.run(repo.create(source_type="telegram", name="invalid", url="https://example.com"))

    assert result is None
    session.add.assert_not_called()
    session.commit.assert_not_awaited()


def test_aggregate_quality_sums_new_funnel_metrics() -> None:
    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 1, 1, 23, 59, 59)

    rows = [
        PublishedNews(
            period_type="daily",
            period_start=start,
            period_end=end,
            title="t1",
            body="b1",
            items_count=2,
            topic_breakdown={"economy": 2},
            quality_metrics={
                "selected": 2,
                "deduplicated": 4,
                "duplicates_removed": 1,
                "fetched_from_db": 5,
                "rejected_by_filter": 1,
                "removed_as_duplicates": 1,
                "removed_by_topic_limit": 1,
                "published_items": 2,
                "rejection_reasons": {"low_relevance": 1},
            },
        ),
        PublishedNews(
            period_type="daily",
            period_start=start,
            period_end=end,
            title="t2",
            body="b2",
            items_count=1,
            topic_breakdown={"politics": 1},
            quality_metrics={
                "selected": 1,
                "deduplicated": 2,
                "duplicates_removed": 1,
                "fetched_from_db": 3,
                "rejected_by_filter": 1,
                "removed_as_duplicates": 1,
                "removed_by_topic_limit": 0,
                "published_items": 1,
                "rejection_reasons": {"noise": 1},
            },
        ),
    ]

    metrics = NewsRepository._aggregate_quality(rows, raw_count=8, rejected_count=2)

    assert metrics["fetched_from_db_total"] == 8
    assert metrics["rejected_by_filter_total"] == 2
    assert metrics["removed_as_duplicates_total"] == 2
    assert metrics["removed_by_topic_limit_total"] == 1
    assert metrics["published_items_total"] == 3
    assert metrics["rejection_reasons"] == {"low_relevance": 1, "noise": 1}


def test_aggregate_quality_uses_legacy_quality_keys_as_fallback() -> None:
    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 1, 1, 23, 59, 59)

    rows = [
        PublishedNews(
            period_type="daily",
            period_start=start,
            period_end=end,
            title="legacy",
            body="legacy",
            items_count=2,
            quality_metrics={
                "raw_total": 6,
                "rejected_total": 2,
                "selected": 3,
                "duplicates_removed": 1,
            },
        )
    ]

    metrics = NewsRepository._aggregate_quality(rows, raw_count=6, rejected_count=2)

    assert metrics["fetched_from_db_total"] == 6
    assert metrics["rejected_by_filter_total"] == 2
    assert metrics["removed_as_duplicates_total"] == 1
    assert metrics["published_items_total"] == 3


def test_fetch_period_news_uses_exclusive_end_boundary() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        period_start = datetime(2024, 1, 1, 10, 0, 0)
        period_end = datetime(2024, 1, 1, 11, 0, 0)
        next_period_end = datetime(2024, 1, 1, 12, 0, 0)

        async with Session() as session:
            session.add_all(
                [
                    RawNews(
                        source_id=1,
                        title="in current period",
                        summary="s",
                        url="https://example.com/1",
                        external_id="n1",
                        published_at=datetime(2024, 1, 1, 10, 30, 0),
                    ),
                    RawNews(
                        source_id=1,
                        title="exactly at end",
                        summary="s",
                        url="https://example.com/2",
                        external_id="n2",
                        published_at=period_end,
                    ),
                ]
            )
            await session.commit()

            repo = NewsRepository(session)
            current_period = await repo.fetch_period_news(period_start, period_end)
            next_period = await repo.fetch_period_news(period_end, next_period_end)

            assert [item.external_id for item in current_period] == ["n1"]
            assert [item.external_id for item in next_period] == ["n2"]

        await engine.dispose()

    asyncio.run(_run())


def test_reject_does_not_create_duplicates_for_same_raw_news_id() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        async with Session() as session:
            repo = NewsRepository(session)

            await repo.reject(raw_news_id=10, source_id=1, reason="low_relevance")
            await repo.reject(raw_news_id=10, source_id=1, reason="duplicate")

            rows = list((await session.execute(select(RejectedNews).where(RejectedNews.raw_news_id == 10))).scalars().all())

            assert len(rows) == 1
            assert rows[0].reason == "low_relevance"

        await engine.dispose()

    asyncio.run(_run())
