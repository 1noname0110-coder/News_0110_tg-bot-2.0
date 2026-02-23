import asyncio
import os
from collections import Counter
from unittest.mock import AsyncMock, MagicMock

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from sqlalchemy.exc import IntegrityError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from datetime import date, datetime, timedelta

from app.db import Base
from app.models import PublishedNews, RawNews, RejectedNews
from app.repositories import NewsRepository, SourceRepository, normalize_http_url


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




def test_source_create_returns_none_on_invalid_url() -> None:
    session = MagicMock()
    session.add = MagicMock()
    session.commit = AsyncMock()

    repo = SourceRepository(session)
    result = asyncio.run(repo.create(source_type="rss", name="invalid", url="ftp://example.com"))

    assert result is None
    session.add.assert_not_called()
    session.commit.assert_not_awaited()


def test_source_create_normalizes_url_before_save() -> None:
    session = MagicMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.refresh = AsyncMock()

    repo = SourceRepository(session)
    result = asyncio.run(repo.create(source_type="rss", name="ok", url=" HTTPS://Example.COM/path "))

    assert result is not None
    added_source = session.add.call_args.args[0]
    assert added_source.url == "https://example.com/path"


def test_normalize_http_url_removes_fragment() -> None:
    assert normalize_http_url("https://example.com/path#section") == "https://example.com/path"

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

    metrics = NewsRepository._aggregate_quality(
        rows,
        raw_count=8,
        rejected_count=2,
        rejected_reason_counts=Counter({"low_relevance": 3}),
    )

    assert metrics["fetched_from_db_total"] == 8
    assert metrics["rejected_by_filter_total"] == 2
    assert metrics["removed_as_duplicates_total"] == 2
    assert metrics["removed_by_topic_limit_total"] == 1
    assert metrics["published_items_total"] == 3
    assert metrics["rejection_reasons"] == {"low_relevance": 3}


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


def test_compute_daily_stats_uses_local_timezone_boundaries() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        day_start_utc = datetime(2026, 1, 7, 14, 0, 0)
        day_end_utc = datetime(2026, 1, 8, 14, 0, 0)

        async with Session() as session:
            session.add_all(
                [
                    RawNews(source_id=1, title="before", summary="s", url="https://example.com/before", external_id="before", published_at=day_start_utc - timedelta(seconds=1)),
                    RawNews(source_id=1, title="start", summary="s", url="https://example.com/start", external_id="start", published_at=day_start_utc),
                    RawNews(source_id=2, title="inside", summary="s", url="https://example.com/inside", external_id="inside", published_at=day_end_utc - timedelta(seconds=1)),
                    RawNews(source_id=2, title="after", summary="s", url="https://example.com/after", external_id="after", published_at=day_end_utc),
                ]
            )
            session.add_all(
                [
                    RejectedNews(raw_news_id=101, source_id=1, reason="before", rejected_at=day_start_utc - timedelta(seconds=1)),
                    RejectedNews(raw_news_id=102, source_id=1, reason="in", rejected_at=day_start_utc),
                    RejectedNews(raw_news_id=103, source_id=2, reason="in", rejected_at=day_end_utc - timedelta(seconds=1)),
                    RejectedNews(raw_news_id=104, source_id=2, reason="after", rejected_at=day_end_utc),
                ]
            )
            session.add_all(
                [
                    PublishedNews(period_type="daily", period_start=day_start_utc, period_end=day_end_utc, title="before", body="b", items_count=1, published_at=day_start_utc - timedelta(seconds=1)),
                    PublishedNews(period_type="daily", period_start=day_start_utc, period_end=day_end_utc, title="start", body="b", items_count=1, published_at=day_start_utc),
                    PublishedNews(period_type="daily", period_start=day_start_utc, period_end=day_end_utc, title="inside", body="b", items_count=1, published_at=day_end_utc - timedelta(seconds=1)),
                    PublishedNews(period_type="daily", period_start=day_start_utc, period_end=day_end_utc, title="after", body="b", items_count=1, published_at=day_end_utc),
                ]
            )
            await session.commit()

            repo = NewsRepository(session, timezone="Asia/Vladivostok")
            stats = await repo.compute_daily_stats(date(2026, 1, 8))

            assert stats.published_count == 2
            assert stats.rejected_count == 2
            assert stats.source_usage == {"1": 1, "2": 1}
            assert stats.rejection_breakdown == {"1": 1, "2": 1}

        await engine.dispose()

    asyncio.run(_run())


def test_compute_weekly_stats_uses_local_timezone_boundaries() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        week_start_utc = datetime(2026, 1, 4, 14, 0, 0)
        week_end_utc = datetime(2026, 1, 11, 14, 0, 0)

        async with Session() as session:
            session.add_all(
                [
                    RawNews(source_id=1, title="before-week", summary="s", url="https://example.com/before-week", external_id="before-week", published_at=week_start_utc - timedelta(seconds=1)),
                    RawNews(source_id=1, title="week-start", summary="s", url="https://example.com/week-start", external_id="week-start", published_at=week_start_utc),
                    RawNews(source_id=2, title="week-end-inside", summary="s", url="https://example.com/week-end-inside", external_id="week-end-inside", published_at=week_end_utc - timedelta(seconds=1)),
                    RawNews(source_id=2, title="after-week", summary="s", url="https://example.com/after-week", external_id="after-week", published_at=week_end_utc),
                ]
            )
            session.add_all(
                [
                    RejectedNews(raw_news_id=201, source_id=1, reason="before", rejected_at=week_start_utc - timedelta(seconds=1)),
                    RejectedNews(raw_news_id=202, source_id=1, reason="in", rejected_at=week_start_utc),
                    RejectedNews(raw_news_id=203, source_id=2, reason="in", rejected_at=week_end_utc - timedelta(seconds=1)),
                    RejectedNews(raw_news_id=204, source_id=2, reason="after", rejected_at=week_end_utc),
                ]
            )
            session.add_all(
                [
                    PublishedNews(period_type="weekly", period_start=week_start_utc, period_end=week_end_utc, title="before", body="b", items_count=1, published_at=week_start_utc - timedelta(seconds=1)),
                    PublishedNews(period_type="weekly", period_start=week_start_utc, period_end=week_end_utc, title="start", body="b", items_count=1, published_at=week_start_utc),
                    PublishedNews(period_type="weekly", period_start=week_start_utc, period_end=week_end_utc, title="inside", body="b", items_count=1, published_at=week_end_utc - timedelta(seconds=1)),
                    PublishedNews(period_type="weekly", period_start=week_start_utc, period_end=week_end_utc, title="after", body="b", items_count=1, published_at=week_end_utc),
                ]
            )
            await session.commit()

            repo = NewsRepository(session, timezone="Asia/Vladivostok")
            stats = await repo.compute_weekly_stats(date(2026, 1, 5))

            assert stats.published_count == 2
            assert stats.rejected_count == 2
            assert stats.source_usage == {"1": 1, "2": 1}
            assert stats.rejection_breakdown == {"1": 1, "2": 1}

        await engine.dispose()

    asyncio.run(_run())


def test_compute_daily_stats_includes_rejection_reasons_without_publications() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        day_start_utc = datetime(2026, 1, 7, 14, 0, 0)

        async with Session() as session:
            session.add_all(
                [
                    RejectedNews(raw_news_id=301, source_id=1, reason="noise", rejected_at=day_start_utc + timedelta(hours=1)),
                    RejectedNews(raw_news_id=302, source_id=2, reason="low_relevance", rejected_at=day_start_utc + timedelta(hours=2)),
                    RejectedNews(raw_news_id=303, source_id=2, reason="noise", rejected_at=day_start_utc + timedelta(hours=3)),
                ]
            )
            await session.commit()

            repo = NewsRepository(session, timezone="Asia/Vladivostok")
            stats = await repo.compute_daily_stats(date(2026, 1, 8))

            assert stats.published_count == 0
            assert stats.rejected_count == 3
            assert stats.quality_metrics["rejection_reasons"] == {"noise": 2, "low_relevance": 1}

        await engine.dispose()

    asyncio.run(_run())



def test_compute_daily_stats_rejection_reasons_use_rejected_news_as_single_source() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        day_start_utc = datetime(2026, 1, 7, 14, 0, 0)
        day_end_utc = datetime(2026, 1, 8, 14, 0, 0)

        async with Session() as session:
            session.add_all(
                [
                    RejectedNews(raw_news_id=501, source_id=1, reason="noise", rejected_at=day_start_utc + timedelta(hours=1)),
                    RejectedNews(raw_news_id=502, source_id=1, reason="noise", rejected_at=day_start_utc + timedelta(hours=2)),
                    RejectedNews(raw_news_id=503, source_id=2, reason="low_relevance", rejected_at=day_start_utc + timedelta(hours=3)),
                ]
            )
            session.add(
                PublishedNews(
                    period_type="daily",
                    period_start=day_start_utc,
                    period_end=day_end_utc,
                    title="digest",
                    body="body",
                    items_count=1,
                    published_at=day_start_utc + timedelta(hours=4),
                    quality_metrics={"rejection_reasons": {"noise": 100, "spam": 50}},
                )
            )
            await session.commit()

            repo = NewsRepository(session, timezone="Asia/Vladivostok")
            stats = await repo.compute_daily_stats(date(2026, 1, 8))

            assert stats.rejected_count == 3
            assert stats.quality_metrics["rejection_reasons"] == {"noise": 2, "low_relevance": 1}
            assert sum(stats.quality_metrics["rejection_reasons"].values()) == stats.rejected_count

        await engine.dispose()

    asyncio.run(_run())

def test_compute_weekly_stats_includes_rejection_reasons_without_publications() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        week_start_utc = datetime(2026, 1, 4, 14, 0, 0)

        async with Session() as session:
            session.add_all(
                [
                    RejectedNews(raw_news_id=401, source_id=1, reason="spam", rejected_at=week_start_utc + timedelta(days=1)),
                    RejectedNews(raw_news_id=402, source_id=1, reason="spam", rejected_at=week_start_utc + timedelta(days=2)),
                    RejectedNews(raw_news_id=403, source_id=2, reason="duplicate", rejected_at=week_start_utc + timedelta(days=3)),
                ]
            )
            await session.commit()

            repo = NewsRepository(session, timezone="Asia/Vladivostok")
            stats = await repo.compute_weekly_stats(date(2026, 1, 5))

            assert stats.published_count == 0
            assert stats.rejected_count == 3
            assert stats.quality_metrics["rejection_reasons"] == {"spam": 2, "duplicate": 1}

        await engine.dispose()

    asyncio.run(_run())


def test_reject_many_does_not_create_duplicates_across_repeated_calls() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        async with Session() as session:
            repo = NewsRepository(session)

            inserted_first = await repo.reject_many([(11, 1, "noise"), (12, 1, "noise"), (11, 1, "duplicate")])
            inserted_second = await repo.reject_many([(11, 1, "again"), (12, 1, "again")])

            rows = list((await session.execute(select(RejectedNews).order_by(RejectedNews.raw_news_id.asc()))).scalars().all())

            assert inserted_first == 2
            assert inserted_second == 0
            assert [row.raw_news_id for row in rows] == [11, 12]

        await engine.dispose()

    asyncio.run(_run())
