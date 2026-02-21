import asyncio
import os
from unittest.mock import AsyncMock, MagicMock

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from sqlalchemy.exc import IntegrityError

from datetime import datetime

from app.models import PublishedNews
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
