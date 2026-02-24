import asyncio
import os
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.db import Base
from app.models import PublishedNews, RawNews, RejectedNews, Source


async def _plan_rows(session, sql: str, **params):
    res = await session.execute(text(f"EXPLAIN QUERY PLAN {sql}"), params)
    return [str(row[-1]) for row in res.all()]


def test_query_plans_use_expected_indexes() -> None:
    async def _run() -> None:
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        Session = async_sessionmaker(engine, expire_on_commit=False)

        async with Session() as session:
            src = Source(name="s1", type="rss", url="https://example.com")
            session.add(src)
            await session.flush()

            session.add_all(
                [
                    RawNews(
                        source_id=src.id,
                        title="t1",
                        summary="s",
                        url="https://example.com/1",
                        external_id="e1",
                        published_at=datetime(2024, 1, 1, 10, 0, 0),
                    ),
                    PublishedNews(
                        period_type="daily",
                        period_start=datetime(2024, 1, 1, 0, 0, 0),
                        period_end=datetime(2024, 1, 2, 0, 0, 0),
                        title="d",
                        body="b",
                        items_count=1,
                    ),
                ]
            )
            await session.flush()

            rn = RejectedNews(raw_news_id=1, source_id=src.id, reason="low_relevance")
            session.add(rn)
            await session.commit()

            raw_plan = await _plan_rows(
                session,
                """
                SELECT id FROM raw_news
                WHERE published_at >= :start_dt AND published_at < :end_dt
                ORDER BY published_at DESC, id DESC
                """,
                start_dt=datetime(2024, 1, 1, 0, 0, 0),
                end_dt=datetime(2024, 1, 2, 0, 0, 0),
            )
            assert any("ix_raw_news_published_at" in row for row in raw_plan)

            rejected_plan = await _plan_rows(
                session,
                """
                SELECT id FROM rejected_news
                WHERE rejected_at >= :start_dt AND rejected_at < :end_dt
                """,
                start_dt=datetime(2024, 1, 1, 0, 0, 0),
                end_dt=datetime(2024, 1, 2, 0, 0, 0),
            )
            assert any("ix_rejected_news_rejected_at" in row for row in rejected_plan)

            period_plan = await _plan_rows(
                session,
                """
                SELECT id FROM published_news
                WHERE period_type = :period_type
                  AND period_start = :period_start
                  AND period_end = :period_end
                """,
                period_type="daily",
                period_start=datetime(2024, 1, 1, 0, 0, 0),
                period_end=datetime(2024, 1, 2, 0, 0, 0),
            )
            assert any("ix_published_news_period" in row for row in period_plan)

        await engine.dispose()

    asyncio.run(_run())
