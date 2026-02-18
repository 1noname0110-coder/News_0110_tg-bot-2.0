from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta
from typing import Iterable

from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DailyStats, PublishedNews, RawNews, RejectedNews, Source, WeeklyStats


class SourceRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def list_active(self) -> list[Source]:
        result = await self.session.execute(select(Source).where(Source.is_active.is_(True)))
        return list(result.scalars().all())

    async def list_all(self) -> list[Source]:
        result = await self.session.execute(select(Source).order_by(Source.id.asc()))
        return list(result.scalars().all())

    async def create(self, source_type: str, name: str, url: str, meta: dict | None = None) -> Source:
        source = Source(type=source_type, name=name, url=url, meta=meta or {})
        self.session.add(source)
        await self.session.commit()
        await self.session.refresh(source)
        return source

    async def remove(self, source_id: int) -> bool:
        source = await self.session.get(Source, source_id)
        if not source:
            return False
        await self.session.delete(source)
        await self.session.commit()
        return True


class NewsRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def add_raw_news(self, items: Iterable[dict]) -> list[RawNews]:
        stored: list[RawNews] = []
        for item in items:
            entry = RawNews(**item)
            try:
                async with self.session.begin_nested():
                    self.session.add(entry)
                    await self.session.flush()
                    stored.append(entry)
            except IntegrityError:
                continue
        await self.session.commit()
        return stored

    async def fetch_period_news(self, start_dt: datetime, end_dt: datetime) -> list[RawNews]:
        result = await self.session.execute(
            select(RawNews).where(and_(RawNews.published_at >= start_dt, RawNews.published_at <= end_dt))
        )
        return list(result.scalars().all())

    async def reject(self, raw_news_id: int, source_id: int, reason: str) -> None:
        self.session.add(RejectedNews(raw_news_id=raw_news_id, source_id=source_id, reason=reason))
        await self.session.commit()

    async def publish_digest(
        self,
        period_type: str,
        period_start: datetime,
        period_end: datetime,
        title: str,
        body: str,
        items_count: int,
        source_breakdown: dict,
        topic_breakdown: dict,
        quality_metrics: dict,
    ) -> PublishedNews:
        row = PublishedNews(
            period_type=period_type,
            period_start=period_start,
            period_end=period_end,
            title=title,
            body=body,
            items_count=items_count,
            source_breakdown=source_breakdown,
            topic_breakdown=topic_breakdown,
            quality_metrics=quality_metrics,
        )
        self.session.add(row)
        await self.session.commit()
        await self.session.refresh(row)
        return row

    async def compute_daily_stats(self, stat_date: date) -> DailyStats:
        start = datetime.combine(stat_date, datetime.min.time())
        end = start + timedelta(days=1)

        raws = await self.fetch_period_news(start, end)
        raw_by_source = Counter(str(r.source_id) for r in raws)

        rej_q = await self.session.execute(
            select(RejectedNews).where(and_(RejectedNews.rejected_at >= start, RejectedNews.rejected_at < end))
        )
        rejected_rows = list(rej_q.scalars().all())
        reject_by_source = Counter(str(r.source_id) for r in rejected_rows)

        pub_q = await self.session.execute(
            select(PublishedNews).where(and_(PublishedNews.published_at >= start, PublishedNews.published_at < end))
        )
        published_rows = list(pub_q.scalars().all())

        quality = self._aggregate_quality(published_rows, len(raws), len(rejected_rows))

        stats = await self.session.scalar(select(DailyStats).where(DailyStats.stat_date == stat_date))
        if not stats:
            stats = DailyStats(stat_date=stat_date)
            self.session.add(stats)

        stats.published_count = len(published_rows)
        stats.source_usage = dict(raw_by_source)
        stats.rejected_count = len(rejected_rows)
        stats.rejection_breakdown = dict(reject_by_source)
        stats.quality_metrics = quality

        await self.session.commit()
        await self.session.refresh(stats)
        return stats

    async def compute_weekly_stats(self, week_start: date) -> WeeklyStats:
        start = datetime.combine(week_start, datetime.min.time())
        end = start + timedelta(days=7)

        raws = await self.fetch_period_news(start, end)
        raw_by_source = Counter(str(r.source_id) for r in raws)

        rej_q = await self.session.execute(
            select(RejectedNews).where(and_(RejectedNews.rejected_at >= start, RejectedNews.rejected_at < end))
        )
        rejected_rows = list(rej_q.scalars().all())
        reject_by_source = Counter(str(r.source_id) for r in rejected_rows)

        pub_q = await self.session.execute(
            select(PublishedNews).where(and_(PublishedNews.published_at >= start, PublishedNews.published_at < end))
        )
        published_rows = list(pub_q.scalars().all())

        quality = self._aggregate_quality(published_rows, len(raws), len(rejected_rows))

        stats = await self.session.scalar(select(WeeklyStats).where(WeeklyStats.week_start == week_start))
        if not stats:
            stats = WeeklyStats(week_start=week_start)
            self.session.add(stats)

        stats.published_count = len(published_rows)
        stats.source_usage = dict(raw_by_source)
        stats.rejected_count = len(rejected_rows)
        stats.rejection_breakdown = dict(reject_by_source)
        stats.quality_metrics = quality

        await self.session.commit()
        await self.session.refresh(stats)
        return stats

    @staticmethod
    def _aggregate_quality(published_rows: list[PublishedNews], raw_count: int, rejected_count: int) -> dict:
        selected = 0
        deduplicated = 0
        duplicates_removed = 0
        topic_distribution: Counter[str] = Counter()
        rejection_reasons: Counter[str] = Counter()

        for row in published_rows:
            qm = row.quality_metrics or {}
            selected += int(qm.get("selected", 0))
            deduplicated += int(qm.get("deduplicated", 0))
            duplicates_removed += int(qm.get("duplicates_removed", 0))
            for topic, count in (row.topic_breakdown or {}).items():
                topic_distribution[str(topic)] += int(count)
            for reason, count in (qm.get("rejection_reasons", {}) or {}).items():
                rejection_reasons[str(reason)] += int(count)

        acceptance_rate = 0.0 if raw_count == 0 else round((raw_count - rejected_count) / raw_count, 4)
        return {
            "raw_total": raw_count,
            "rejected_total": rejected_count,
            "selected_total": selected,
            "deduplicated_total": deduplicated,
            "duplicates_removed_total": duplicates_removed,
            "acceptance_rate": acceptance_rate,
            "topic_distribution": dict(topic_distribution),
            "rejection_reasons": dict(rejection_reasons),
        }
