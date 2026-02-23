from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Iterable
from urllib.parse import SplitResult, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import DailyStats, PublishedNews, RawNews, RejectedNews, Source, WeeklyStats

ALLOWED_SOURCE_TYPES = {"rss", "site", "api"}


class SourceCreateStatus(str, Enum):
    CREATED = "created"
    INVALID_SOURCE_TYPE = "invalid_source_type"
    INVALID_URL = "invalid_url"
    DUPLICATE_NAME = "duplicate_name"
    DB_ERROR = "db_error"


@dataclass(slots=True)
class SourceCreateResult:
    status: SourceCreateStatus
    source: Source | None = None
    error: Exception | None = None


def normalize_http_url(url: str) -> str | None:
    candidate = url.strip()
    if not candidate or any(ch.isspace() for ch in candidate):
        return None

    parsed = urlsplit(candidate)
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        return None

    hostname = parsed.hostname
    if not parsed.netloc or not hostname:
        return None

    try:
        port = parsed.port
    except ValueError:
        return None

    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo = f"{userinfo}:{parsed.password}"
        userinfo = f"{userinfo}@"

    netloc = f"{userinfo}{hostname.lower()}"
    if port is not None:
        netloc = f"{netloc}:{port}"

    normalized = SplitResult(
        scheme=scheme,
        netloc=netloc,
        path=parsed.path,
        query=parsed.query,
        fragment="",
    )
    return urlunsplit(normalized)


class SourceRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def list_active(self) -> list[Source]:
        result = await self.session.execute(select(Source).where(Source.is_active.is_(True)))
        return list(result.scalars().all())

    async def list_all(self) -> list[Source]:
        result = await self.session.execute(select(Source).order_by(Source.id.asc()))
        return list(result.scalars().all())

    @staticmethod
    def _is_duplicate_name_error(error: IntegrityError) -> bool:
        details = " ".join(
            [
                str(getattr(error, "orig", "") or ""),
                str(getattr(error, "statement", "") or ""),
            ]
        ).lower()
        return any(marker in details for marker in ("sources.name", "uq_sources_name", "unique"))

    async def create(self, source_type: str, name: str, url: str, meta: dict | None = None) -> SourceCreateResult:
        if source_type not in ALLOWED_SOURCE_TYPES:
            return SourceCreateResult(status=SourceCreateStatus.INVALID_SOURCE_TYPE)

        normalized_url = normalize_http_url(url)
        if not normalized_url:
            return SourceCreateResult(status=SourceCreateStatus.INVALID_URL)

        source = Source(type=source_type, name=name, url=normalized_url, meta=meta or {})
        self.session.add(source)
        try:
            await self.session.commit()
        except IntegrityError as exc:
            await self.session.rollback()
            if self._is_duplicate_name_error(exc):
                return SourceCreateResult(status=SourceCreateStatus.DUPLICATE_NAME, error=exc)
            return SourceCreateResult(status=SourceCreateStatus.DB_ERROR, error=exc)
        except Exception as exc:
            await self.session.rollback()
            return SourceCreateResult(status=SourceCreateStatus.DB_ERROR, error=exc)
        await self.session.refresh(source)
        return SourceCreateResult(status=SourceCreateStatus.CREATED, source=source)

    async def remove(self, source_id: int) -> bool:
        source = await self.session.get(Source, source_id)
        if not source:
            return False
        await self.session.delete(source)
        await self.session.commit()
        return True


class NewsRepository:
    def __init__(self, session: AsyncSession, timezone: str = "UTC"):
        self.session = session
        self.timezone = timezone

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

    async def fetch_period_news(self, start_dt: datetime, end_dt: datetime, limit: int | None = None) -> list[RawNews]:
        query = (
            select(RawNews)
            .where(and_(RawNews.published_at >= start_dt, RawNews.published_at < end_dt))
            .order_by(RawNews.published_at.desc(), RawNews.id.desc())
        )
        if limit and limit > 0:
            query = query.limit(limit)

        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def reject(self, raw_news_id: int, source_id: int, reason: str) -> None:
        await self.reject_many([(raw_news_id, source_id, reason)])

    async def reject_many(self, rejects: Iterable[tuple[int, int, str]], batch_size: int = 100) -> int:
        prepared = list(rejects)
        if not prepared:
            return 0

        inserted = 0
        for idx in range(0, len(prepared), batch_size):
            chunk = prepared[idx:idx + batch_size]
            raw_news_ids = [raw_news_id for raw_news_id, _, _ in chunk]
            existing_q = await self.session.execute(
                select(RejectedNews.raw_news_id).where(RejectedNews.raw_news_id.in_(raw_news_ids))
            )
            existing_ids = set(existing_q.scalars().all())

            pending_rows = [
                (raw_news_id, source_id, reason)
                for raw_news_id, source_id, reason in chunk
                if raw_news_id not in existing_ids
            ]
            for raw_news_id, source_id, reason in pending_rows:
                self.session.add(RejectedNews(raw_news_id=raw_news_id, source_id=source_id, reason=reason))

            try:
                await self.session.commit()
                inserted += len(pending_rows)
            except IntegrityError:
                await self.session.rollback()
                for raw_news_id, source_id, reason in pending_rows:
                    existing = await self.session.scalar(
                        select(RejectedNews.id).where(RejectedNews.raw_news_id == raw_news_id)
                    )
                    if existing:
                        continue
                    try:
                        async with self.session.begin_nested():
                            self.session.add(RejectedNews(raw_news_id=raw_news_id, source_id=source_id, reason=reason))
                            await self.session.flush()
                            inserted += 1
                    except IntegrityError:
                        continue
                await self.session.commit()

        return inserted

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

    async def is_period_already_published(
        self,
        period_type: str,
        period_start: datetime,
        period_end: datetime,
    ) -> bool:
        query = select(PublishedNews.id).where(
            and_(
                PublishedNews.period_type == period_type,
                PublishedNews.period_start == period_start,
                PublishedNews.period_end == period_end,
            )
        )
        return await self.session.scalar(query) is not None

    async def compute_daily_stats(self, stat_date: date) -> DailyStats:
        start, end = self._local_period_to_utc_bounds(
            datetime.combine(stat_date, datetime.min.time()),
            timedelta(days=1),
        )

        raws = await self.fetch_period_news(start, end)
        raw_by_source = Counter(str(r.source_id) for r in raws)

        rej_q = await self.session.execute(
            select(RejectedNews).where(and_(RejectedNews.rejected_at >= start, RejectedNews.rejected_at < end))
        )
        rejected_rows = list(rej_q.scalars().all())
        reject_by_source = Counter(str(r.source_id) for r in rejected_rows)
        reject_reasons = Counter(str(r.reason) for r in rejected_rows)

        pub_q = await self.session.execute(
            select(PublishedNews).where(and_(PublishedNews.published_at >= start, PublishedNews.published_at < end))
        )
        published_rows = list(pub_q.scalars().all())

        quality = self._aggregate_quality(published_rows, len(raws), len(rejected_rows), reject_reasons)

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
        start, end = self._local_period_to_utc_bounds(
            datetime.combine(week_start, datetime.min.time()),
            timedelta(days=7),
        )
        return await self._compute_weekly_stats_in_bounds(week_start, start, end)

    async def compute_weekly_stats_live(self, week_start: date, now_local_naive: datetime) -> WeeklyStats:
        start, _ = self._local_period_to_utc_bounds(
            datetime.combine(week_start, datetime.min.time()),
            timedelta(days=7),
        )
        now_utc = (
            now_local_naive.replace(tzinfo=ZoneInfo(self.timezone))
            .astimezone(ZoneInfo("UTC"))
            .replace(tzinfo=None)
        )
        return await self._compute_weekly_stats_in_bounds(week_start, start, now_utc)

    async def _compute_weekly_stats_in_bounds(self, week_start: date, start: datetime, end: datetime) -> WeeklyStats:

        raws = await self.fetch_period_news(start, end)
        raw_by_source = Counter(str(r.source_id) for r in raws)

        rej_q = await self.session.execute(
            select(RejectedNews).where(and_(RejectedNews.rejected_at >= start, RejectedNews.rejected_at < end))
        )
        rejected_rows = list(rej_q.scalars().all())
        reject_by_source = Counter(str(r.source_id) for r in rejected_rows)
        reject_reasons = Counter(str(r.reason) for r in rejected_rows)

        pub_q = await self.session.execute(
            select(PublishedNews).where(and_(PublishedNews.published_at >= start, PublishedNews.published_at < end))
        )
        published_rows = list(pub_q.scalars().all())

        quality = self._aggregate_quality(published_rows, len(raws), len(rejected_rows), reject_reasons)

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

    def _local_period_to_utc_bounds(self, start_local_naive: datetime, length: timedelta) -> tuple[datetime, datetime]:
        tz = ZoneInfo(self.timezone)
        start_local = start_local_naive.replace(tzinfo=tz)
        end_local = start_local + length
        return (
            start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        )

    @staticmethod
    def _aggregate_quality(
        published_rows: list[PublishedNews],
        raw_count: int,
        rejected_count: int,
        rejected_reason_counts: Counter[str] | None = None,
    ) -> dict:
        selected = 0
        deduplicated = 0
        duplicates_removed = 0
        fetched_from_db = 0
        rejected_by_filter = 0
        removed_as_duplicates = 0
        removed_by_topic_limit = 0
        published_items = 0
        topic_distribution: Counter[str] = Counter()
        rejection_reasons: Counter[str] = Counter(rejected_reason_counts or {})

        for row in published_rows:
            qm = row.quality_metrics or {}
            selected += int(qm.get("selected", 0))
            deduplicated += int(qm.get("deduplicated", 0))
            duplicates_removed += int(qm.get("duplicates_removed", 0))
            fetched_from_db += int(qm.get("fetched_from_db", qm.get("raw_total", 0)))
            rejected_by_filter += int(qm.get("rejected_by_filter", qm.get("rejected_total", 0)))
            removed_as_duplicates += int(qm.get("removed_as_duplicates", qm.get("duplicates_removed", 0)))
            removed_by_topic_limit += int(qm.get("removed_by_topic_limit", 0))
            published_items += int(qm.get("published_items", qm.get("selected", 0)))
            for topic, count in (row.topic_breakdown or {}).items():
                topic_distribution[str(topic)] += int(count)

        acceptance_rate = 0.0 if raw_count == 0 else round((raw_count - rejected_count) / raw_count, 4)
        return {
            "raw_total": raw_count,
            "rejected_total": rejected_count,
            "selected_total": selected,
            "deduplicated_total": deduplicated,
            "duplicates_removed_total": duplicates_removed,
            "fetched_from_db_total": fetched_from_db,
            "rejected_by_filter_total": rejected_by_filter,
            "removed_as_duplicates_total": removed_as_duplicates,
            "removed_by_topic_limit_total": removed_by_topic_limit,
            "published_items_total": published_items,
            "acceptance_rate": acceptance_rate,
            "topic_distribution": dict(topic_distribution),
            "rejection_reasons": dict(rejection_reasons),
        }
