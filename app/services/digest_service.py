from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.repositories import NewsRepository, SourceRepository
from app.services.collector import NewsCollector
from app.services.filtering import NewsFilter
from app.services.summarizer import DigestSummarizer


class DigestService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.collector = NewsCollector(settings)
        self.filter = NewsFilter()
        self.summarizer = DigestSummarizer(settings)

    async def collect_and_store(self, session: AsyncSession) -> None:
        source_repo = SourceRepository(session)
        news_repo = NewsRepository(session)

        sources = await source_repo.list_active()
        for source in sources:
            items = await self.collector.collect_from_source(source)
            if items:
                await news_repo.add_raw_news(items)

    async def publish_daily(self, bot: Bot, session: AsyncSession) -> None:
        tz = ZoneInfo(self.settings.timezone)
        now_local = datetime.now(tz)
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

        await self._publish_period(
            bot=bot,
            session=session,
            period_type="daily",
            start_dt=start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            end_dt=now_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        )

    async def publish_weekly(self, bot: Bot, session: AsyncSession) -> None:
        tz = ZoneInfo(self.settings.timezone)
        now_local = datetime.now(tz)
        week_start_local = (now_local - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)

        await self._publish_period(
            bot=bot,
            session=session,
            period_type="weekly",
            start_dt=week_start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            end_dt=now_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        )

    async def _publish_period(self, bot: Bot, session: AsyncSession, period_type: str, start_dt: datetime, end_dt: datetime) -> None:
        news_repo = NewsRepository(session)

        raw_items = await news_repo.fetch_period_news(start_dt, end_dt)
        accepted = []
        rejection_reasons = Counter()

        for item in raw_items:
            result = self.filter.evaluate(item.title, item.summary)
            if result.accepted:
                accepted.append(item)
            else:
                rejection_reasons[result.reason] += 1
                await news_repo.reject(item.id, item.source_id, result.reason)

        digest = await self.summarizer.build_digest(period_type, accepted)
        quality_metrics = dict(digest.quality_metrics)
        quality_metrics["raw_total"] = len(raw_items)
        quality_metrics["accepted_total"] = len(accepted)
        quality_metrics["rejected_total"] = len(raw_items) - len(accepted)
        quality_metrics["rejection_reasons"] = dict(rejection_reasons)

        message = f"{digest.title}\n\n{digest.body}"
        await bot.send_message(chat_id=self.settings.channel_id, text=message)

        await news_repo.publish_digest(
            period_type=period_type,
            period_start=start_dt,
            period_end=end_dt,
            title=digest.title,
            body=digest.body,
            items_count=digest.items_count,
            source_breakdown=digest.source_breakdown,
            topic_breakdown=digest.topic_breakdown,
            quality_metrics=quality_metrics,
        )
