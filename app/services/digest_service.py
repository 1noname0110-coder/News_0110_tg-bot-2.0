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
    TELEGRAM_MAX_CHARS = 3900

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

        period_limit = self.settings.max_period_news_daily if period_type == "daily" else self.settings.max_period_news_weekly
        raw_items = await news_repo.fetch_period_news(start_dt, end_dt, limit=period_limit)
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

        await self._send_digest_messages(bot, digest.title, digest.body)

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

    async def _send_digest_messages(self, bot: Bot, title: str, body: str) -> None:
        chunks = self._split_body(body)
        total = len(chunks)
        for idx, chunk in enumerate(chunks, 1):
            header = f"{title}\n\n" if idx == 1 else f"{title} (продолжение {idx}/{total})\n\n"
            await bot.send_message(chat_id=self.settings.channel_id, text=header + chunk)

    def _split_body(self, body: str) -> list[str]:
        if len(body) <= self.TELEGRAM_MAX_CHARS:
            return [body]

        sections = body.split("\n\n")
        chunks: list[str] = []
        current = ""

        for section in sections:
            candidate = section if not current else f"{current}\n\n{section}"
            if len(candidate) <= self.TELEGRAM_MAX_CHARS:
                current = candidate
            else:
                if current:
                    chunks.append(current)
                if len(section) <= self.TELEGRAM_MAX_CHARS:
                    current = section
                else:
                    start = 0
                    while start < len(section):
                        part = section[start : start + self.TELEGRAM_MAX_CHARS]
                        chunks.append(part)
                        start += self.TELEGRAM_MAX_CHARS
                    current = ""
        if current:
            chunks.append(current)
        return chunks or [body[: self.TELEGRAM_MAX_CHARS]]
