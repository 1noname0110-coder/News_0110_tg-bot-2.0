from __future__ import annotations

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiogram import Bot

from app.config import Settings
from app.db import AsyncSessionLocal
from app.services.digest_service import DigestService


class BotScheduler:
    def __init__(self, settings: Settings, bot: Bot, digest_service: DigestService):
        self.settings = settings
        self.bot = bot
        self.digest_service = digest_service
        self.scheduler = AsyncIOScheduler(timezone=settings.timezone)

    def setup(self) -> None:
        self.scheduler.add_job(
            self._collect_job,
            CronTrigger(minute="*/30", timezone=self.settings.timezone),
            id="collect_news",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._daily_job,
            CronTrigger(hour=self.settings.daily_publish_hour, minute=0, timezone=self.settings.timezone),
            id="publish_daily",
            replace_existing=True,
        )
        self.scheduler.add_job(
            self._weekly_job,
            CronTrigger(day_of_week="sun", hour=self.settings.weekly_publish_hour, minute=0, timezone=self.settings.timezone),
            id="publish_weekly",
            replace_existing=True,
        )

    async def _collect_job(self) -> None:
        async with AsyncSessionLocal() as session:
            await self.digest_service.collect_and_store(session)

    async def _daily_job(self) -> None:
        async with AsyncSessionLocal() as session:
            await self.digest_service.publish_daily(self.bot, session)

    async def _weekly_job(self) -> None:
        async with AsyncSessionLocal() as session:
            await self.digest_service.publish_weekly(self.bot, session)

    def start(self) -> None:
        self.setup()
        self.scheduler.start()
