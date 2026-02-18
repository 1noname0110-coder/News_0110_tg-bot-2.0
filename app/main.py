import asyncio
import logging
import pathlib
import sys

if __package__ in {None, ""}:
    # Поддержка запуска как файла: python app/main.py
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from app.bot import create_bot, create_dispatcher
from app.config import get_settings
from app.db import init_db
from app.services.digest_service import DigestService
from app.services.scheduler_service import BotScheduler


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    settings = get_settings()
    await init_db()

    bot = create_bot(settings)
    dp = create_dispatcher(settings)

    digest_service = DigestService(settings)
    scheduler = BotScheduler(settings=settings, bot=bot, digest_service=digest_service)
    scheduler.start()

    await dp.start_polling(bot, settings=settings)


if __name__ == "__main__":
    asyncio.run(main())
