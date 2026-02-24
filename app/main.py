import asyncio
import logging
import pathlib
import sys

if __package__ in {None, ""}:
    # Поддержка запуска как файла: python app/main.py
    sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from app.bot import create_bot, create_dispatcher
from app.config import get_settings
from app.db import close_engine, init_db
from app.services.digest_service import DigestService
from app.services.scheduler_service import BotScheduler


logger = logging.getLogger(__name__)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    logger.info("Инициализация приложения")
    settings = get_settings()
    await init_db()

    bot = create_bot(settings)
    dp = create_dispatcher(settings)

    digest_service = DigestService(settings)
    scheduler = BotScheduler(settings=settings, bot=bot, digest_service=digest_service)
    dp["digest_service"] = digest_service

    logger.info("Запуск фоновых сервисов")
    scheduler.start()

    try:
        logger.info("Запуск polling Telegram-бота")
        await dp.start_polling(bot, settings=settings)
    finally:
        logger.info("Запуск graceful shutdown")
        scheduler.stop()
        await digest_service.aclose()
        await bot.session.close()
        await close_engine()
        logger.info("Graceful shutdown завершен")


if __name__ == "__main__":
    asyncio.run(main())
