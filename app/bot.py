from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from app.config import Settings
from app.handlers.admin import router as admin_router


def create_bot(settings: Settings) -> Bot:
    return Bot(token=settings.bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))


def create_dispatcher(settings: Settings) -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(admin_router)
    dp["settings"] = settings
    return dp
