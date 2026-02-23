from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app import main as app_main


@pytest.mark.asyncio
async def test_main_calls_shutdown_hooks_on_polling_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = object()

    scheduler = Mock()
    scheduler.start = Mock()
    scheduler.stop = Mock()

    digest_service = Mock()
    digest_service.aclose = AsyncMock()

    bot = SimpleNamespace(session=SimpleNamespace(close=AsyncMock()))

    dp = Mock()
    dp.start_polling = AsyncMock(side_effect=RuntimeError("polling stopped"))

    monkeypatch.setattr(app_main, "get_settings", Mock(return_value=settings))
    monkeypatch.setattr(app_main, "init_db", AsyncMock())
    monkeypatch.setattr(app_main, "create_bot", Mock(return_value=bot))
    monkeypatch.setattr(app_main, "create_dispatcher", Mock(return_value=dp))
    monkeypatch.setattr(app_main, "DigestService", Mock(return_value=digest_service))
    monkeypatch.setattr(app_main, "BotScheduler", Mock(return_value=scheduler))

    with pytest.raises(RuntimeError, match="polling stopped"):
        await app_main.main()

    scheduler.start.assert_called_once_with()
    dp.start_polling.assert_awaited_once_with(bot, settings=settings)

    scheduler.stop.assert_called_once_with()
    digest_service.aclose.assert_awaited_once_with()
    bot.session.close.assert_awaited_once_with()
