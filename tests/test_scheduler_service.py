from unittest.mock import Mock

from app.services.scheduler_service import BotScheduler


def test_stop_calls_apscheduler_shutdown_when_running() -> None:
    scheduler_service = BotScheduler.__new__(BotScheduler)
    scheduler_service.scheduler = Mock(running=True)

    scheduler_service.stop()

    scheduler_service.scheduler.shutdown.assert_called_once_with(wait=False)


def test_stop_skips_shutdown_when_not_running() -> None:
    scheduler_service = BotScheduler.__new__(BotScheduler)
    scheduler_service.scheduler = Mock(running=False)

    scheduler_service.stop()

    scheduler_service.scheduler.shutdown.assert_not_called()
