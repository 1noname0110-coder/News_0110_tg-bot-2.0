from __future__ import annotations

import os
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.handlers import admin
from app.services.digest_service import DigestService


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "BOT_TOKEN": "token",
            "CHANNEL_ID": "-1001234567890",
            "ADMIN_USER_IDS": "42",
            "TIMEZONE": "Asia/Vladivostok",
        }
    )


class _FixedDateTime(datetime):
    fixed_now: datetime

    @classmethod
    def now(cls, tz=None):  # noqa: ANN001
        if tz is None:
            return cls.fixed_now
        return cls.fixed_now.astimezone(tz)


@pytest.mark.asyncio
async def test_publish_weekly_uses_calendar_week_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()
    service = DigestService(settings)

    fixed_now = datetime(2026, 1, 7, 15, 30, tzinfo=ZoneInfo(settings.timezone))
    _FixedDateTime.fixed_now = fixed_now
    monkeypatch.setattr("app.services.digest_service.datetime", _FixedDateTime)

    captured: dict[str, datetime | str] = {}

    async def _fake_publish_period(*, bot, session, period_type, start_dt, end_dt):  # noqa: ANN001
        captured["start_dt"] = start_dt
        captured["end_dt"] = end_dt
        captured["period_type"] = period_type

    monkeypatch.setattr(service, "_publish_period", _fake_publish_period)

    await service.publish_weekly(bot=object(), session=object())

    assert captured["period_type"] == "weekly"
    assert captured["start_dt"] == datetime(2026, 1, 4, 14, 0, 0)
    assert captured["end_dt"] == datetime(2026, 1, 7, 5, 30, 0)


@pytest.mark.asyncio
async def test_statweek_shows_same_calendar_week_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()

    fixed_now = datetime(2026, 1, 7, 15, 30, tzinfo=ZoneInfo(settings.timezone))
    _FixedDateTime.fixed_now = fixed_now
    monkeypatch.setattr("app.handlers.admin.datetime", _FixedDateTime)

    captured_week_start: dict[str, object] = {}

    class _FakeRepo:
        def __init__(self, _session):
            pass

        async def compute_weekly_stats(self, week_start):
            captured_week_start["value"] = week_start
            return SimpleNamespace(
                published_count=3,
                rejected_count=1,
                source_usage={"1": 2},
                rejection_breakdown={"1": 1},
            )

    class _FakeSessionCtx:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return None

    answers: list[str] = []

    class _FakeMessage:
        text = "/statweek"
        from_user = SimpleNamespace(id=42)

        async def answer(self, text: str) -> None:
            answers.append(text)

    monkeypatch.setattr(admin, "AsyncSessionLocal", _FakeSessionCtx)
    monkeypatch.setattr(admin, "NewsRepository", _FakeRepo)

    await admin.stat_week(_FakeMessage(), settings)

    assert str(captured_week_start["value"]) == "2026-01-05"
    assert answers
    assert "Статистика недели с 05.01.2026 00:00 по 07.01.2026 15:30" in answers[0]
