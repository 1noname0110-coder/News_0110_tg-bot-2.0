from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.handlers import admin


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "BOT_TOKEN": "token",
            "CHANNEL_ID": "-1001234567890",
            "ADMIN_USER_IDS": "42",
            "TIMEZONE": "Asia/Vladivostok",
        }
    )


@pytest.mark.asyncio
async def test_addsource_rejects_invalid_source_type() -> None:
    settings = _settings()
    answers: list[str] = []

    class _FakeMessage:
        text = "/addsource telegram name https://example.com"
        from_user = SimpleNamespace(id=42)

        async def answer(self, text: str) -> None:
            answers.append(text)

    await admin.add_source(_FakeMessage(), settings)

    assert answers == ["Некорректный тип источника. Допустимые значения: api, rss, site."]
