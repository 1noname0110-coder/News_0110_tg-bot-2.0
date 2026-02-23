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


@pytest.mark.asyncio
async def test_addsource_rejects_invalid_url() -> None:
    settings = _settings()
    answers: list[str] = []

    class _FakeMessage:
        text = "/addsource rss name ftp://example.com"
        from_user = SimpleNamespace(id=42)

        async def answer(self, text: str) -> None:
            answers.append(text)

    await admin.add_source(_FakeMessage(), settings)

    assert answers == ["Некорректный URL. Разрешены только http:// и https:// ссылки."]


@pytest.mark.asyncio
async def test_addsource_accepts_valid_url(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()
    answers: list[str] = []
    captured: dict[str, str] = {}

    class _FakeMessage:
        text = "/addsource rss test HTTP://Example.COM/News"
        from_user = SimpleNamespace(id=42)

        async def answer(self, text: str) -> None:
            answers.append(text)

    class _FakeSession:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeRepo:
        def __init__(self, session):
            pass

        async def create(self, source_type: str, name: str, url: str, meta: dict | None = None):
            captured["source_type"] = source_type
            captured["name"] = name
            captured["url"] = url
            captured["meta"] = meta or {}
            return SimpleNamespace(id=7, name=name, type=source_type)

    monkeypatch.setattr(admin, "AsyncSessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(admin, "SourceRepository", _FakeRepo)

    await admin.add_source(_FakeMessage(), settings)

    assert captured == {
        "source_type": "rss",
        "name": "test",
        "url": "http://example.com/News",
        "meta": {},
    }
    assert answers == ["Источник добавлен: #7 test (rss)"]
