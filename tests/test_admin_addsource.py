from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.handlers import admin
from app.repositories import SourceCreateResult, SourceCreateStatus, SourceUpdateResult, SourceUpdateStatus


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
            return SourceCreateResult(
                status=SourceCreateStatus.CREATED,
                source=SimpleNamespace(id=7, name=name, type=source_type),
            )

    monkeypatch.setattr(admin, "get_session_factory", lambda: (lambda: _FakeSession()))
    monkeypatch.setattr(admin, "SourceRepository", _FakeRepo)

    await admin.add_source(_FakeMessage(), settings)

    assert captured == {
        "source_type": "rss",
        "name": "test",
        "url": "http://example.com/News",
        "meta": {},
    }
    assert answers == ["Источник добавлен: #7 test (rss)"]


@pytest.mark.asyncio
async def test_addsource_returns_duplicate_message(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()
    answers: list[str] = []

    class _FakeMessage:
        text = "/addsource rss dup https://example.com"
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
            return SourceCreateResult(status=SourceCreateStatus.DUPLICATE_NAME)

    monkeypatch.setattr(admin, "get_session_factory", lambda: (lambda: _FakeSession()))
    monkeypatch.setattr(admin, "SourceRepository", _FakeRepo)

    await admin.add_source(_FakeMessage(), settings)

    assert answers == ["Источник с таким именем уже существует."]


@pytest.mark.asyncio
async def test_addsource_returns_generic_db_error_and_logs_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()
    answers: list[str] = []
    log_calls: list[tuple] = []

    class _FakeMessage:
        text = "/addsource rss test https://example.com"
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
            return SourceCreateResult(status=SourceCreateStatus.DB_ERROR, error=RuntimeError("db fail"))

    def _fake_logger(*args, **kwargs):
        log_calls.append((args, kwargs))

    monkeypatch.setattr(admin, "get_session_factory", lambda: (lambda: _FakeSession()))
    monkeypatch.setattr(admin, "SourceRepository", _FakeRepo)
    monkeypatch.setattr(admin.logger, "exception", _fake_logger)

    await admin.add_source(_FakeMessage(), settings)

    assert answers == ["Ошибка сохранения источника, проверьте логи"]
    assert len(log_calls) == 1


@pytest.mark.asyncio
async def test_listsources_returns_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()
    answers: list[str] = []

    class _FakeMessage:
        text = "/listsources"
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

        async def list_sources(self, active_only=None):
            return [
                SimpleNamespace(id=1, is_active=True, type="rss", name="A", url="https://a"),
                SimpleNamespace(id=2, is_active=False, type="site", name="B", url="https://b"),
            ]

    monkeypatch.setattr(admin, "get_session_factory", lambda: (lambda: _FakeSession()))
    monkeypatch.setattr(admin, "SourceRepository", _FakeRepo)

    await admin.list_sources(_FakeMessage(), settings)

    assert "#1 [on] rss A https://a" in answers[0]
    assert "#2 [off] site B https://b" in answers[0]


@pytest.mark.asyncio
async def test_togglesource_updates_source(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()
    answers: list[str] = []

    class _FakeMessage:
        text = "/togglesource 7 off"
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

        async def toggle(self, source_id: int, enabled: bool):
            assert source_id == 7
            assert enabled is False
            return SourceUpdateResult(
                status=SourceUpdateStatus.UPDATED,
                source=SimpleNamespace(id=7, is_active=False),
            )

    monkeypatch.setattr(admin, "get_session_factory", lambda: (lambda: _FakeSession()))
    monkeypatch.setattr(admin, "SourceRepository", _FakeRepo)

    await admin.toggle_source(_FakeMessage(), settings)

    assert answers == ["Источник #7 выключен."]


@pytest.mark.asyncio
async def test_editsource_meta_requires_object() -> None:
    settings = _settings()
    answers: list[str] = []

    class _FakeMessage:
        text = "/editsource 7 meta []"
        from_user = SimpleNamespace(id=42)

        async def answer(self, text: str) -> None:
            answers.append(text)

    await admin.edit_source(_FakeMessage(), settings)

    assert answers == ["Для поля meta требуется JSON-объект."]


@pytest.mark.asyncio
async def test_checksource_reports_error(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings()
    answers: list[str] = []

    class _FakeMessage:
        text = "/checksource 5"
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

        async def get_by_id(self, source_id: int):
            return SimpleNamespace(id=5, name="x", type="rss", url="https://x", meta={})

    class _FakeCollector:
        def __init__(self, settings):
            pass

        async def _fetch_rss(self, source):
            raise RuntimeError("boom")

        async def aclose(self):
            return None

    monkeypatch.setattr(admin, "get_session_factory", lambda: (lambda: _FakeSession()))
    monkeypatch.setattr(admin, "SourceRepository", _FakeRepo)
    monkeypatch.setattr(admin, "NewsCollector", _FakeCollector)

    await admin.check_source(_FakeMessage(), settings)

    assert "Найдено элементов: 0" in answers[0]
    assert "Первая ошибка: RuntimeError: boom" in answers[0]
