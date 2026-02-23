import os
from datetime import datetime

import httpx
import pytest

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.models import Source
from app.services.collector import NewsCollector


def _settings() -> Settings:
    return Settings.model_validate({"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890"})


def _source() -> Source:
    return Source(id=1, name="Site", type="site", url="https://example.com/news", meta={"selector": "article"})


class _FakeResponse:
    def __init__(self, text: str):
        self.text = text
        self.content = text.encode("utf-8")

    def raise_for_status(self) -> None:
        return None


class _FakeAsyncClient:
    def __init__(self, text: str, *args, **kwargs):
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get(self, url: str) -> _FakeResponse:
        return _FakeResponse(self._text)


def test_strip_html_keeps_plain_text_without_bs4_noise() -> None:
    collector = NewsCollector(_settings())
    text = "https://tass.ru/rss/v2.xml?sections=Russia"
    assert collector._strip_html(text) == text


def test_strip_html_extracts_text_from_markup() -> None:
    collector = NewsCollector(_settings())
    html = "<p>Заголовок <b>дня</b></p>"
    assert collector._strip_html(html) == "Заголовок дня"


@pytest.mark.asyncio
async def test_fetch_site_reordered_articles_keep_external_id_with_links(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = NewsCollector(_settings())
    source = _source()

    html_first = """
    <section>
      <article><h2>Первая новость</h2><a href="/items/1">читать</a><p>Кратко 1</p></article>
      <article><h2>Вторая новость</h2><a href="/items/2">читать</a><p>Кратко 2</p></article>
    </section>
    """
    html_second = """
    <section>
      <article><h2>Вторая новость</h2><a href="/items/2">читать</a><p>Кратко 2</p></article>
      <article><h2>Первая новость</h2><a href="/items/1">читать</a><p>Кратко 1</p></article>
    </section>
    """

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(html_first, *args, **kwargs))
    first_items = await collector._fetch_site(source)

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(html_second, *args, **kwargs))
    second_items = await collector._fetch_site(source)

    first_ids = {item["title"]: item["external_id"] for item in first_items}
    second_ids = {item["title"]: item["external_id"] for item in second_items}

    assert first_ids == second_ids


@pytest.mark.asyncio
async def test_fetch_site_reordered_articles_keep_external_id_without_links(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = NewsCollector(_settings())
    source = _source()

    html_first = """
    <section>
      <article><h2>Первая новость</h2><p>Кратко 1</p></article>
      <article><h2>Вторая новость</h2><p>Кратко 2</p></article>
    </section>
    """
    html_second = """
    <section>
      <article><h2>Вторая новость</h2><p>Кратко 2</p></article>
      <article><h2>Первая новость</h2><p>Кратко 1</p></article>
    </section>
    """

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(html_first, *args, **kwargs))
    first_items = await collector._fetch_site(source)

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _FakeAsyncClient(html_second, *args, **kwargs))
    second_items = await collector._fetch_site(source)

    first_ids = {item["title"]: item["external_id"] for item in first_items}
    second_ids = {item["title"]: item["external_id"] for item in second_items}

    assert first_ids == second_ids


@pytest.mark.asyncio
async def test_fetch_rss_uses_http_content_for_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = NewsCollector(_settings())
    source = Source(id=2, name="RSS", type="rss", url="https://example.com/rss", meta={})

    class _FakeParsed:
        entries = []

    called = {"parse": False, "to_thread": False, "http_get": False}

    def fake_parse(payload: bytes):
        called["parse"] = True
        assert payload == b"<rss></rss>"
        return _FakeParsed()

    async def fake_to_thread(func, *args, **kwargs):
        called["to_thread"] = True
        return func(*args, **kwargs)

    class _RssClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str) -> _FakeResponse:
            called["http_get"] = True
            assert url == source.url
            return _FakeResponse("<rss></rss>")

    monkeypatch.setattr("app.services.collector.feedparser.parse", fake_parse)
    monkeypatch.setattr("app.services.collector.asyncio.to_thread", fake_to_thread)
    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _RssClient(*args, **kwargs))

    items = await collector._fetch_rss(source)

    assert items == []
    assert called == {"parse": True, "to_thread": True, "http_get": True}


@pytest.mark.asyncio
async def test_fetch_rss_retries_after_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = NewsCollector(_settings())
    source = Source(id=2, name="RSS", type="rss", url="https://example.com/rss", meta={})

    class _FakeParsed:
        entries = [
            {
                "id": "news-1",
                "title": "Новость",
                "summary": "Описание",
                "link": "https://example.com/news/1",
            }
        ]

    attempts = {"count": 0}

    class _RetryClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str) -> _FakeResponse:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise httpx.ConnectError("network error")
            return _FakeResponse("<rss></rss>")

    async def fake_to_thread(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _RetryClient(*args, **kwargs))
    monkeypatch.setattr("app.services.collector.feedparser.parse", lambda payload: _FakeParsed())
    monkeypatch.setattr("app.services.collector.asyncio.to_thread", fake_to_thread)

    items = await collector.collect_from_source(source)

    assert attempts["count"] == 2
    assert len(items) == 1
    assert items[0]["external_id"] == "news-1"

def test_parse_dt_rfc822_to_utc_naive() -> None:
    collector = NewsCollector(_settings())

    dt = collector._parse_dt("Wed, 02 Oct 2002 13:00:00 +0200")

    assert dt.tzinfo is None
    assert dt == datetime(2002, 10, 2, 11, 0, 0)


def test_parse_dt_iso8601_to_utc_naive() -> None:
    collector = NewsCollector(_settings())

    dt = collector._parse_dt("2024-01-15T10:30:45Z")

    assert dt.tzinfo is None
    assert dt == datetime(2024, 1, 15, 10, 30, 45)


def test_parse_dt_invalid_returns_current_utc_naive() -> None:
    collector = NewsCollector(_settings())

    before = datetime.utcnow()
    dt = collector._parse_dt("not-a-date")
    after = datetime.utcnow()

    assert dt.tzinfo is None
    assert before <= dt <= after


@pytest.mark.asyncio
async def test_fetch_api_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = NewsCollector(_settings())
    source = Source(id=3, name="API", type="api", url="https://example.com/api", meta={})

    attempts = {"count": 0}

    class _ApiResponse:
        text = '{"items": []}'
        content = text.encode("utf-8")

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "items": [
                    {
                        "id": "item-1",
                        "title": "API title",
                        "summary": "API summary",
                        "url": "https://example.com/item/1",
                    }
                ]
            }

    class _ApiRetryClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise httpx.ConnectError("temporary network issue")
            return _ApiResponse()

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _ApiRetryClient(*args, **kwargs))

    items = await collector.collect_from_source(source)

    assert attempts["count"] == 2
    assert len(items) == 1
    assert items[0]["external_id"] == "item-1"


@pytest.mark.asyncio
async def test_collect_from_source_returns_empty_on_retry_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = NewsCollector(_settings())
    source = _source()

    attempts = {"count": 0}

    class _AlwaysFailClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str):
            attempts["count"] += 1
            raise httpx.ConnectError("network down")

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _AlwaysFailClient(*args, **kwargs))

    items = await collector.collect_from_source(source)

    assert attempts["count"] == 3
    assert items == []


@pytest.mark.asyncio
async def test_fetch_api_invalid_json_returns_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    collector = NewsCollector(_settings())
    source = Source(id=3, name="API", type="api", url="https://example.com/api", meta={})

    class _InvalidJsonResponse:
        text = "not-json"
        content = text.encode("utf-8")

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            raise ValueError("invalid json")

    class _ApiClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str):
            return _InvalidJsonResponse()

    monkeypatch.setattr(httpx, "AsyncClient", lambda *args, **kwargs: _ApiClient(*args, **kwargs))

    items = await collector.collect_from_source(source)

    assert items == []
