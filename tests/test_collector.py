import os

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
