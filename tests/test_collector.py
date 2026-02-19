import os

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.services.collector import NewsCollector


def _settings() -> Settings:
    return Settings.model_validate({"BOT_TOKEN": "token", "CHANNEL_ID": "-1001234567890"})


def test_strip_html_keeps_plain_text_without_bs4_noise() -> None:
    collector = NewsCollector(_settings())
    text = "https://tass.ru/rss/v2.xml?sections=Russia"
    assert collector._strip_html(text) == text


def test_strip_html_extracts_text_from_markup() -> None:
    collector = NewsCollector(_settings())
    html = "<p>Заголовок <b>дня</b></p>"
    assert collector._strip_html(html) == "Заголовок дня"
