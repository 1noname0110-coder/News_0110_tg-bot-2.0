import os
from datetime import datetime, timedelta

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.models import RawNews
from app.services.summarizer import DigestSummarizer


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "BOT_TOKEN": "x",
            "CHANNEL_ID": "@c",
            "ADMIN_USER_IDS": "1",
            "LLM_ENABLED": False,
            "DEDUP_SIMILARITY_THRESHOLD": 0.8,
            "PER_TOPIC_LIMIT_DAILY": 2,
            "PER_TOPIC_LIMIT_WEEKLY": 3,
            "PUBLISH_ALL_IMPORTANT": True,
        }
    )


def test_extractive_deduplicates_and_balances_topics() -> None:
    settings = _settings()
    s = DigestSummarizer(settings)

    base = datetime(2024, 1, 1, 10, 0, 0)
    items = [
        RawNews(id=1, source_id=1, title="ЦБ повысил ключевую ставку", summary="Решение по ставке влияет на инфляцию и кредитование", url="https://example.com/1", external_id="a", published_at=base),
        RawNews(id=2, source_id=2, title="Центробанк повысил ключевую ставку", summary="Похожая новость из другого источника", url="https://example.com/2", external_id="b", published_at=base + timedelta(minutes=1)),
        RawNews(id=3, source_id=1, title="Правительство утвердило новый закон о бюджете", summary="Закон меняет параметры бюджетного планирования", url="https://example.com/3", external_id="c", published_at=base + timedelta(minutes=2)),
        RawNews(id=4, source_id=3, title="На саммите ООН обсудили санкции", summary="Международные переговоры и новые ограничения", url="https://example.com/4", external_id="d", published_at=base + timedelta(minutes=3)),
    ]

    digest = s._build_extractive("daily", items, {"1": 2, "2": 1, "3": 1})

    assert digest.items_count >= 3
    assert digest.quality_metrics["duplicates_removed"] >= 1
    assert sum(digest.topic_breakdown.values()) == digest.items_count
    assert "Источник</a>" in digest.body


def test_extractive_reports_removed_by_topic_limit() -> None:
    settings = _settings()
    settings.per_topic_limit_daily = 1
    s = DigestSummarizer(settings)

    base = datetime(2024, 1, 1, 10, 0, 0)
    items = [
        RawNews(id=1, source_id=1, title="Парламент утвердил бюджет", summary="Бюджет и правительство", url="https://example.com/a", external_id="a", published_at=base),
        RawNews(id=2, source_id=1, title="Правительство обсудило реформу", summary="Политическое решение", url="https://example.com/b", external_id="b", published_at=base + timedelta(minutes=1)),
        RawNews(id=3, source_id=1, title="Президент подписал указ", summary="Политика и госуправление", url="https://example.com/c", external_id="c", published_at=base + timedelta(minutes=2)),
        RawNews(id=4, source_id=2, title="ЦБ изменил ставку", summary="Экономика и инфляция", url="https://example.com/d", external_id="d", published_at=base + timedelta(minutes=3)),
        RawNews(id=5, source_id=2, title="ООН провела встречу", summary="Международные переговоры", url="https://example.com/e", external_id="e", published_at=base + timedelta(minutes=4)),
    ]

    digest = s._build_extractive("daily", items, {"1": 3, "2": 2})

    assert digest.quality_metrics["removed_by_topic_limit"] >= 1
