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
        RawNews(id=2, source_id=2, title="Центробанк повысил ключевую ставку", summary="Похожая новость из другого источника", url="https://example.com/1", external_id="b", published_at=base + timedelta(minutes=1)),
        RawNews(id=3, source_id=1, title="Правительство утвердило новый закон о бюджете", summary="Закон меняет параметры бюджетного планирования", url="https://example.com/3", external_id="c", published_at=base + timedelta(minutes=2)),
        RawNews(id=4, source_id=3, title="На саммите ООН обсудили санкции", summary="Международные переговоры и новые ограничения", url="https://example.com/4", external_id="d", published_at=base + timedelta(minutes=3)),
    ]

    digest = s._build_extractive("daily", items, {"1": 2, "2": 1, "3": 1})

    assert digest.items_count >= 3
    assert digest.quality_metrics["duplicates_removed"] >= 1
    assert sum(digest.topic_breakdown.values()) == digest.items_count
    assert "Источник</a>" in digest.body


def test_deduplicate_keeps_same_titles_for_different_events() -> None:
    settings = _settings()
    s = DigestSummarizer(settings)
    base = datetime(2024, 1, 1, 10, 0, 0)
    items = [
        RawNews(
            id=10,
            source_id=1,
            title="В городе произошел взрыв",
            summary="Инцидент произошел на заводе, есть пострадавшие.",
            url="https://source-a.example/news/1",
            external_id="event-a",
            published_at=base,
        ),
        RawNews(
            id=11,
            source_id=2,
            title="В городе произошел взрыв",
            summary="В другом районе сообщили о взрыве бытового газа в жилом доме.",
            url="https://source-b.example/news/2",
            external_id="event-b",
            published_at=base + timedelta(minutes=3),
        ),
    ]

    deduped = s._deduplicate(items)

    assert len(deduped) == 2


def test_deduplicate_merges_different_titles_with_same_url() -> None:
    settings = _settings()
    s = DigestSummarizer(settings)
    base = datetime(2024, 1, 1, 10, 0, 0)
    shared_url = "https://news.example/article/123"
    items = [
        RawNews(
            id=20,
            source_id=1,
            title="Министр объявил о новых мерах поддержки",
            summary="Власти представили пакет мер для бизнеса.",
            url=shared_url,
            external_id="id-1",
            published_at=base,
        ),
        RawNews(
            id=21,
            source_id=2,
            title="Правительство представило пакет поддержки бизнеса",
            summary="По словам министра, меры начнут действовать в следующем месяце.",
            url=shared_url,
            external_id="id-2",
            published_at=base + timedelta(minutes=2),
        ),
    ]

    deduped = s._deduplicate(items)

    assert len(deduped) == 1


def test_build_extractive_publish_all_important_allows_more_than_default_cap() -> None:
    settings = _settings()
    settings.publish_all_important = True
    settings.per_topic_limit_daily = 100
    settings.dedup_similarity_threshold = 0.99
    s = DigestSummarizer(settings)

    base = datetime(2024, 1, 1, 10, 0, 0)
    unique_topics = [
        "металлургия",
        "логистика",
        "телеком",
        "фармацевтика",
        "агросектор",
        "энергетика",
        "строительство",
        "машиностроение",
        "банкинг",
        "страхование",
        "ритейл",
        "транспорт",
        "IT-экспорт",
        "судоходство",
        "авиапром",
        "биотех",
    ]
    items = [
        RawNews(
            id=100 + i,
            source_id=i,
            title=f"{topic}: зафиксирован новый этап реформ",
            summary=f"Отрасль {topic} показала уникальную динамику: отдельный пакет мер, контракты и экспортные эффекты для региона {i}.",
            url=f"https://example.com/news/{i}",
            external_id=f"ext-{i}",
            published_at=base + timedelta(minutes=i),
        )
        for i, topic in enumerate(unique_topics, start=1)
    ]

    digest = s._build_extractive("daily", items, {str(i): 1 for i in range(1, 17)})

    assert digest.items_count > 12
    assert digest.items_count == 16


def test_build_extractive_publish_all_important_false_keeps_default_cap() -> None:
    settings = _settings()
    settings.publish_all_important = False
    settings.per_topic_limit_daily = 100
    settings.dedup_similarity_threshold = 0.99
    s = DigestSummarizer(settings)

    base = datetime(2024, 1, 1, 10, 0, 0)
    unique_topics = [
        "металлургия",
        "логистика",
        "телеком",
        "фармацевтика",
        "агросектор",
        "энергетика",
        "строительство",
        "машиностроение",
        "банкинг",
        "страхование",
        "ритейл",
        "транспорт",
        "IT-экспорт",
        "судоходство",
        "авиапром",
        "биотех",
    ]
    items = [
        RawNews(
            id=200 + i,
            source_id=i,
            title=f"{topic}: зафиксирован новый этап реформ",
            summary=f"Отрасль {topic} показала уникальную динамику: отдельный пакет мер, контракты и экспортные эффекты для региона {i}.",
            url=f"https://example.com/important/{i}",
            external_id=f"important-{i}",
            published_at=base + timedelta(minutes=i),
        )
        for i, topic in enumerate(unique_topics, start=1)
    ]

    digest = s._build_extractive("daily", items, {str(i): 1 for i in range(1, 17)})

    assert digest.items_count == 12
