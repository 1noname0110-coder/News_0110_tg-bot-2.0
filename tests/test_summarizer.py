import os
from datetime import datetime, timedelta
from types import SimpleNamespace

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

import pytest

from app.config import Settings
from app.models import RawNews
from app.services.filtering import FilterResult
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


def _with_filter_results(items: list[RawNews]) -> list[tuple[RawNews, FilterResult]]:
    scored: list[tuple[RawNews, FilterResult]] = []
    for idx, item in enumerate(items, start=1):
        text = f"{item.title} {item.summary}".lower()
        if "полит" in text or "правитель" in text or "парламент" in text:
            topic = "politics"
        elif "энерг" in text:
            topic = "energy"
        elif "технолог" in text or "it" in text:
            topic = "technology"
        else:
            topic = "economics"
        scored.append((item, FilterResult(True, "релевантно", 200 - idx, topic, [])))
    return scored

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

    digest = s._build_extractive("daily", _with_filter_results(items), {"1": 2, "2": 1, "3": 1})

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

    deduped = s._deduplicate(_with_filter_results(items))

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

    deduped = s._deduplicate(_with_filter_results(items))

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

    digest = s._build_extractive("daily", _with_filter_results(items), {str(i): 1 for i in range(1, 17)})

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

    digest = s._build_extractive("daily", _with_filter_results(items), {str(i): 1 for i in range(1, 17)})

    assert digest.items_count == 12


def test_build_extractive_keeps_unique_items_with_small_input() -> None:
    settings = _settings()
    settings.publish_all_important = True
    settings.per_topic_limit_daily = 3
    s = DigestSummarizer(settings)

    base = datetime(2024, 1, 1, 10, 0, 0)
    items = [
        RawNews(
            id=501,
            source_id=1,
            title="Экономика: обновлен макропрогноз",
            summary="Минэкономики уточнило параметры роста и инфляции на следующий период.",
            url="https://example.com/small/1",
            external_id="small-1",
            published_at=base,
        ),
        RawNews(
            id=502,
            source_id=2,
            title="Политика: утвержден план реформ",
            summary="Профильный комитет согласовал последовательность институциональных изменений.",
            url="https://example.com/small/2",
            external_id="small-2",
            published_at=base + timedelta(minutes=1),
        ),
    ]

    digest = s._build_extractive("daily", _with_filter_results(items), {"1": 1, "2": 1})

    assert digest.items_count == 2
    assert sum(digest.topic_breakdown.values()) == 2
    assert digest.quality_metrics["selected"] == 2
    assert digest.body.count("https://example.com/small/1") == 1
    assert digest.body.count("https://example.com/small/2") == 1



@pytest.mark.asyncio
async def test_build_digest_with_llm_parses_items_and_metrics() -> None:
    settings = _settings()
    s = DigestSummarizer(settings)

    class FakeCompletions:
        async def create(self, **kwargs):  # noqa: ANN003
            content = (
                '{"title":"Дайджест","items":['
                '{"topic":"Экономика","headline":"Рост экспорта в АТР на фоне новых контрактов и тарифных корректировок","url":"https://example.com/1"},'
                '{"topic":"Политика","headline":"Парламент принял пакет поправок в бюджетное и налоговое регулирование","url":"https://example.com/2"},'
                '{"topic":"Международка","headline":"Подписано новое межправительственное соглашение по торговому сотрудничеству","url":"https://example.com/3"},'
                '{"topic":"Технологии","headline":"Ускорен запуск спутниковой программы с расширением производственных мощностей","url":"https://example.com/4"},'
                '{"topic":"Энергетика","headline":"Согласованы новые поставки СПГ в рамках долгосрочных экспортных договоров","url":"https://example.com/5"}'
                ']}'
            )
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
            )

    s.client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))

    base = datetime(2024, 1, 1, 10, 0, 0)
    items = [
        RawNews(
            id=i,
            source_id=i,
            title=f"Новость {i}",
            summary=f"Описание {i}",
            url=f"https://source.example/{i}",
            external_id=f"ext-{i}",
            published_at=base + timedelta(minutes=i),
        )
        for i in range(1, 6)
    ]

    digest = await s.build_digest("daily", _with_filter_results(items))

    assert digest.title == "Дайджест"
    assert digest.items_count == 5
    assert digest.quality_metrics["selected"] == 5
    assert digest.topic_breakdown
    assert "Источник: https://example.com/1" in digest.body


@pytest.mark.asyncio
async def test_build_digest_with_llm_invalid_format_falls_back_to_extractive() -> None:
    settings = _settings()
    s = DigestSummarizer(settings)

    class FakeCompletions:
        async def create(self, **kwargs):  # noqa: ANN003
            return SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content='{"title":"Плохо","items":[{"topic":"Экономика","headline":"Коротко","url":"https://bad.example/1"}]}'))],
            )

    s.client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))

    base = datetime(2024, 1, 1, 10, 0, 0)
    items = [
        RawNews(
            id=i,
            source_id=i,
            title=title,
            summary=(
                f"{topic} {i}: расширенное описание реформ и отраслевых последствий с уникальными параметрами {i * 7}. "
                "Показатели подтверждены ведомствами и профильными ассоциациями."
            ),
            url=f"https://valid.example/{i}",
            external_id=f"fallback-{i}",
            published_at=base + timedelta(minutes=i),
        )
        for i, (topic, title) in enumerate(
            [
                ("Экономика", "Минфин утвердил план бюджетной корректировки"),
                ("Политика", "Парламент одобрил пакет институциональных реформ"),
                ("Транспорт", "Правительство запустило программу модернизации портов"),
                ("Энергетика", "Оператор сети объявил о расширении генерирующих мощностей"),
                ("Технологии", "Ведомство согласовало новый регламент по ИИ-сервисам"),
            ],
            start=1,
        )
    ]

    digest = await s.build_digest("daily", _with_filter_results(items))

    assert digest.items_count > 0
    assert digest.quality_metrics["selected"] == digest.items_count
    assert "<a href=\"https://valid.example/1\">Источник</a>" in digest.body
    assert sum(digest.topic_breakdown.values()) == digest.items_count


@pytest.mark.asyncio
async def test_build_digest_with_llm_exception_falls_back_to_extractive() -> None:
    settings = _settings()
    settings.llm_enabled = True
    s = DigestSummarizer(settings)

    class FakeCompletions:
        async def create(self, **kwargs):  # noqa: ANN003
            raise RuntimeError("llm unavailable")

    s.client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))

    base = datetime(2024, 1, 1, 10, 0, 0)
    items = [
        RawNews(
            id=i,
            source_id=i,
            title=title,
            summary=(
                f"{topic} {i}: расширенное описание реформ и отраслевых последствий с уникальными параметрами {i * 9}. "
                "Показатели подтверждены ведомствами и профильными ассоциациями."
            ),
            url=f"https://exception.example/{i}",
            external_id=f"exception-{i}",
            published_at=base + timedelta(minutes=i),
        )
        for i, (topic, title) in enumerate(
            [
                ("Экономика", "Минфин представил обновленный бюджетный прогноз"),
                ("Политика", "Правительство согласовало пакет административных изменений"),
                ("Транспорт", "Оператор сообщил о расширении железнодорожных коридоров"),
                ("Энергетика", "Регулятор утвердил параметры модернизации сетей"),
                ("Технологии", "Профильное ведомство запустило пилот по цифровым сервисам"),
            ],
            start=1,
        )
    ]

    digest = await s.build_digest("daily", _with_filter_results(items))

    assert digest.items_count > 0
    assert digest.quality_metrics["selected"] == digest.items_count
    assert '<a href="https://exception.example/1">Источник</a>' in digest.body
    assert sum(digest.topic_breakdown.values()) == digest.items_count


def test_parse_llm_response_rejects_duplicate_urls() -> None:
    settings = _settings()
    s = DigestSummarizer(settings)
    content = (
        '{"title":"Проверка","items":['
        '{"topic":"Экономика","headline":"Согласован пакет мер для расширения промышленного экспорта и логистики","url":"https://example.com/same"},'
        '{"topic":"Политика","headline":"Парламент утвердил дорожную карту по институциональным изменениям в регионах","url":"https://example.com/same"},'
        '{"topic":"Энергетика","headline":"Регулятор подтвердил параметры модернизации сетевой инфраструктуры страны","url":"https://example.com/3"},'
        '{"topic":"Транспорт","headline":"Оператор объявил об увеличении пропускной способности ключевых железнодорожных узлов","url":"https://example.com/4"},'
        '{"topic":"Технологии","headline":"Ведомство запустило обновленный контур регулирования цифровых сервисов","url":"https://example.com/5"}'
        ']}'
    )

    parsed, reason = s._parse_llm_response("daily", content, 12)

    assert parsed is None
    assert reason is not None
    assert "duplicate url" in reason


def test_parse_llm_response_rejects_headline_length() -> None:
    settings = _settings()
    s = DigestSummarizer(settings)
    content = (
        '{"title":"Проверка","items":['
        '{"topic":"Экономика","headline":"Коротко","url":"https://example.com/1"},'
        '{"topic":"Политика","headline":"Парламент утвердил дорожную карту по институциональным изменениям в регионах","url":"https://example.com/2"},'
        '{"topic":"Энергетика","headline":"Регулятор подтвердил параметры модернизации сетевой инфраструктуры страны","url":"https://example.com/3"},'
        '{"topic":"Транспорт","headline":"Оператор объявил об увеличении пропускной способности ключевых железнодорожных узлов","url":"https://example.com/4"},'
        '{"topic":"Технологии","headline":"Ведомство запустило обновленный контур регулирования цифровых сервисов","url":"https://example.com/5"}'
        ']}'
    )

    parsed, reason = s._parse_llm_response("daily", content, 12)

    assert parsed is None
    assert reason is not None
    assert "headline length out of range" in reason


def test_parse_llm_response_accepts_valid_json() -> None:
    settings = _settings()
    s = DigestSummarizer(settings)
    content = (
        '{"title":"Проверка","items":['
        '{"topic":"Экономика","headline":"Согласован пакет мер для расширения промышленного экспорта и логистики","url":"https://example.com/1"},'
        '{"topic":"Политика","headline":"Парламент утвердил дорожную карту по институциональным изменениям в регионах","url":"https://example.com/2"},'
        '{"topic":"Энергетика","headline":"Регулятор подтвердил параметры модернизации сетевой инфраструктуры страны","url":"https://example.com/3"},'
        '{"topic":"Транспорт","headline":"Оператор объявил об увеличении пропускной способности ключевых железнодорожных узлов","url":"https://example.com/4"},'
        '{"topic":"Технологии","headline":"Ведомство запустило обновленный контур регулирования цифровых сервисов","url":"https://example.com/5"}'
        ']}'
    )

    parsed, reason = s._parse_llm_response("daily", content, 12)

    assert reason is None
    assert parsed is not None
    assert parsed["title"] == "Проверка"
    assert len(parsed["items"]) == 5
