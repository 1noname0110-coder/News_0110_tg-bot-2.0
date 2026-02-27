from datetime import datetime, timedelta

import pytest

from app.config import Settings
from app.models import RawNews
from app.services.pipeline import RankedNewsItem
from app.services.summarizer import DigestSummarizer


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "BOT_TOKEN": "x",
            "CHANNEL_ID": "@c",
            "ADMIN_USER_IDS": "1",
            "LLM_ENABLED": False,
        }
    )


def _ranked(item: RawNews, *, score: int, topic: str, high: bool = True) -> RankedNewsItem:
    return RankedNewsItem(
        raw=item,
        score=score,
        topic=topic,
        accepted=True,
        reason="релевантно",
        decision_trace=[],
        is_high_confidence=high,
        publish_priority=2 if high else 1,
        dedup_exact_key=f"{item.source_id}:{item.external_id}:{item.url}",
        dedup_similarity_key=DigestSummarizer.normalize_text(f"{item.title} {item.summary[:180]}"),
    )


def test_deduplicate_merges_same_url() -> None:
    s = DigestSummarizer(_settings())
    base = datetime(2024, 1, 1, 10, 0, 0)
    shared_url = "https://news.example/article/123"
    n1 = RawNews(id=1, source_id=1, title="Единая новость", summary="alpha", url=shared_url, external_id="1", published_at=base)
    n2 = RawNews(id=2, source_id=2, title="Единая новость", summary="alpha", url=shared_url, external_id="2", published_at=base + timedelta(minutes=1))

    out = s._deduplicate([_ranked(n1, score=10, topic="politics"), _ranked(n2, score=9, topic="politics")])
    assert len(out) == 1


def test_extractive_selection_has_consistent_markup() -> None:
    s = DigestSummarizer(_settings())
    base = datetime(2024, 1, 1, 10, 0, 0)
    items = [
        _ranked(RawNews(id=11, source_id=1, title="Title one", summary="alpha unique summary", url="https://example.com/1", external_id="1", published_at=base + timedelta(minutes=1)), score=9, topic="politics"),
        _ranked(RawNews(id=12, source_id=2, title="Title two", summary="beta unique summary", url="https://example.com/2", external_id="2", published_at=base + timedelta(minutes=2)), score=8, topic="economy"),
        _ranked(RawNews(id=13, source_id=3, title="Title three", summary="gamma unique summary", url="https://example.com/3", external_id="3", published_at=base + timedelta(minutes=3)), score=7, topic="international"),
        _ranked(RawNews(id=14, source_id=4, title="Title four", summary="delta unique summary", url="https://example.com/4", external_id="4", published_at=base + timedelta(minutes=4)), score=6, topic="conflict"),
        _ranked(RawNews(id=15, source_id=5, title="Title five", summary="epsilon unique summary", url="https://example.com/5", external_id="5", published_at=base + timedelta(minutes=5)), score=5, topic="politics"),
    ]

    digest = s._build_extractive("daily", items, {str(i): 1 for i in range(1, 6)})
    assert digest.items_count == 5
    assert digest.quality_metrics["selected"] == 5
    assert digest.body.count("Источник</a>") == 5


@pytest.mark.asyncio
async def test_llm_and_extractive_use_same_link_markup() -> None:
    settings = _settings()
    settings.llm_enabled = True
    settings.llm_api_key = "k"
    s = DigestSummarizer(settings)

    class _Resp:
        choices = [type("C", (), {"message": type("M", (), {"content": '{"title":"Дайджест","items":[{"topic":"Политика","headline":"Новость номер один подтверждена источниками","url":"https://example.com/1"},{"topic":"Экономика","headline":"Новость номер два подтверждена источниками","url":"https://example.com/2"},{"topic":"Международка","headline":"Новость номер три подтверждена источниками","url":"https://example.com/3"},{"topic":"Политика","headline":"Новость номер четыре подтверждена источниками","url":"https://example.com/4"},{"topic":"Экономика","headline":"Новость номер пять подтверждена источниками","url":"https://example.com/5"}]}'})})]

    class _Client:
        class chat:
            class completions:
                @staticmethod
                async def create(**_kwargs):
                    return _Resp()

    s.client = _Client()
    base = datetime(2024, 1, 1, 10, 0, 0)
    ranked = [
        _ranked(RawNews(id=i, source_id=1, title=f"t{i}", summary="s", url=f"https://example.com/{i}", external_id=str(i), published_at=base + timedelta(minutes=i)), score=10, topic="politics")
        for i in range(1, 6)
    ]

    digest = await s.build_digest("daily", ranked)
    assert digest.items_count == 5
    assert digest.body.count("Источник</a>") == 5
