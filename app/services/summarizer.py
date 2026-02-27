from __future__ import annotations

import html
import json
import logging
import re
from urllib.parse import urlparse
from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any

from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, HttpUrl, ValidationError, field_validator

from app.config import Settings
from app.services.pipeline import RankedNewsItem


logger = logging.getLogger(__name__)


@dataclass
class DigestOutput:
    title: str
    body: str
    items_count: int
    source_breakdown: dict[str, int]
    topic_breakdown: dict[str, int]
    quality_metrics: dict[str, int | float]


class LlmDigestItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    topic: str
    headline: str
    url: HttpUrl

    @field_validator("topic", "headline")
    @classmethod
    def _strip_required_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("field must not be empty")
        return normalized


class LlmDigestResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str
    items: list[LlmDigestItem]

    @field_validator("title")
    @classmethod
    def _title_required(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("title must not be empty")
        return normalized


class DigestSummarizer:
    _LLM_HEADLINE_MIN_LENGTH = 15
    _LLM_HEADLINE_MAX_LENGTH = 220
    _EXTRACTIVE_FALLBACK_WAVE2_SCORE_RATIO = 0.6
    _EXTRACTIVE_FALLBACK_WAVE3_SCORE_RATIO = 0.45

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = None
        if settings.llm_enabled and settings.llm_api_key:
            self.client = AsyncOpenAI(api_key=settings.llm_api_key, base_url=settings.llm_base_url)

    async def build_digest(self, period_type: str, news: list[RankedNewsItem]) -> DigestOutput:
        if not news:
            title = "Сводка: значимых событий не выявлено"
            return DigestOutput(
                title=title,
                body="Новых материалов стратегического уровня за период не найдено.",
                items_count=0,
                source_breakdown={},
                topic_breakdown={},
                quality_metrics={
                    "accepted_before_dedup": 0,
                    "deduplicated": 0,
                    "selected": 0,
                    "selected_base_pass": 0,
                    "selected_fallback": 0,
                    "selected_fallback_wave2": 0,
                    "selected_fallback_wave3": 0,
                    "duplicates_removed": 0,
                    "removed_by_topic_limit": 0,
                    "fallback_wave2_min_score": 0,
                    "fallback_wave3_min_score": 0,
                },
            )

        source_breakdown = Counter(str(entry.raw.source_id) for entry in news)

        if self.client:
            generated: dict[str, Any] | None = None
            try:
                generated = await self._build_with_llm(period_type, news)
            except Exception:
                logger.exception("LLM summarization failed, falling back to extractive digest")

            if generated:
                return DigestOutput(
                    title=generated["title"],
                    body=generated["body"],
                    items_count=len(generated["items"]),
                    source_breakdown=dict(source_breakdown),
                    topic_breakdown=generated["topic_breakdown"],
                    quality_metrics={
                        "accepted_before_dedup": len(news),
                        "deduplicated": len(news),
                        "selected": len(generated["items"]),
                        "selected_base_pass": len(generated["items"]),
                        "selected_fallback": 0,
                        "selected_fallback_wave2": 0,
                        "selected_fallback_wave3": 0,
                        "duplicates_removed": 0,
                        "removed_by_topic_limit": 0,
                        "fallback_wave2_min_score": 0,
                        "fallback_wave3_min_score": 0,
                    },
                )

            logger.warning("LLM summarization returned invalid format, falling back to extractive digest")

        return self._build_extractive(period_type, news, dict(source_breakdown))

    async def _build_with_llm(self, period_type: str, news: list[RankedNewsItem]) -> dict[str, Any] | None:
        limit = 15 if period_type == "weekly" else 12
        prompt_items = "\n".join(
            [f"- {entry.raw.title}. {entry.raw.summary[:350]} (источник {entry.raw.source_id})" for entry in news[:80]]
        )
        system = (
            "Ты редактор сухой аналитической сводки. Пиши только факты, без эмоций, пропаганды и кликбейта. "
            "Исключай локальные бытовые события и тактические детали конфликтов."
        )
        user = (
            f"Сформируй {period_type} сводку на русском языке. "
            "Верни СТРОГО JSON-объект без markdown и пояснений. "
            "Формат ответа: "
            '{"title": "...", "items": [{"topic": "...", "headline": "...", "url": "https://..."}]}. '
            f"Сделай от 5 до {limit} пунктов. "
            "Каждый headline — фактологичный, нейтральный и длиной 15-220 символов. "
            "URL должен быть абсолютным (https://...).\n"
            f"Новости:\n{prompt_items}"
        )

        response = await self.client.chat.completions.create(
            model=self.settings.llm_model,
            temperature=0.1,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        )
        content = response.choices[0].message.content or ""
        allowed_urls = {entry.raw.url.strip() for entry in news if entry.raw.url.strip()}
        parsed, reason = self._parse_llm_response(period_type, content, limit, allowed_urls)
        if parsed is None:
            logger.warning("LLM JSON validation failed: %s", reason)
        return parsed

    def _parse_llm_response(
        self,
        period_type: str,
        content: str,
        limit: int,
        allowed_urls: set[str],
    ) -> tuple[dict[str, Any] | None, str | None]:
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            return None, f"invalid JSON: {exc}"

        try:
            parsed = LlmDigestResponse.model_validate(payload)
        except ValidationError as exc:
            return None, f"schema validation error: {exc}"

        if not 5 <= len(parsed.items) <= limit:
            return None, f"items count out of range: {len(parsed.items)}"

        items: list[dict[str, Any]] = []
        topic_breakdown: Counter[str] = Counter()
        lines: list[str] = []
        seen_urls: set[str] = set()

        for item_number, item in enumerate(parsed.items, start=1):
            topic = item.topic.strip()
            headline = item.headline.strip()
            source_url = str(item.url).strip()

            if not (self._LLM_HEADLINE_MIN_LENGTH <= len(headline) <= self._LLM_HEADLINE_MAX_LENGTH):
                return None, f"headline length out of range at item {item_number}"
            if source_url in seen_urls:
                return None, f"duplicate url at item {item_number}: {source_url}"
            if source_url not in allowed_urls:
                return None, f"url not in source news at item {item_number}: {source_url}"
            seen_urls.add(source_url)

            topic_breakdown[self._normalize(topic)] += 1
            safe_topic = html.escape(topic)
            safe_headline = html.escape(headline)
            safe_url = html.escape(source_url, quote=True)
            lines.append(f"{item_number}) [{safe_topic}] {safe_headline}\n<a href=\"{safe_url}\">Источник</a>")
            items.append({"number": item_number, "topic": topic, "headline": headline, "source_url": source_url})

        return {
            "title": parsed.title,
            "body": "\n\n".join(lines),
            "items": items,
            "topic_breakdown": dict(topic_breakdown),
        }, None

    def _build_extractive(
        self,
        period_type: str,
        news: list[RankedNewsItem],
        source_breakdown: dict[str, int],
    ) -> DigestOutput:
        default_cap = 12 if period_type == "daily" else 15
        min_items = 5 if period_type == "daily" else 7
        per_topic_limit = self.settings.per_topic_limit

        deduped = self._deduplicate(news)
        ranked = sorted(
            deduped,
            key=lambda item: (item.publish_priority, item.score, len(item.raw.summary), item.raw.published_at),
            reverse=True,
        )
        cap = min(default_cap, len(ranked))

        topic_count: dict[str, int] = defaultdict(int)
        selected: list[RankedNewsItem] = []
        selected_ids: set[int] = set()
        removed_by_topic_limit = 0

        high_confidence_required = (
            self.settings.high_confidence_min_count_daily
            if period_type == "daily"
            else self.settings.high_confidence_min_count_weekly
        )

        for entry in ranked:
            if len(selected) >= min(cap, high_confidence_required):
                break
            if entry.raw.id in selected_ids or not entry.is_high_confidence:
                continue
            if topic_count[entry.topic] >= per_topic_limit:
                removed_by_topic_limit += 1
                continue
            selected.append(entry)
            selected_ids.add(entry.raw.id)
            topic_count[entry.topic] += 1

        target_count = max(min_items, cap)
        base_selected_count = len(selected)

        wave2_added = 0
        wave3_added = 0

        top_score = ranked[0].score if ranked else 0
        wave2_floor = max(self.settings.min_publish_score, int(round(top_score * self._EXTRACTIVE_FALLBACK_WAVE2_SCORE_RATIO)))
        wave3_floor = max(1, int(round(top_score * self._EXTRACTIVE_FALLBACK_WAVE3_SCORE_RATIO)))

        for entry in ranked:
            if len(selected) >= target_count:
                break
            if entry.raw.id in selected_ids or entry.score < wave2_floor:
                continue
            if topic_count[entry.topic] >= per_topic_limit:
                removed_by_topic_limit += 1
                continue
            selected.append(entry)
            selected_ids.add(entry.raw.id)
            topic_count[entry.topic] += 1
            wave2_added += 1

        for entry in ranked:
            if len(selected) >= min_items:
                break
            if entry.raw.id in selected_ids or entry.score < wave3_floor:
                continue
            if topic_count[entry.topic] >= per_topic_limit:
                removed_by_topic_limit += 1
                continue
            selected.append(entry)
            selected_ids.add(entry.raw.id)
            topic_count[entry.topic] += 1
            wave3_added += 1

        title = "Итоги дня: политика и экономика" if period_type == "daily" else "Итоги недели: ключевые изменения"
        lines: list[str] = []
        topic_breakdown: Counter[str] = Counter()

        for idx, entry in enumerate(selected, 1):
            item = entry.raw
            topic_breakdown[entry.topic] += 1
            snippet = self._make_dry_snippet(item.summary)
            safe_title = html.escape(item.title)
            safe_snippet = html.escape(snippet)
            safe_url = html.escape(item.url, quote=True)
            lines.append(
                f"{idx}) [{self._topic_ru(entry.topic)}] {safe_title}\n{safe_snippet}\n<a href=\"{safe_url}\">Источник</a>"
            )

        return DigestOutput(
            title=title,
            body="\n\n".join(lines),
            items_count=len(selected),
            source_breakdown=source_breakdown,
            topic_breakdown=dict(topic_breakdown),
            quality_metrics={
                "accepted_before_dedup": len(news),
                "deduplicated": len(deduped),
                "selected": len(selected),
                "selected_base_pass": base_selected_count,
                "selected_fallback": wave2_added + wave3_added,
                "selected_fallback_wave2": wave2_added,
                "selected_fallback_wave3": wave3_added,
                "duplicates_removed": max(0, len(news) - len(deduped)),
                "removed_by_topic_limit": removed_by_topic_limit,
                "fallback_wave2_min_score": wave2_floor,
                "fallback_wave3_min_score": wave3_floor,
            },
        )

    def _deduplicate(self, news: list[RankedNewsItem]) -> list[RankedNewsItem]:
        selected: list[RankedNewsItem] = []
        for candidate in sorted(news, key=lambda n: (n.raw.published_at, len(n.raw.summary)), reverse=True):
            duplicate = False
            for existing in selected:
                if candidate.dedup_exact_key and candidate.dedup_exact_key == existing.dedup_exact_key:
                    duplicate = True
                    break
                threshold = (
                    self.settings.dedup_threshold_same_source
                    if candidate.raw.source_id == existing.raw.source_id
                    else self.settings.dedup_threshold_cross_source
                )
                ratio = SequenceMatcher(None, candidate.dedup_similarity_key, existing.dedup_similarity_key).ratio()
                if ratio >= threshold:
                    duplicate = True
                    break
            if not duplicate:
                selected.append(candidate)
        return selected

    @staticmethod
    def _domain_path(url: str) -> str:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().removeprefix("www.")
        path = parsed.path.rstrip("/")
        if not domain:
            return ""
        return f"{domain}{path}"

    @staticmethod
    def _normalize(text: str) -> str:
        text = text.lower()
        text = re.sub(r"[^\w\s]", " ", text)
        tokens = [t for t in text.split() if t not in {"и", "в", "на", "по", "с", "к", "о", "для", "что"}]
        return " ".join(tokens)

    @staticmethod
    def _make_dry_snippet(summary: str) -> str:
        text = summary.replace("\n", " ").strip()
        text = re.sub(r"\s+", " ", text)
        if len(text) > 210:
            text = text[:207] + "..."
        return text

    @staticmethod
    def _topic_ru(topic: str) -> str:
        mapping = {
            "economy": "Экономика",
            "politics": "Политика",
            "international": "Международка",
            "conflict": "Конфликты",
        }
        return mapping.get(topic, "Прочее")
