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
from app.models import RawNews
from app.services.filtering import FilterResult


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

    async def build_digest(self, period_type: str, news: list[tuple[RawNews, FilterResult]]) -> DigestOutput:
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

        source_breakdown = Counter(str(item.source_id) for item, _result in news)

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

    async def _build_with_llm(self, period_type: str, news: list[tuple[RawNews, FilterResult]]) -> dict[str, Any] | None:
        limit = 15 if period_type == "weekly" else 12
        prompt_items = "\n".join(
            [f"- {item.title}. {item.summary[:350]} (источник {item.source_id})" for item, _result in news[:80]]
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
        allowed_urls = {item.url.strip() for item, _result in news if item.url.strip()}
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
        news: list[tuple[RawNews, FilterResult]],
        source_breakdown: dict[str, int],
    ) -> DigestOutput:
        default_cap = 12 if period_type == "daily" else 15
        min_items = 5 if period_type == "daily" else 7
        publish_all_important = self.settings.publish_all_important
        per_topic_limit = self.settings.per_topic_limit_daily if period_type == "daily" else self.settings.per_topic_limit_weekly

        deduped = self._deduplicate(news)

        ranked = sorted(
            deduped,
            key=lambda item: (item[1].score, len(item[0].summary), item[0].published_at),
            reverse=True,
        )
        cap = default_cap
        if publish_all_important:
            cap = len(ranked)

        topic_count: dict[str, int] = defaultdict(int)
        selected: list[tuple[RawNews, str]] = []
        selected_ids: set[int] = set()
        removed_by_topic_limit = 0
        selected_in_base_pass = 0
        selected_in_fallback_wave2 = 0
        selected_in_fallback_wave3 = 0

        max_score = ranked[0][1].score if ranked else 0
        fallback_wave2_min_score = max(0, int(max_score * self._EXTRACTIVE_FALLBACK_WAVE2_SCORE_RATIO))
        fallback_wave3_min_score = max(0, int(max_score * self._EXTRACTIVE_FALLBACK_WAVE3_SCORE_RATIO))

        for item, result in ranked:
            if item.id in selected_ids:
                continue
            if topic_count[result.topic] >= per_topic_limit:
                removed_by_topic_limit += 1
                continue
            selected.append((item, result.topic))
            selected_ids.add(item.id)
            topic_count[result.topic] += 1
            selected_in_base_pass += 1
            if len(selected) >= cap:
                break

        if len(selected) < min_items:
            for item, result in ranked:
                if item.id in selected_ids:
                    continue
                if result.score < fallback_wave2_min_score:
                    continue
                if topic_count[result.topic] >= per_topic_limit:
                    continue
                selected.append((item, result.topic))
                selected_ids.add(item.id)
                topic_count[result.topic] += 1
                selected_in_fallback_wave2 += 1
                if len(selected) >= cap:
                    break

            if len(selected) < min_items:
                for item, result in ranked:
                    if item.id in selected_ids:
                        continue
                    if result.score < fallback_wave3_min_score:
                        continue
                    selected.append((item, result.topic))
                    selected_ids.add(item.id)
                    selected_in_fallback_wave3 += 1
                    if len(selected) >= min(min_items, len(ranked)):
                        break

        title = "Итоги дня: политика и экономика" if period_type == "daily" else "Итоги недели: ключевые изменения"
        lines: list[str] = []
        topic_breakdown: Counter[str] = Counter()

        for idx, (item, topic) in enumerate(selected, 1):
            topic_breakdown[topic] += 1
            snippet = self._make_dry_snippet(item.summary)
            safe_title = html.escape(item.title)
            safe_snippet = html.escape(snippet)
            safe_url = html.escape(item.url, quote=True)
            lines.append(
                f"{idx}) [{self._topic_ru(topic)}] {safe_title}\n{safe_snippet}\n<a href=\"{safe_url}\">Источник</a>"
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
                "selected_base_pass": selected_in_base_pass,
                "selected_fallback": selected_in_fallback_wave2 + selected_in_fallback_wave3,
                "selected_fallback_wave2": selected_in_fallback_wave2,
                "selected_fallback_wave3": selected_in_fallback_wave3,
                "duplicates_removed": max(0, len(news) - len(deduped)),
                "removed_by_topic_limit": removed_by_topic_limit,
                "fallback_wave2_min_score": fallback_wave2_min_score,
                "fallback_wave3_min_score": fallback_wave3_min_score,
            },
        )

    def _deduplicate(self, news: list[tuple[RawNews, FilterResult]]) -> list[tuple[RawNews, FilterResult]]:
        selected: list[tuple[RawNews, FilterResult]] = []
        summary_prefix_len = 180
        for candidate in sorted(news, key=lambda n: (n[0].published_at, len(n[0].summary)), reverse=True):
            candidate_item = candidate[0]
            candidate_key = self._dedup_similarity_key(candidate_item, summary_prefix_len)
            duplicate = False
            for existing in selected:
                existing_item = existing[0]
                if self._is_exact_duplicate(candidate_item, existing_item):
                    duplicate = True
                    break

                threshold = self._dedup_threshold(candidate_item, existing_item)
                ratio = SequenceMatcher(
                    None,
                    candidate_key,
                    self._dedup_similarity_key(existing_item, summary_prefix_len),
                ).ratio()
                if ratio >= threshold:
                    duplicate = True
                    break
            if not duplicate:
                selected.append(candidate)
        return selected

    def _dedup_similarity_key(self, item: RawNews, summary_prefix_len: int) -> str:
        title = self._normalize(item.title)
        summary_prefix = self._normalize(item.summary[:summary_prefix_len])
        return f"{title} {summary_prefix}".strip()

    def _dedup_threshold(self, candidate: RawNews, existing: RawNews) -> float:
        base = self.settings.dedup_similarity_threshold
        same_source_default = min(1.0, base)
        cross_source_default = min(1.0, max(base, base + 0.08))

        if candidate.source_id == existing.source_id:
            return self.settings.dedup_similarity_threshold_same_source or same_source_default
        return self.settings.dedup_similarity_threshold_cross_source or cross_source_default

    def _is_exact_duplicate(self, candidate: RawNews, existing: RawNews) -> bool:
        if candidate.url and existing.url and candidate.url == existing.url:
            return True
        if candidate.external_id and existing.external_id and candidate.external_id == existing.external_id:
            return True
        candidate_domain_path = self._domain_path(candidate.url)
        existing_domain_path = self._domain_path(existing.url)
        return bool(candidate_domain_path and candidate_domain_path == existing_domain_path)

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
