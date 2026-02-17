from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher

from openai import AsyncOpenAI

from app.config import Settings
from app.models import RawNews
from app.services.filtering import NewsFilter


@dataclass
class DigestOutput:
    title: str
    body: str
    items_count: int
    source_breakdown: dict[str, int]
    topic_breakdown: dict[str, int]
    quality_metrics: dict[str, int | float]


class DigestSummarizer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.filter = NewsFilter()
        self.client = None
        if settings.llm_enabled and settings.llm_api_key:
            self.client = AsyncOpenAI(api_key=settings.llm_api_key, base_url=settings.llm_base_url)

    async def build_digest(self, period_type: str, news: list[RawNews]) -> DigestOutput:
        if not news:
            title = "Сводка: значимых событий не выявлено"
            return DigestOutput(
                title=title,
                body="Новых материалов стратегического уровня за период не найдено.",
                items_count=0,
                source_breakdown={},
                topic_breakdown={},
                quality_metrics={"accepted_before_dedup": 0, "deduplicated": 0, "selected": 0},
            )

        source_breakdown = Counter(str(n.source_id) for n in news)

        if self.client:
            generated = await self._build_with_llm(period_type, news)
            return DigestOutput(
                title=generated["title"],
                body=generated["body"],
                items_count=min(len(news), 15 if period_type == "weekly" else 12),
                source_breakdown=dict(source_breakdown),
                topic_breakdown={},
                quality_metrics={"accepted_before_dedup": len(news), "deduplicated": len(news), "selected": min(len(news), 15 if period_type == "weekly" else 12)},
            )

        return self._build_extractive(period_type, news, dict(source_breakdown))

    async def _build_with_llm(self, period_type: str, news: list[RawNews]) -> dict:
        limit = 15 if period_type == "weekly" else 12
        prompt_items = "\n".join(
            [f"- {n.title}. {n.summary[:350]} (источник {n.source_id})" for n in news[:80]]
        )
        system = (
            "Ты редактор сухой аналитической сводки. Пиши только факты, без эмоций, пропаганды и кликбейта. "
            "Исключай локальные бытовые события и тактические детали конфликтов."
        )
        user = (
            f"Сформируй {period_type} сводку на русском языке. "
            f"Верни строго формат:\nЗАГОЛОВОК: ...\nПУНКТЫ:\n1) ...\n"
            f"Сделай от 5 до {limit} пунктов, каждый пункт 1-3 строки.\n"
            f"Новости:\n{prompt_items}"
        )

        response = await self.client.chat.completions.create(
            model=self.settings.llm_model,
            temperature=0.1,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        )
        content = response.choices[0].message.content or ""
        title = "Ежедневная сводка" if period_type == "daily" else "Недельная сводка"
        if "ЗАГОЛОВОК:" in content:
            title = content.split("ЗАГОЛОВОК:", 1)[1].splitlines()[0].strip()
        body = content.split("ПУНКТЫ:")[-1].strip() or content
        return {"title": title, "body": body}

    def _build_extractive(self, period_type: str, news: list[RawNews], source_breakdown: dict[str, int]) -> DigestOutput:
        cap = 12 if period_type == "daily" else 15
        min_items = 5 if period_type == "daily" else 7
        per_topic_limit = self.settings.per_topic_limit_daily if period_type == "daily" else self.settings.per_topic_limit_weekly

        deduped = self._deduplicate(news)

        ranked = sorted(
            deduped,
            key=lambda n: (self.filter.evaluate(n.title, n.summary).score, len(n.summary), n.published_at),
            reverse=True,
        )

        topic_count: dict[str, int] = defaultdict(int)
        selected: list[tuple[RawNews, str]] = []
        for item in ranked:
            topic = self.filter.evaluate(item.title, item.summary).topic
            if topic_count[topic] >= per_topic_limit:
                continue
            selected.append((item, topic))
            topic_count[topic] += 1
            if len(selected) >= cap:
                break

        if len(selected) < min_items:
            for item in ranked:
                if any(item.id == chosen.id for chosen, _ in selected):
                    continue
                topic = self.filter.evaluate(item.title, item.summary).topic
                selected.append((item, topic))
                if len(selected) >= min_items:
                    break

        title = "Итоги дня: политика и экономика" if period_type == "daily" else "Итоги недели: ключевые изменения"
        lines: list[str] = []
        topic_breakdown: Counter[str] = Counter()

        for idx, (item, topic) in enumerate(selected, 1):
            topic_breakdown[topic] += 1
            snippet = self._make_dry_snippet(item.summary)
            lines.append(f"{idx}) [{self._topic_ru(topic)}] {item.title}\n{snippet}")

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
                "duplicates_removed": max(0, len(news) - len(deduped)),
            },
        )

    def _deduplicate(self, news: list[RawNews]) -> list[RawNews]:
        selected: list[RawNews] = []
        for candidate in sorted(news, key=lambda n: (n.published_at, len(n.summary)), reverse=True):
            norm = self._normalize(candidate.title)
            duplicate = False
            for existing in selected:
                ratio = SequenceMatcher(None, norm, self._normalize(existing.title)).ratio()
                if ratio >= self.settings.dedup_similarity_threshold:
                    duplicate = True
                    break
            if not duplicate:
                selected.append(candidate)
        return selected

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
