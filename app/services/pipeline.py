from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from app.models import RawNews
from app.services.filtering import FilterResult


FILTER_RESULT_ATTR = "_filter_result_snapshot"


@dataclass(slots=True)
class FilterResultSnapshot:
    score: int
    topic: str
    reason: str
    decision_trace: list[dict[str, object]]


def attach_filter_result(raw: RawNews, result: FilterResult) -> FilterResultSnapshot:
    snapshot = FilterResultSnapshot(
        score=result.score,
        topic=result.topic,
        reason=result.reason,
        decision_trace=list(result.decision_trace),
    )
    setattr(raw, FILTER_RESULT_ATTR, snapshot)
    return snapshot


def get_attached_filter_result(raw: RawNews) -> FilterResultSnapshot | None:
    value = getattr(raw, FILTER_RESULT_ATTR, None)
    if isinstance(value, FilterResultSnapshot):
        return value
    return None


@dataclass(slots=True)
class RankedNewsItem:
    raw: RawNews
    score: int
    topic: str
    accepted: bool
    reason: str
    decision_trace: list[dict[str, object]] = field(default_factory=list)
    is_high_confidence: bool = False
    publish_priority: int = 0
    dedup_exact_key: str = ""
    dedup_similarity_key: str = ""

    @classmethod
    def from_filter_result(
        cls,
        *,
        raw: RawNews,
        result: FilterResult,
        normalize_text: Callable[[str], str],
    ) -> "RankedNewsItem":
        attach_filter_result(raw, result)
        return cls(
            raw=raw,
            score=result.score,
            topic=result.topic,
            accepted=result.accepted,
            reason=result.reason,
            decision_trace=result.decision_trace,
            is_high_confidence=result.is_high_confidence,
            publish_priority=2 if result.is_high_confidence else 1,
            dedup_exact_key=f"{raw.source_id}:{raw.external_id or ''}:{raw.url or ''}",
            dedup_similarity_key=normalize_text(f"{raw.title} {raw.summary[:180]}"),
        )


@dataclass(slots=True)
class EvaluatedNewsItem:
    raw: RawNews
    filter_result: FilterResult

    def to_ranked(self, normalize_text: Callable[[str], str]) -> RankedNewsItem:
        return RankedNewsItem.from_filter_result(
            raw=self.raw,
            result=self.filter_result,
            normalize_text=normalize_text,
        )
