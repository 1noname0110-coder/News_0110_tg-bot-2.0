from __future__ import annotations

from dataclasses import dataclass, field

from app.models import RawNews


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
