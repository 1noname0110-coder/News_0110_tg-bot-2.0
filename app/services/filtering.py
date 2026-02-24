from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


@dataclass
class FilterResult:
    accepted: bool
    reason: str
    score: int
    topic: str
    decision_trace: list[dict[str, Any]]


@lru_cache(maxsize=1)
def _load_filter_rules() -> dict[str, Any]:
    config_path = Path(__file__).resolve().parent.parent / "config" / "filter_rules.json"
    with config_path.open("r", encoding="utf-8") as f:
        return json.load(f)


class NewsFilter:
    def __init__(self, threshold_profile: str = "balanced"):
        self.rules = _load_filter_rules()
        available_profiles = self.rules["threshold_profiles"]
        self.threshold_profile = threshold_profile if threshold_profile in available_profiles else "balanced"
        self.profile_rules = available_profiles[self.threshold_profile]

    def _add_trace(
        self,
        trace: list[dict[str, Any]],
        rule: str,
        delta: int,
        *,
        pattern: str | None = None,
        topic: str | None = None,
    ) -> None:
        event: dict[str, Any] = {"rule": rule, "delta": delta}
        if pattern is not None:
            event["pattern"] = pattern
        if topic is not None:
            event["topic"] = topic
        trace.append(event)

    def evaluate(self, title: str, summary: str) -> FilterResult:
        text = f"{title} {summary}".lower()
        decision_trace: list[dict[str, Any]] = []

        topics: dict[str, list[str]] = self.rules["topics"]
        weights: dict[str, int] = self.rules["weights"]

        topic_scores: dict[str, int] = {topic: 0 for topic in topics}
        score = 0

        for topic, patterns in topics.items():
            for pattern in patterns:
                if re.search(pattern, text):
                    topic_scores[topic] += weights["topic_match"]
                    score += weights["topic_match"]
                    self._add_trace(
                        decision_trace,
                        "topic_match",
                        weights["topic_match"],
                        pattern=pattern,
                        topic=topic,
                    )

        strategic_verb_found = False
        for pattern in self.rules["strategic_verbs"]:
            if re.search(pattern, text):
                strategic_verb_found = True
                score += weights["strategic_verb"]
                self._add_trace(decision_trace, "strategic_verb", weights["strategic_verb"], pattern=pattern)
                break

        for pattern in self.rules["low_priority_patterns"]:
            if re.search(pattern, text):
                score += weights["low_priority"]
                self._add_trace(decision_trace, "low_priority", weights["low_priority"], pattern=pattern)

        for pattern in self.rules["clickbait_patterns"]:
            if re.search(pattern, text):
                score += weights["clickbait"]
                self._add_trace(decision_trace, "clickbait", weights["clickbait"], pattern=pattern)

        topic = max(topic_scores, key=topic_scores.get)

        if topic == "conflict":
            for pattern in self.rules["conflict_tactical_patterns"]:
                if re.search(pattern, text):
                    self._add_trace(decision_trace, "conflict_tactical", 0, pattern=pattern, topic=topic)
                    return FilterResult(False, "тактические детали конфликта", score, topic, decision_trace)

        primary_passed = (
            score >= self.profile_rules["primary_score_min"]
            and topic_scores[topic] >= self.profile_rules["primary_topic_min"]
            and (not self.profile_rules["primary_requires_strategic"] or strategic_verb_found)
        )
        fallback_passed = (
            score >= self.profile_rules["fallback_score_min"]
            and topic_scores[topic] >= self.profile_rules["fallback_topic_min"]
        )

        if primary_passed or fallback_passed:
            decision_trace.append(
                {
                    "rule": "threshold_accept",
                    "delta": 0,
                    "profile": self.threshold_profile,
                    "primary_passed": primary_passed,
                    "fallback_passed": fallback_passed,
                }
            )
            return FilterResult(True, "релевантно", score, topic, decision_trace)

        decision_trace.append(
            {
                "rule": "threshold_reject",
                "delta": 0,
                "profile": self.threshold_profile,
                "primary_passed": primary_passed,
                "fallback_passed": fallback_passed,
            }
        )
        return FilterResult(False, "низкая стратегическая значимость", score, topic, decision_trace)
