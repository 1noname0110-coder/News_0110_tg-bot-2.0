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
        self.compiled_topics = {
            topic: self._compile_patterns(patterns, group_name=f"topics.{topic}")
            for topic, patterns in self.rules["topics"].items()
        }
        self.compiled_strategic_verbs = self._compile_patterns(
            self.rules["strategic_verbs"], group_name="strategic_verbs"
        )
        self.compiled_low_priority_patterns = self._compile_patterns(
            self.rules["low_priority_patterns"], group_name="low_priority_patterns"
        )
        self.compiled_clickbait_patterns = self._compile_patterns(
            self.rules["clickbait_patterns"], group_name="clickbait_patterns"
        )
        self.compiled_conflict_tactical_patterns = self._compile_patterns(
            self.rules["conflict_tactical_patterns"], group_name="conflict_tactical_patterns"
        )

    @classmethod
    def _compile_patterns(cls, patterns: list[str], *, group_name: str) -> list[re.Pattern[str]]:
        compiled: list[re.Pattern[str]] = []
        for pattern in patterns:
            cls._validate_regex_pattern(pattern, group_name=group_name)
            compiled.append(re.compile(pattern))
        return compiled

    @staticmethod
    def _validate_regex_pattern(pattern: str, *, group_name: str) -> None:
        nested_quantifier_re = re.compile(r"\((?:\?[:=!<][^)]*)?[^()]*[+*][^()]*\)[+*]")
        repeated_group_re = re.compile(r"\([^)]*\{\d+(?:,\d*)?\}[^)]*\)\{\d+(?:,\d*)?\}")
        unbounded_wildcard_re = re.compile(r"(?:\.\*|\.\+){2,}")

        max_pattern_length = 400
        if len(pattern) > max_pattern_length:
            raise ValueError(f"Unsafe regex pattern in {group_name}: pattern is too long")

        if nested_quantifier_re.search(pattern) or repeated_group_re.search(pattern) or unbounded_wildcard_re.search(pattern):
            raise ValueError(f"Unsafe regex pattern in {group_name}: {pattern!r}")

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

    @staticmethod
    def _normalize_source_trust(value: float) -> float:
        return min(1.5, max(0.5, value))

    def evaluate(self, title: str, summary: str, source_trust: float = 1.0) -> FilterResult:
        text = f"{title} {summary}".lower()
        decision_trace: list[dict[str, Any]] = []

        weights: dict[str, int] = self.rules["weights"]

        topic_scores: dict[str, int] = {topic: 0 for topic in self.compiled_topics}
        score = 0

        for topic, patterns in self.compiled_topics.items():
            for pattern in patterns:
                if pattern.search(text):
                    topic_scores[topic] += weights["topic_signal"]
                    score += weights["topic_signal"]
                    self._add_trace(
                        decision_trace,
                        "topic_match",
                        weights["topic_signal"],
                        pattern=pattern.pattern,
                        topic=topic,
                    )

        for pattern in self.rules.get("topic_boundary_patterns", []):
            if re.search(pattern, text):
                score += weights["topic_boundary"]
                self._add_trace(decision_trace, "topic_boundary", weights["topic_boundary"], pattern=pattern)

        for pattern in self.rules.get("official_entity_patterns", []):
            if re.search(pattern, text):
                score += weights["official_entities"]
                self._add_trace(decision_trace, "official_entities", weights["official_entities"], pattern=pattern)

        for pattern in self.rules.get("event_scale_patterns", []):
            if re.search(pattern, text):
                score += weights["event_scale"]
                self._add_trace(decision_trace, "event_scale", weights["event_scale"], pattern=pattern)

        for pattern in self.rules.get("stop_patterns", []):
            if re.search(pattern, text):
                score += weights["stop_pattern"]
                self._add_trace(decision_trace, "stop_pattern", weights["stop_pattern"], pattern=pattern)

        strategic_verb_found = False
        for pattern in self.compiled_strategic_verbs:
            if pattern.search(text):
                strategic_verb_found = True
                score += weights["strategic_verb"]
                self._add_trace(
                    decision_trace,
                    "strategic_verb",
                    weights["strategic_verb"],
                    pattern=pattern.pattern,
                )
                break

        for pattern in self.compiled_low_priority_patterns:
            if pattern.search(text):
                score += weights["locality_penalty"]
                self._add_trace(
                    decision_trace,
                    "low_priority",
                    weights["locality_penalty"],
                    pattern=pattern.pattern,
                )

        for pattern in self.compiled_clickbait_patterns:
            if pattern.search(text):
                score += weights["clickbait_penalty"]
                self._add_trace(
                    decision_trace,
                    "clickbait",
                    weights["clickbait_penalty"],
                    pattern=pattern.pattern,
                )

        trust = self._normalize_source_trust(source_trust)
        trust_delta = round((trust - 1.0) * weights["source_trust_factor"])
        if trust_delta != 0:
            score += trust_delta
            self._add_trace(decision_trace, "source_trust", trust_delta)

        topic = max(topic_scores, key=topic_scores.get)

        if topic == "conflict":
            for pattern in self.compiled_conflict_tactical_patterns:
                if pattern.search(text):
                    self._add_trace(decision_trace, "conflict_tactical", 0, pattern=pattern.pattern, topic=topic)
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
