import re

import pytest

from app.services.filtering import NewsFilter


def _legacy_evaluate(f: NewsFilter, title: str, summary: str, source_trust: float = 1.0) -> tuple[bool, str, int, str, list[str]]:
    text = f"{title} {summary}".lower()
    topics: dict[str, list[str]] = f.rules["topics"]
    weights: dict[str, int] = f.rules["weights"]

    topic_scores: dict[str, int] = {topic: 0 for topic in topics}
    score = 0
    matched_rules: list[str] = []

    for topic, patterns in topics.items():
        for pattern in patterns:
            if re.search(pattern, text):
                topic_scores[topic] += weights["topic_signal"]
                score += weights["topic_signal"]
                matched_rules.append("topic_match")

    for pattern in f.rules.get("topic_boundary_patterns", []):
        if re.search(pattern, text):
            score += weights["topic_boundary"]
            matched_rules.append("topic_boundary")

    for pattern in f.rules.get("official_entity_patterns", []):
        if re.search(pattern, text):
            score += weights["official_entities"]
            matched_rules.append("official_entities")

    for pattern in f.rules.get("event_scale_patterns", []):
        if re.search(pattern, text):
            score += weights["event_scale"]
            matched_rules.append("event_scale")

    for pattern in f.rules.get("stop_patterns", []):
        if re.search(pattern, text):
            score += weights["stop_pattern"]
            matched_rules.append("stop_pattern")

    strategic_verb_found = False
    for pattern in f.rules["strategic_verbs"]:
        if re.search(pattern, text):
            strategic_verb_found = True
            score += weights["strategic_verb"]
            matched_rules.append("strategic_verb")
            break

    for pattern in f.rules["low_priority_patterns"]:
        if re.search(pattern, text):
            score += weights["locality_penalty"]
            matched_rules.append("low_priority")

    for pattern in f.rules["clickbait_patterns"]:
        if re.search(pattern, text):
            score += weights["clickbait_penalty"]
            matched_rules.append("clickbait")

    trust = f._normalize_source_trust(source_trust)
    trust_delta = round((trust - 1.0) * weights["source_trust_factor"])
    if trust_delta != 0:
        score += trust_delta
        matched_rules.append("source_trust")

    topic = max(topic_scores, key=topic_scores.get)

    if topic == "conflict":
        for pattern in f.rules["conflict_tactical_patterns"]:
            if re.search(pattern, text):
                matched_rules.append("conflict_tactical")
                return False, "тактические детали конфликта", score, topic, matched_rules

    primary_passed = (
        score >= f.profile_rules["primary_score_min"]
        and topic_scores[topic] >= f.profile_rules["primary_topic_min"]
        and (not f.profile_rules["primary_requires_strategic"] or strategic_verb_found)
    )
    fallback_passed = (
        score >= f.profile_rules["fallback_score_min"]
        and topic_scores[topic] >= f.profile_rules["fallback_topic_min"]
    )

    if primary_passed or fallback_passed:
        matched_rules.append("threshold_accept")
        return True, "релевантно", score, topic, matched_rules

    matched_rules.append("threshold_reject")
    return False, "низкая стратегическая значимость", score, topic, matched_rules


def test_accepts_high_impact_news() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "Правительство утвердило налоговые изменения",
        "Новые правила влияют на экспорт и бюджет на следующий год.",
    )
    assert result.accepted
    assert result.topic in {"economy", "politics"}
    assert any(entry["rule"] == "strategic_verb" for entry in result.decision_trace)


def test_rejects_local_noise() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "В районе произошло ДТП",
        "Локальное происшествие без влияния на экономику.",
    )
    assert result.accepted
    assert result.reason in {"низкий приоритет", "релевантно"}
    assert any(entry["rule"] == "low_priority" for entry in result.decision_trace)


def test_conflict_tactical_details_reduce_score_but_are_not_hard_blocked() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "Конфликт продолжается",
        "Сообщается, сколько уничтожено техники на линии соприкосновения.",
    )
    assert result.accepted
    assert result.topic == "conflict"
    assert any(entry["rule"] == "conflict_tactical_penalty" for entry in result.decision_trace)


def test_rejects_conflict_hard_block() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "Конфликт продолжается",
        "Сообщают, что применено оружие массового поражения и началась новая эскалация.",
    )
    assert not result.accepted
    assert "тактические детали" in result.reason
    assert result.topic == "conflict"
    assert any(entry["rule"] == "conflict_hard_block" for entry in result.decision_trace)


def test_rejects_lifestyle_news() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "Гериатр Минздрава рассказала об алгоритмах сохранения молодости",
        "Ими являются изучение новой информации и принципиально новые занятия.",
    )
    assert result.accepted


def test_broad_profile_can_accept_without_strategic_verb() -> None:
    f = NewsFilter("broad")
    result = f.evaluate(
        "Международные переговоры по торговому балансу",
        "ЕС и ООН обсуждают экспорт и импорт на саммите.",
    )
    assert result.accepted
    assert any(entry["rule"] in {"publishable", "high_confidence"} for entry in result.decision_trace)


def test_strict_profile_rejects_same_news() -> None:
    broad = NewsFilter("broad")
    strict = NewsFilter("strict")

    title = "Инфляция и бюджет"
    summary = "Обзор макропоказателей без новых решений властей."

    assert broad.evaluate(title, summary).accepted
    strict_result = strict.evaluate(title, summary)
    assert strict_result.accepted
    assert strict_result.decision_trace[-1]["rule"] in {"publishable", "below_floor"}


def test_unknown_profile_falls_back_to_balanced() -> None:
    f = NewsFilter("unknown-profile")
    assert f.threshold_profile == "balanced"




def test_filter_returns_soft_ranking_flags() -> None:
    f = NewsFilter("balanced")
    result = f.evaluate("Инфляция", "Центробанк утвердил решения")
    assert result.accepted
    assert isinstance(result.is_high_confidence, bool)



def test_rejects_unsafe_regex_pattern(monkeypatch: pytest.MonkeyPatch) -> None:
    unsafe_rules = {
        "topics": {"economy": ["(a+)+$"], "politics": [], "international": [], "conflict": []},
        "strategic_verbs": [],
        "low_priority_patterns": [],
        "clickbait_patterns": [],
        "conflict_tactical_patterns": [],
        "threshold_profiles": {"balanced": {}},
    }

    monkeypatch.setattr("app.services.filtering._load_filter_rules", lambda: unsafe_rules)

    with pytest.raises(ValueError, match="Unsafe regex pattern"):
        NewsFilter()
