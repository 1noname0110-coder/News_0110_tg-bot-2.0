from app.services.filtering import NewsFilter


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
    assert not result.accepted
    assert any(entry["rule"] == "low_priority" for entry in result.decision_trace)


def test_rejects_tactical_conflict_details() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "Конфликт продолжается",
        "Сообщается, сколько уничтожено техники на линии соприкосновения.",
    )
    assert not result.accepted
    assert "тактические детали" in result.reason
    assert result.topic == "conflict"
    assert any(entry["rule"] == "conflict_tactical" for entry in result.decision_trace)


def test_rejects_lifestyle_news() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "Гериатр Минздрава рассказала об алгоритмах сохранения молодости",
        "Ими являются изучение новой информации и принципиально новые занятия.",
    )
    assert not result.accepted


def test_broad_profile_can_accept_without_strategic_verb() -> None:
    f = NewsFilter("broad")
    result = f.evaluate(
        "Международные переговоры по торговому балансу",
        "ЕС и ООН обсуждают экспорт и импорт на саммите.",
    )
    assert result.accepted
    assert any(entry["rule"] == "threshold_accept" for entry in result.decision_trace)


def test_strict_profile_rejects_same_news() -> None:
    broad = NewsFilter("broad")
    strict = NewsFilter("strict")

    title = "Инфляция и бюджет"
    summary = "Обзор макропоказателей без новых решений властей."

    assert broad.evaluate(title, summary).accepted
    strict_result = strict.evaluate(title, summary)
    assert not strict_result.accepted
    assert strict_result.decision_trace[-1]["rule"] == "threshold_reject"


def test_unknown_profile_falls_back_to_balanced() -> None:
    f = NewsFilter("unknown-profile")
    assert f.threshold_profile == "balanced"
