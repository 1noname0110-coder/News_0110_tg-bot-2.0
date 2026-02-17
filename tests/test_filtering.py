from app.services.filtering import NewsFilter


def test_accepts_high_impact_news() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "Правительство утвердило налоговые изменения",
        "Новые правила влияют на экспорт и бюджет на следующий год.",
    )
    assert result.accepted
    assert result.topic in {"economy", "politics"}


def test_rejects_local_noise() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "В районе произошло ДТП",
        "Локальное происшествие без влияния на экономику.",
    )
    assert not result.accepted


def test_rejects_tactical_conflict_details() -> None:
    f = NewsFilter()
    result = f.evaluate(
        "Конфликт продолжается",
        "Сообщается, сколько уничтожено техники на линии соприкосновения.",
    )
    assert not result.accepted
    assert "тактические детали" in result.reason
    assert result.topic == "conflict"
