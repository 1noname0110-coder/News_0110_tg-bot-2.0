from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class FilterResult:
    accepted: bool
    reason: str
    score: int
    topic: str


class NewsFilter:
    TOPIC_PATTERNS = {
        "economy": [
            r"ввп", r"инфляц", r"ставк[аи]", r"центробанк", r"бюджет", r"налог", r"безработиц", r"дефицит", r"профицит",
            r"промпроизвод", r"экспорт", r"импорт", r"госдолг", r"торгов[а-я ]*баланс", r"opec|опек", r"эмбарго", r"swift",
        ],
        "politics": [
            r"президент", r"правительств", r"парламент", r"госдум", r"совфед", r"указ", r"постановлен", r"закон", r"ратификац",
            r"совбез", r"кабинет министров", r"минист[её]рств",
        ],
        "international": [
            r"международ", r"саммит", r"оон", r"ес", r"нато", r"мид", r"санкц", r"переговор", r"двусторон", r"многосторон",
        ],
        "conflict": [
            r"конфликт", r"войн", r"операц", r"перемири", r"фронт", r"эскалац", r"деэскалац",
        ],
    }

    LOW_PRIORITY_PATTERNS = [
        r"дтп", r"пожар", r"задержан", r"убийств", r"шоу", r"знаменит", r"твиттер", r"telegram-канал", r"локальн", r"район",
        r"област[ьи]", r"матч", r"происшеств", r"муниципаль", r"местн[а-я ]*власт", r"бытов",
    ]

    CONFLICT_TACTICAL_PATTERNS = [
        r"уничтожено", r"ликвидирован", r"потер[ьяи]", r"число погиб", r"единиц техники", r"ранен[оы]", r"штурм", r"дронов уничтож",
    ]

    CLICKBAIT_PATTERNS = [r"шок", r"сенсац", r"срочно", r"невероят", r"взорвал[оаи]", r"скандал"]

    def evaluate(self, title: str, summary: str) -> FilterResult:
        text = f"{title} {summary}".lower()

        topic_scores: dict[str, int] = {topic: 0 for topic in self.TOPIC_PATTERNS}
        score = 0

        for topic, patterns in self.TOPIC_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, text):
                    topic_scores[topic] += 2
                    score += 2

        for pattern in self.LOW_PRIORITY_PATTERNS:
            if re.search(pattern, text):
                score -= 3

        for pattern in self.CLICKBAIT_PATTERNS:
            if re.search(pattern, text):
                score -= 2

        topic = max(topic_scores, key=topic_scores.get)

        if topic == "conflict":
            for pattern in self.CONFLICT_TACTICAL_PATTERNS:
                if re.search(pattern, text):
                    return FilterResult(False, "тактические детали конфликта", score, topic)

        if score >= 3 and topic_scores[topic] > 0:
            return FilterResult(True, "релевантно", score, topic)
        return FilterResult(False, "низкая стратегическая значимость", score, topic)
