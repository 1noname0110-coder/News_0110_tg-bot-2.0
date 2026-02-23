from __future__ import annotations

from datetime import datetime, timedelta


def get_calendar_day_bounds(now_local: datetime) -> tuple[datetime, datetime]:
    day_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)
    return day_start, day_end


def get_calendar_week_bounds(now_local: datetime) -> tuple[datetime, datetime]:
    """Возвращает фиксированный календарный полуинтервал недели [start, start+7d)."""
    week_start = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    week_end = week_start + timedelta(days=7)
    return week_start, week_end
