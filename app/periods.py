from __future__ import annotations

from datetime import datetime, timedelta


def get_calendar_week_bounds(now_local: datetime) -> tuple[datetime, datetime]:
    week_start = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    return week_start, now_local

