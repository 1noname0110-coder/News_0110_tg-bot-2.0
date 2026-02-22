from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from app.config import Settings
from app.db import AsyncSessionLocal
from app.periods import get_calendar_week_bounds
from app.repositories import NewsRepository, SourceRepository

router = Router(name="admin")


def _is_admin(message: Message, settings: Settings) -> bool:
    return bool(message.from_user and message.from_user.id in settings.admin_ids)


@router.message(Command("addsource"))
async def add_source(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    parts = (message.text or "").split(maxsplit=4)
    if len(parts) < 4:
        await message.answer("Формат: /addsource <rss|site|api> <имя> <url> [json_meta]")
        return

    source_type, name, url = parts[1], parts[2], parts[3]
    meta = {}
    if len(parts) == 5:
        try:
            meta = json.loads(parts[4])
            if not isinstance(meta, dict):
                await message.answer("meta должен быть JSON-объектом.")
                return
        except json.JSONDecodeError:
            await message.answer("Некорректный JSON в meta.")
            return

    async with AsyncSessionLocal() as session:
        repo = SourceRepository(session)
        source = await repo.create(source_type=source_type, name=name, url=url, meta=meta)

    if not source:
        await message.answer("Источник с таким именем уже существует.")
        return

    await message.answer(f"Источник добавлен: #{source.id} {source.name} ({source.type})")


@router.message(Command("removesource"))
async def remove_source(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Формат: /removesource <id>")
        return

    source_id = int(parts[1])
    async with AsyncSessionLocal() as session:
        repo = SourceRepository(session)
        ok = await repo.remove(source_id)
    await message.answer("Источник удалён." if ok else "Источник не найден.")


@router.message(Command("stat"))
async def stat_day(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    tz = ZoneInfo(settings.timezone)
    today = datetime.now(tz).date()

    async with AsyncSessionLocal() as session:
        repo = NewsRepository(session)
        stats = await repo.compute_daily_stats(today)

    total_sources = sum(stats.source_usage.values()) or 1
    total_rejections = sum(stats.rejection_breakdown.values()) or 1

    usage_lines = [
        f"Источник {sid}: {count} ({count / total_sources:.0%})"
        for sid, count in sorted(stats.source_usage.items(), key=lambda x: x[1], reverse=True)
    ] or ["Нет данных по источникам"]

    reject_lines = [
        f"Источник {sid}: {count} ({count / total_rejections:.0%})"
        for sid, count in sorted(stats.rejection_breakdown.items(), key=lambda x: x[1], reverse=True)
    ] or ["Отклонений нет"]

    text = (
        f"Статистика за {today:%d.%m.%Y}\n"
        f"Опубликовано сводок: {stats.published_count}\n"
        f"Отклонено новостей: {stats.rejected_count}\n\n"
        f"Использование источников:\n" + "\n".join(usage_lines) + "\n\n"
        f"Отклонения по источникам:\n" + "\n".join(reject_lines)
    )
    await message.answer(text)


@router.message(Command("statweek"))
async def stat_week(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    tz = ZoneInfo(settings.timezone)
    now_local = datetime.now(tz)
    week_start, week_end = get_calendar_week_bounds(now_local)

    async with AsyncSessionLocal() as session:
        repo = NewsRepository(session)
        stats = await repo.compute_weekly_stats(week_start.date())

    total_sources = sum(stats.source_usage.values()) or 1
    total_rejections = sum(stats.rejection_breakdown.values()) or 1

    usage_lines = [
        f"Источник {sid}: {count} ({count / total_sources:.0%})"
        for sid, count in sorted(stats.source_usage.items(), key=lambda x: x[1], reverse=True)
    ] or ["Нет данных по источникам"]

    reject_lines = [
        f"Источник {sid}: {count} ({count / total_rejections:.0%})"
        for sid, count in sorted(stats.rejection_breakdown.items(), key=lambda x: x[1], reverse=True)
    ] or ["Отклонений нет"]

    text = (
        f"Статистика недели с {week_start:%d.%m.%Y %H:%M} по {week_end:%d.%m.%Y %H:%M}\n"
        f"Опубликовано сводок: {stats.published_count}\n"
        f"Отклонено новостей: {stats.rejected_count}\n\n"
        f"Использование источников:\n" + "\n".join(usage_lines) + "\n\n"
        f"Отклонения по источникам:\n" + "\n".join(reject_lines)
    )
    await message.answer(text)


@router.message(Command("quality"))
async def quality(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    tz = ZoneInfo(settings.timezone)
    today = datetime.now(tz).date()

    async with AsyncSessionLocal() as session:
        repo = NewsRepository(session)
        stats = await repo.compute_daily_stats(today)

    qm = stats.quality_metrics or {}
    topic_dist = qm.get("topic_distribution", {})
    reject_reasons = qm.get("rejection_reasons", {})

    topics = [f"{k}: {v}" for k, v in sorted(topic_dist.items(), key=lambda x: x[1], reverse=True)] or ["Нет"]
    reasons = [f"{k}: {v}" for k, v in sorted(reject_reasons.items(), key=lambda x: x[1], reverse=True)] or ["Нет"]

    fetched_from_db = int(qm.get("fetched_from_db_total", qm.get("raw_total", 0)))
    rejected_by_filter = int(qm.get("rejected_by_filter_total", qm.get("rejected_total", 0)))
    removed_as_duplicates = int(qm.get("removed_as_duplicates_total", qm.get("duplicates_removed_total", 0)))
    removed_by_topic_limit = int(qm.get("removed_by_topic_limit_total", 0))
    published_items = int(qm.get("published_items_total", qm.get("selected_total", 0)))

    text = (
        f"Качество сводки за {today:%d.%m.%Y}\n"
        f"Воронка отбора:\n"
        f"1) Загружено из БД: {fetched_from_db}\n"
        f"2) Отброшено фильтром: {rejected_by_filter}\n"
        f"3) Удалено как дубликаты: {removed_as_duplicates}\n"
        f"4) Снято из-за лимита по теме: {removed_by_topic_limit}\n"
        f"5) Опубликовано пунктов: {published_items}\n\n"
        f"Acceptance rate: {qm.get('acceptance_rate', 0):.0%}\n"
        f"После дедупликации: {qm.get('deduplicated_total', 0)}\n"
        f"Выбрано всего: {qm.get('selected_total', 0)}\n\n"
        f"Распределение тем:\n" + "\n".join(topics) + "\n\n"
        f"Причины отклонений:\n" + "\n".join(reasons)
    )
    await message.answer(text)
