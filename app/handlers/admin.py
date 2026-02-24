from __future__ import annotations

import json
import logging
from time import perf_counter
from datetime import datetime
from zoneinfo import ZoneInfo

from aiogram import Bot, Router
from aiogram.filters import Command
from aiogram.types import Message

from app.config import Settings
from app.db import get_session_factory
from app.periods import get_calendar_week_bounds
from app.repositories import (
    ALLOWED_SOURCE_TYPES,
    NewsRepository,
    SourceCreateStatus,
    SourceUpdateStatus,
    SourceRepository,
    normalize_http_url,
)
from app.services.collector import NewsCollector

router = Router(name="admin")
logger = logging.getLogger(__name__)


def _format_delivery_sla_block(success_rate: float, retry_count: int, errors: list[dict[str, str | int | None]]) -> str:
    error_lines = [
        f"digest={row.get('digest_id')} chunk={row.get('chunk_idx')} status={row.get('status')} {row.get('error_type') or '-'}: {row.get('error_message') or '-'}"
        for row in errors
    ] or ["Нет"]
    return (
        "Delivery SLA:\n"
        f"Success rate: {success_rate:.0%}\n"
        f"Retry count: {retry_count}\n"
        "Последние ошибки:\n" + "\n".join(error_lines)
    )

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

    source_type, name, raw_url = parts[1], parts[2], parts[3]
    if source_type not in ALLOWED_SOURCE_TYPES:
        allowed_values = ", ".join(sorted(ALLOWED_SOURCE_TYPES))
        await message.answer(f"Некорректный тип источника. Допустимые значения: {allowed_values}.")
        return

    url = normalize_http_url(raw_url)
    if not url:
        await message.answer("Некорректный URL. Разрешены только http:// и https:// ссылки.")
        return

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

    async with get_session_factory()() as session:
        repo = SourceRepository(session)
        result = await repo.create(source_type=source_type, name=name, url=url, meta=meta)

    if result.status == SourceCreateStatus.DUPLICATE_NAME:
        await message.answer("Источник с таким именем уже существует.")
        return
    if result.status != SourceCreateStatus.CREATED or result.source is None:
        logger.exception(
            "Ошибка сохранения источника: status=%s type=%s name=%s url=%s",
            result.status,
            source_type,
            name,
            url,
            exc_info=result.error,
        )
        await message.answer("Ошибка сохранения источника, проверьте логи")
        return

    await message.answer(f"Источник добавлен: #{result.source.id} {result.source.name} ({result.source.type})")


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
    async with get_session_factory()() as session:
        repo = SourceRepository(session)
        ok = await repo.remove(source_id)
    await message.answer("Источник удалён." if ok else "Источник не найден.")


@router.message(Command("listsources"))
async def list_sources(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    async with get_session_factory()() as session:
        repo = SourceRepository(session)
        sources = await repo.list_sources(active_only=None)

    if not sources:
        await message.answer("Источники не найдены.")
        return

    lines = [
        f"#{source.id} [{ 'on' if source.is_active else 'off' }] {source.type} {source.name} {source.url}"
        for source in sources
    ]
    await message.answer("Список источников:\n" + "\n".join(lines))


@router.message(Command("togglesource"))
async def toggle_source(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    parts = (message.text or "").split(maxsplit=2)
    if len(parts) != 3 or not parts[1].isdigit() or parts[2].lower() not in {"on", "off"}:
        await message.answer("Формат: /togglesource <id> <on|off>")
        return

    source_id = int(parts[1])
    enabled = parts[2].lower() == "on"
    async with get_session_factory()() as session:
        repo = SourceRepository(session)
        result = await repo.toggle(source_id=source_id, enabled=enabled)

    if result.status == SourceUpdateStatus.NOT_FOUND:
        await message.answer("Источник не найден.")
        return
    if result.status != SourceUpdateStatus.UPDATED or not result.source:
        await message.answer("Ошибка обновления источника.")
        return

    await message.answer(f"Источник #{result.source.id} {'включён' if result.source.is_active else 'выключен' }.")


@router.message(Command("editsource"))
async def edit_source(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    parts = (message.text or "").split(maxsplit=3)
    if len(parts) != 4 or not parts[1].isdigit():
        await message.answer("Формат: /editsource <id> <field> <value>")
        return

    source_id = int(parts[1])
    field = parts[2].strip().lower()
    raw_value = parts[3].strip()

    value: str | dict = raw_value
    if field == "meta":
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            await message.answer("Для поля meta требуется JSON-объект.")
            return
        if not isinstance(parsed, dict):
            await message.answer("Для поля meta требуется JSON-объект.")
            return
        value = parsed

    async with get_session_factory()() as session:
        repo = SourceRepository(session)
        result = await repo.update(source_id=source_id, field=field, value=value)

    if result.status == SourceUpdateStatus.NOT_FOUND:
        await message.answer("Источник не найден.")
        return
    if result.status == SourceUpdateStatus.INVALID_FIELD:
        await message.answer("Некорректное поле. Допустимо: name, type, url, meta.")
        return
    if result.status == SourceUpdateStatus.INVALID_VALUE:
        await message.answer("Некорректное значение поля.")
        return
    if result.status == SourceUpdateStatus.DUPLICATE_NAME:
        await message.answer("Источник с таким именем уже существует.")
        return
    if result.status != SourceUpdateStatus.UPDATED or not result.source:
        await message.answer("Ошибка обновления источника.")
        return

    await message.answer(f"Источник обновлён: #{result.source.id} {field}={getattr(result.source, field, 'updated')}")


@router.message(Command("checksource"))
async def check_source(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Формат: /checksource <id>")
        return

    source_id = int(parts[1])
    async with get_session_factory()() as session:
        repo = SourceRepository(session)
        source = await repo.get_by_id(source_id)

    if not source:
        await message.answer("Источник не найден.")
        return

    collector = NewsCollector(settings)
    started = perf_counter()
    items_count = 0
    first_error = "-"
    try:
        if source.type == "rss":
            items = await collector._fetch_rss(source)
        elif source.type == "site":
            items = await collector._fetch_site(source)
        elif source.type == "api":
            items = await collector._fetch_api(source)
        else:
            items = []
            first_error = f"unsupported source type: {source.type}"
        items_count = len(items)
    except Exception as exc:
        first_error = f"{type(exc).__name__}: {exc}"
    finally:
        elapsed_ms = (perf_counter() - started) * 1000
        await collector.aclose()

    await message.answer(
        f"Проверка источника #{source.id} ({source.name})\n"
        f"Время ответа: {elapsed_ms:.1f} ms\n"
        f"Найдено элементов: {items_count}\n"
        f"Первая ошибка: {first_error}"
    )


@router.message(Command("stat"))
async def stat_day(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    tz = ZoneInfo(settings.timezone)
    today = datetime.now(tz).date()

    async with get_session_factory()() as session:
        repo = NewsRepository(session, timezone=settings.timezone)
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

    async with get_session_factory()() as session:
        repo = NewsRepository(session, timezone=settings.timezone)
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
        f"Статистика недели [{week_start:%d.%m.%Y %H:%M}, {week_end:%d.%m.%Y %H:%M})\n"
        f"(фиксированный календарный полуинтервал)\n"
        f"Опубликовано сводок: {stats.published_count}\n"
        f"Отклонено новостей: {stats.rejected_count}\n\n"
        f"Использование источников:\n" + "\n".join(usage_lines) + "\n\n"
        f"Отклонения по источникам:\n" + "\n".join(reject_lines)
    )
    await message.answer(text)


@router.message(Command("statweek_live"))
async def stat_week_live(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    tz = ZoneInfo(settings.timezone)
    now_local = datetime.now(tz)
    week_start, _ = get_calendar_week_bounds(now_local)

    async with get_session_factory()() as session:
        repo = NewsRepository(session, timezone=settings.timezone)
        stats = await repo.compute_weekly_stats_live(week_start.date(), now_local.replace(tzinfo=None))

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
        f"Статистика недели [{week_start:%d.%m.%Y %H:%M}, {now_local:%d.%m.%Y %H:%M})\n"
        f"(с начала недели по текущий момент)\n"
        f"Опубликовано сводок: {stats.published_count}\n"
        f"Отклонено новостей: {stats.rejected_count}\n\n"
        f"Использование источников:\n" + "\n".join(usage_lines) + "\n\n"
        f"Отклонения по источникам:\n" + "\n".join(reject_lines)
    )
    await message.answer(text)




@router.message(Command("redeliver"))
async def redeliver(message: Message, bot: Bot, settings: Settings, digest_service) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    parts = (message.text or "").split()
    if len(parts) != 2 or not parts[1].isdigit():
        await message.answer("Формат: /redeliver <digest_id>")
        return

    digest_id = int(parts[1])
    async with get_session_factory()() as session:
        result = await digest_service.redeliver_digest(bot=bot, session=session, digest_id=digest_id)

    if result.get("status") == "not_found":
        await message.answer(f"Дайджест #{digest_id} не найден.")
        return

    await message.answer(
        f"Переотправка #{digest_id}: status={result.get('status')} sent={result.get('sent_chunks')}/{result.get('total_chunks')} failed={result.get('failed_chunks')}"
    )


@router.message(Command("quality"))
async def quality(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    tz = ZoneInfo(settings.timezone)
    today = datetime.now(tz).date()

    async with get_session_factory()() as session:
        repo = NewsRepository(session, timezone=settings.timezone)
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

    async with get_session_factory()() as session:
        delivery_repo = NewsRepository(session, timezone=settings.timezone)
        sla = await delivery_repo.get_delivery_sla_stats()

    sla_block = _format_delivery_sla_block(sla.success_rate, sla.retry_count, sla.last_errors)

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
        f"Причины отклонений:\n" + "\n".join(reasons) + "\n\n" + sla_block
    )
    await message.answer(text)


@router.message(Command("delivery"))
async def delivery(message: Message, settings: Settings) -> None:
    if not _is_admin(message, settings):
        await message.answer("Недостаточно прав.")
        return

    async with get_session_factory()() as session:
        repo = NewsRepository(session, timezone=settings.timezone)
        sla = await repo.get_delivery_sla_stats()

    await message.answer(_format_delivery_sla_block(sla.success_rate, sla.retry_count, sla.last_errors))
