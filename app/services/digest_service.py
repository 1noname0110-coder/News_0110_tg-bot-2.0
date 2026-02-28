from __future__ import annotations

import asyncio
import logging
import re
from collections import Counter
from datetime import datetime
from time import perf_counter
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
)
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.models import RawNews
from app.periods import get_calendar_day_bounds, get_calendar_week_bounds
from app.repositories import DigestDeliveryStatus, NewsRepository, SourceRepository, source_trust_coefficient
from app.services.collector import NewsCollector
from app.services.filtering import NewsFilter
from app.services.pipeline import EvaluatedNewsItem, attach_filter_result
from app.services.summarizer import DigestSummarizer

logger = logging.getLogger(__name__)


class DigestService:
    TELEGRAM_MESSAGE_MAX = 4096
    TELEGRAM_MAX_CHARS = TELEGRAM_MESSAGE_MAX  # legacy alias
    DIGEST_TITLE_MAX_CHARS = 500
    CHUNK_HEADER_SEPARATOR = "\n\n"
    SEND_RETRY_ATTEMPTS = 4
    SEND_RETRY_DELAY_SECONDS = 2
    COLLECT_MAX_CONCURRENCY = 8

    def __init__(self, settings: Settings):
        self.settings = settings
        self.collector = NewsCollector(settings)
        self.filter = NewsFilter("balanced")
        self.summarizer = DigestSummarizer(settings)

    async def aclose(self) -> None:
        logger.info("Остановка DigestService: закрытие внешних клиентов")
        await self.collector.aclose()

        client = self.summarizer.client
        if client is not None:
            aclose = getattr(client, "aclose", None)
            if callable(aclose):
                await aclose()
            else:
                close = getattr(client, "close", None)
                if callable(close):
                    maybe_coro = close()
                    if asyncio.iscoroutine(maybe_coro):
                        await maybe_coro

        logger.info("DigestService остановлен")

    async def collect_and_store(self, session: AsyncSession) -> None:
        source_repo = SourceRepository(session)
        news_repo = NewsRepository(session, timezone=self.settings.timezone)

        sources = await source_repo.list_active()
        semaphore = asyncio.Semaphore(self.COLLECT_MAX_CONCURRENCY)

        async def _collect_for_source(source):  # noqa: ANN001
            started_at = perf_counter()
            try:
                async with semaphore:
                    items = await self.collector.collect_from_source(source)
            except Exception:
                elapsed = perf_counter() - started_at
                logger.exception(
                    "Сбор источника завершился ошибкой: source_id=%s source_name=%s duration=%.2fs",
                    source.id,
                    source.name,
                    elapsed,
                )
                return source, []

            elapsed = perf_counter() - started_at
            logger.info(
                "Сбор источника завершен: source_id=%s source_name=%s duration=%.2fs items=%s",
                source.id,
                source.name,
                elapsed,
                len(items),
            )
            return source, items

        batches = await asyncio.gather(*(_collect_for_source(source) for source in sources))
        for source, items in batches:
            if items:
                await news_repo.add_raw_news(items)
            if source.type == "site":
                await source_repo.update_meta(source.id, source.meta)

    async def publish_daily(self, bot: Bot, session: AsyncSession) -> None:
        tz = ZoneInfo(self.settings.timezone)
        now_local = datetime.now(tz)
        day_start_local, day_end_local = get_calendar_day_bounds(now_local)

        await self._publish_period(
            bot=bot,
            session=session,
            period_type="daily",
            start_dt=day_start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            end_dt=day_end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        )

    async def republish_period(
        self,
        bot: Bot,
        session: AsyncSession,
        period_type: str,
        start_dt: datetime,
        end_dt: datetime,
    ) -> None:
        await self._publish_period(
            bot=bot,
            session=session,
            period_type=period_type,
            start_dt=start_dt,
            end_dt=end_dt,
            allow_republish=True,
        )

    async def redeliver_digest(self, bot: Bot, session: AsyncSession, digest_id: int) -> dict[str, int | str | list[int]]:
        news_repo = NewsRepository(session, timezone=self.settings.timezone)
        digest = await news_repo.get_published_digest(digest_id)
        if digest is None:
            return {"status": "not_found", "total_chunks": 0, "sent_chunks": 0, "failed_chunks": []}

        sent_chunks = await news_repo.get_successfully_delivered_chunks(digest_id)
        await news_repo.transition_digest_status(digest_id, status=DigestDeliveryStatus.SENDING)
        send_result = await self._send_digest_messages(
            bot,
            digest.title,
            digest.body,
            news_repo=news_repo,
            digest_id=digest_id,
            skip_chunk_indexes=sent_chunks,
        )
        final_status_value = str(send_result.get("status", DigestDeliveryStatus.FAILED.value))
        if final_status_value == "success":
            final_status_value = DigestDeliveryStatus.SENT.value
        final_status = DigestDeliveryStatus(final_status_value)
        await news_repo.transition_digest_status(
            digest_id,
            status=final_status,
            delivery_payload=send_result,
        )
        return send_result

    async def publish_weekly(self, bot: Bot, session: AsyncSession) -> None:
        tz = ZoneInfo(self.settings.timezone)
        now_local = datetime.now(tz)
        week_start_local, week_end_local = get_calendar_week_bounds(now_local)

        await self._publish_period(
            bot=bot,
            session=session,
            period_type="weekly",
            start_dt=week_start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            end_dt=week_end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        )


    async def publish_daily_preview(self, bot: Bot, session: AsyncSession) -> None:
        tz = ZoneInfo(self.settings.timezone)
        now_local = datetime.now(tz)
        day_start_local, _ = get_calendar_day_bounds(now_local)

        await self._publish_period(
            bot=bot,
            session=session,
            period_type="daily",
            start_dt=day_start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            end_dt=now_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            allow_republish=True,
        )

    async def publish_weekly_preview(self, bot: Bot, session: AsyncSession) -> None:
        tz = ZoneInfo(self.settings.timezone)
        now_local = datetime.now(tz)
        week_start_local, _ = get_calendar_week_bounds(now_local)

        await self._publish_period(
            bot=bot,
            session=session,
            period_type="weekly",
            start_dt=week_start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            end_dt=now_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
            allow_republish=True,
        )

    async def _publish_period(
        self,
        bot: Bot,
        session: AsyncSession,
        period_type: str,
        start_dt: datetime,
        end_dt: datetime,
        allow_republish: bool = False,
    ) -> None:
        news_repo = NewsRepository(session, timezone=self.settings.timezone)

        if not allow_republish and await news_repo.is_period_already_published(period_type, start_dt, end_dt):
            logger.info(
                "Пропуск публикации: период уже опубликован period_type=%s period_start=%s period_end=%s",
                period_type,
                start_dt,
                end_dt,
            )
            return

        period_limit = self.settings.max_period_news
        sources = await SourceRepository(session).list_active()
        source_map = {source.id: source for source in sources}
        raw_items = await news_repo.fetch_period_news(start_dt, end_dt, limit=period_limit)
        evaluated_items: list[EvaluatedNewsItem] = []
        reject_entries: list[tuple[int, int, str]] = []
        rejection_reasons = Counter()
        filter_rule_hits = Counter()
        filter_rule_score_impact = Counter()
        suspicious_rejections = 0

        for item in raw_items:
            source = source_map.get(item.source_id)
            source_trust = source_trust_coefficient(getattr(source, "meta", {}))
            result = self.filter.evaluate(item.title, item.summary, source_trust=source_trust)
            attach_filter_result(item, result)
            for trace_entry in getattr(result, "decision_trace", []):
                rule = str(trace_entry.get("rule", "unknown"))
                filter_rule_hits[rule] += 1
                filter_rule_score_impact[rule] += int(trace_entry.get("delta", 0))

            if not result.accepted:
                rejection_reasons[result.reason] += 1
                reject_entries.append((item.id, item.source_id, result.reason))
                has_suspicious_rule = any(
                    entry.get("rule") in {"low_priority", "clickbait", "stop_pattern"}
                    for entry in getattr(result, "decision_trace", [])
                )
                if has_suspicious_rule:
                    suspicious_rejections += 1
                continue

            evaluated_items.append(EvaluatedNewsItem(raw=item, filter_result=result))

        await news_repo.reject_many(reject_entries)

        digest = await self.summarizer.build_digest(period_type, evaluated_items)
        quality_metrics = dict(digest.quality_metrics)
        accepted_total = int(quality_metrics.get("accepted_before_dedup", len(evaluated_items)))
        rejected_total = max(0, len(raw_items) - accepted_total)

        quality_metrics["fetched_from_db"] = len(raw_items)
        quality_metrics["raw_total"] = len(raw_items)
        quality_metrics["accepted_total"] = accepted_total
        quality_metrics["rejected_total"] = rejected_total
        quality_metrics["rejected_by_filter"] = rejected_total
        quality_metrics["removed_as_duplicates"] = int(quality_metrics.get("duplicates_removed", 0))
        quality_metrics["removed_by_topic_limit"] = int(quality_metrics.get("removed_by_topic_limit", 0))
        quality_metrics["published_items"] = digest.items_count
        quality_metrics["rejection_reasons"] = dict(rejection_reasons)
        quality_metrics["filter_rule_hits"] = dict(filter_rule_hits)
        quality_metrics["filter_rule_score_impact"] = dict(filter_rule_score_impact)
        quality_metrics["suspicious_rules_rejection_share"] = (
            suspicious_rejections / quality_metrics["rejected_total"] if quality_metrics["rejected_total"] else 0.0
        )

        quality_metrics["delivery_status"] = DigestDeliveryStatus.PREPARED.value
        prepared = await news_repo.publish_digest(
            period_type=period_type,
            period_start=start_dt,
            period_end=end_dt,
            title=digest.title,
            body=digest.body,
            items_count=digest.items_count,
            source_breakdown=digest.source_breakdown,
            topic_breakdown=digest.topic_breakdown,
            quality_metrics=quality_metrics,
            status=DigestDeliveryStatus.PREPARED,
        )

        await news_repo.transition_digest_status(prepared.id, status=DigestDeliveryStatus.SENDING)
        send_result = await self._send_digest_messages(
            bot,
            digest.title,
            digest.body,
            news_repo=news_repo,
            digest_id=prepared.id,
        )
        final_status_value = str(send_result.get("status", DigestDeliveryStatus.FAILED.value))
        if final_status_value == "success":
            final_status_value = DigestDeliveryStatus.SENT.value
        final_status = DigestDeliveryStatus(final_status_value)
        await news_repo.transition_digest_status(
            prepared.id,
            status=final_status,
            delivery_payload=send_result,
        )

    async def _send_digest_messages(
        self,
        bot: Bot,
        title: str,
        body: str,
        *,
        news_repo: NewsRepository,
        digest_id: int | None,
        skip_chunk_indexes: set[int] | None = None,
    ) -> dict[str, int | str | list[int]]:
        chunks = self._split_body(body)
        total = len(chunks)
        sent_chunks = 0
        failed_chunks: list[int] = []
        skipped_chunks: list[int] = []
        already_sent = set(skip_chunk_indexes or set())
        safe_title = self._truncate_text(title.strip(), self.DIGEST_TITLE_MAX_CHARS)

        for idx, chunk in enumerate(chunks, 1):
            if idx in already_sent:
                skipped_chunks.append(idx)
                sent_chunks += 1
                logger.info("Дайджест: пропуск уже доставленного чанка %s/%s", idx, total)
                continue
            header = self._build_chunk_header(safe_title, idx, total)
            budget = self._message_body_budget(header)
            if len(chunk) > budget:
                chunk = self._fit_chunk_to_budget(chunk, budget)

            sent = False
            logger.info(
                "Дайджест: подготовлен чанк %s/%s, длина тела=%s, длина с заголовком=%s",
                idx,
                total,
                len(chunk),
                len(header + chunk),
            )

            for attempt in range(1, self.SEND_RETRY_ATTEMPTS + 1):
                await news_repo.record_chunk_attempt_and_transition(
                    digest_id=digest_id,
                    chunk_idx=idx,
                    attempt_status="attempt",
                    digest_status=DigestDeliveryStatus.SENDING,
                )
                logger.info("Дайджест: попытка отправки чанка %s/%s (попытка %s/%s)", idx, total, attempt, self.SEND_RETRY_ATTEMPTS)
                try:
                    await bot.send_message(chat_id=self.settings.channel_id, text=header + chunk)
                    await news_repo.record_chunk_attempt_and_transition(
                        digest_id=digest_id,
                        chunk_idx=idx,
                        attempt_status="success",
                        digest_status=DigestDeliveryStatus.SENDING,
                    )
                    sent_chunks += 1
                    sent = True
                    logger.info(
                        "Дайджест: чанк %s/%s отправлен успешно (попытка %s)",
                        idx,
                        total,
                        attempt,
                    )
                    break
                except TelegramRetryAfter as exc:
                    if attempt >= self.SEND_RETRY_ATTEMPTS:
                        logger.error(
                            "Дайджест: чанк %s/%s не отправлен после %s попыток (flood control, retry_after=%s)",
                            idx,
                            total,
                            attempt,
                            exc.retry_after,
                        )
                        await news_repo.record_chunk_attempt_and_transition(
                            digest_id=digest_id,
                            chunk_idx=idx,
                            attempt_status="failed",
                            error_type=type(exc).__name__,
                            error_message=str(exc),
                        )
                        break
                    retry_after = max(int(exc.retry_after), self.SEND_RETRY_DELAY_SECONDS)
                    logger.warning(
                        "Дайджест: чанк %s/%s flood control (retry_after=%s), повтор через %ss (попытка %s/%s)",
                        idx,
                        total,
                        exc.retry_after,
                        retry_after,
                        attempt,
                        self.SEND_RETRY_ATTEMPTS,
                    )
                    await news_repo.record_chunk_attempt_and_transition(
                        digest_id=digest_id,
                        chunk_idx=idx,
                        attempt_status="retry",
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    await asyncio.sleep(retry_after)
                except (TelegramNetworkError, TelegramServerError) as exc:
                    if attempt >= self.SEND_RETRY_ATTEMPTS:
                        logger.error(
                            "Дайджест: чанк %s/%s не отправлен после %s попыток (%s)",
                            idx,
                            total,
                            attempt,
                            type(exc).__name__,
                        )
                        await news_repo.record_chunk_attempt_and_transition(
                            digest_id=digest_id,
                            chunk_idx=idx,
                            attempt_status="failed",
                            error_type=type(exc).__name__,
                            error_message=str(exc),
                        )
                        break
                    logger.warning(
                        "Дайджест: чанк %s/%s ошибка %s, повтор через %ss (попытка %s/%s)",
                        idx,
                        total,
                        type(exc).__name__,
                        self.SEND_RETRY_DELAY_SECONDS,
                        attempt,
                        self.SEND_RETRY_ATTEMPTS,
                    )
                    await news_repo.record_chunk_attempt_and_transition(
                        digest_id=digest_id,
                        chunk_idx=idx,
                        attempt_status="retry",
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    await asyncio.sleep(self.SEND_RETRY_DELAY_SECONDS)
                except (TelegramBadRequest, TelegramForbiddenError) as exc:
                    logger.error(
                        "Дайджест: чанк %s/%s не отправлен из-за невосстановимой ошибки %s",
                        idx,
                        total,
                        type(exc).__name__,
                    )
                    await news_repo.record_chunk_attempt_and_transition(
                        digest_id=digest_id,
                        chunk_idx=idx,
                        attempt_status="failed",
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    break
                except TelegramAPIError as exc:
                    logger.exception("Дайджест: чанк %s/%s не отправлен из-за ошибки Telegram API", idx, total)
                    await news_repo.record_chunk_attempt_and_transition(
                        digest_id=digest_id,
                        chunk_idx=idx,
                        attempt_status="failed",
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                    break

            if not sent:
                failed_chunks.append(idx)

        if sent_chunks == total:
            status = "sent"
        elif sent_chunks > 0:
            status = "partial"
        else:
            status = "failed"
        logger.info(
            "Дайджест: отправка завершена, статус=%s, отправлено %s/%s, ошибки в чанках=%s",
            status,
            sent_chunks,
            total,
            failed_chunks,
        )
        return {
            "status": status,
            "total_chunks": total,
            "sent_chunks": sent_chunks,
            "failed_chunks": failed_chunks,
            "skipped_chunks": skipped_chunks,
        }

    def _split_body(self, body: str) -> list[str]:
        chunk_limit = self._message_limit
        if len(body) <= chunk_limit:
            return [body]

        blocks = self._extract_logical_blocks(body)
        chunks: list[str] = []
        current = ""

        for block in blocks:
            if len(block) > chunk_limit:
                oversized_parts = self._split_oversized_block(block)
            else:
                oversized_parts = [block]

            for part in oversized_parts:
                candidate = part if not current else f"{current}\n\n{part}"
                if len(candidate) <= chunk_limit and self._has_balanced_anchor_tags(candidate):
                    current = candidate
                    continue

                if current:
                    chunks.append(current)
                current = part

        if current:
            chunks.append(current)

        valid_chunks = [chunk for chunk in chunks if chunk.strip() and self._has_balanced_anchor_tags(chunk)]
        return valid_chunks or [body]

    def _extract_logical_blocks(self, body: str) -> list[str]:
        blocks: list[str] = []
        for section in body.split("\n\n"):
            section = section.strip()
            if not section:
                continue

            lines = [line.strip() for line in section.split("\n") if line.strip()]
            if len(lines) <= 1:
                blocks.append(section)
                continue

            blocks.extend(lines)
        return blocks or [body]

    def _split_oversized_block(self, block: str) -> list[str]:
        chunk_limit = self._message_limit
        parts: list[str] = []
        current = ""

        for token in self._tokenize_block(block):
            candidate = f"{current}{token}"
            if len(candidate) <= chunk_limit:
                current = candidate
                continue

            if current and self._has_balanced_anchor_tags(current):
                parts.append(current.strip())
                current = token.lstrip()
            else:
                current = candidate

        if current.strip():
            parts.append(current.strip())

        return parts or [block]

    @staticmethod
    def _tokenize_block(block: str) -> list[str]:
        tokens: list[str] = []
        cursor = 0
        for match in re.finditer(r"<a\b[^>]*>.*?</a>", block, flags=re.IGNORECASE | re.DOTALL):
            if match.start() > cursor:
                tokens.extend(re.findall(r"\S+\s*", block[cursor:match.start()]))
            tokens.append(match.group(0))
            cursor = match.end()

        if cursor < len(block):
            tokens.extend(re.findall(r"\S+\s*", block[cursor:]))

        return tokens or [block]

    @staticmethod
    def _has_balanced_anchor_tags(text: str) -> bool:
        openings = re.findall(r"<a\b[^>]*>", text)
        closings = re.findall(r"</a>", text)
        return len(openings) == len(closings)

    def _build_chunk_header(self, title: str, idx: int, total: int) -> str:
        suffix = "" if idx == 1 else f" (продолжение {idx}/{total})"
        max_header_text_len = self._message_limit - len(self.CHUNK_HEADER_SEPARATOR) - 1
        allowed_title_len = max(1, max_header_text_len - len(suffix))
        safe_title = self._truncate_text(title, allowed_title_len)
        return f"{safe_title}{suffix}{self.CHUNK_HEADER_SEPARATOR}"

    @property
    def _message_limit(self) -> int:
        legacy_limit = getattr(self, "TELEGRAM_MAX_CHARS", self.TELEGRAM_MESSAGE_MAX)
        return int(legacy_limit)

    def _message_body_budget(self, header: str) -> int:
        return max(0, self._message_limit - len(header))

    def _fit_chunk_to_budget(self, chunk: str, budget: int) -> str:
        if budget <= 0:
            return ""
        if len(chunk) <= budget:
            return chunk

        fitted = ""
        for token in self._tokenize_block(chunk):
            candidate = f"{fitted}{token}"
            if len(candidate) <= budget and self._has_balanced_anchor_tags(candidate):
                fitted = candidate
                continue

            if not fitted:
                if re.search(r"<a\b[^>]*>.*?</a>", token, flags=re.IGNORECASE | re.DOTALL):
                    continue
                fitted = token[:budget]
            break

        fallback = fitted.strip() if fitted.strip() else chunk[:budget].strip()
        safe_fallback = self._rollback_unbalanced_anchor_tail(fallback)

        if safe_fallback:
            return safe_fallback

        plain_text = self._to_plain_text(chunk)
        return self._truncate_text(plain_text, budget).strip()

    def _rollback_unbalanced_anchor_tail(self, text: str) -> str:
        candidate = text.strip()
        if not candidate:
            return ""
        if self._has_balanced_anchor_tags(candidate):
            return candidate

        open_tag_pattern = re.compile(r"<a\b[^>]*>", flags=re.IGNORECASE)
        close_tag_pattern = re.compile(r"</a>", flags=re.IGNORECASE)

        opens = list(open_tag_pattern.finditer(candidate))
        closes = list(close_tag_pattern.finditer(candidate))
        if not opens:
            return self._trim_dangling_html(candidate)

        if len(opens) > len(closes):
            unmatched_open = opens[len(closes)]
            candidate = candidate[: unmatched_open.start()].rstrip()

        return self._trim_dangling_html(candidate)

    @staticmethod
    def _trim_dangling_html(text: str) -> str:
        trimmed = text.strip()
        dangling_start = trimmed.rfind("<")
        dangling_end = trimmed.rfind(">")
        if dangling_start > dangling_end:
            trimmed = trimmed[:dangling_start].rstrip()
        return trimmed

    @staticmethod
    def _to_plain_text(text: str) -> str:
        without_tags = re.sub(r"<[^>]+>", " ", text)
        normalized = re.sub(r"\s+", " ", without_tags).strip()
        return normalized or "digest"

    @staticmethod
    def _truncate_text(text: str, limit: int) -> str:
        if len(text) <= limit:
            return text
        if limit <= 1:
            return text[:limit]
        return f"{text[: limit - 1].rstrip()}…"
