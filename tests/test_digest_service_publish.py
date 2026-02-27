import os
from datetime import datetime
from types import SimpleNamespace

import pytest
from aiogram.exceptions import TelegramForbiddenError, TelegramNetworkError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

from app.config import Settings
from app.db import Base
from app.models import DeliveryAttempt, PublishedNews, RawNews, RejectedNews
from app.repositories import NewsRepository
from app.services.digest_service import DigestService


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "BOT_TOKEN": "x",
            "CHANNEL_ID": "@c",
            "ADMIN_USER_IDS": "1",
        }
    )


@pytest.mark.asyncio
async def test_publish_period_does_not_duplicate_rejected_news_on_rerun() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        session.add_all(
            [
                RawNews(
                    source_id=1,
                    title="r1",
                    summary="s",
                    url="https://example.com/1",
                    external_id="r1",
                    published_at=datetime(2026, 1, 1, 10, 0, 0),
                ),
                RawNews(
                    source_id=1,
                    title="r2",
                    summary="s",
                    url="https://example.com/2",
                    external_id="r2",
                    published_at=datetime(2026, 1, 1, 11, 0, 0),
                ),
            ]
        )
        await session.commit()

        service = DigestService(_settings())
        service.filter.evaluate = lambda _title, _summary, **_kwargs: SimpleNamespace(accepted=False, reason="noise", decision_trace=[], is_high_confidence=False, score=0, topic="general")
        async def _fake_build_digest(_period, _accepted):  # noqa: ANN001
            return SimpleNamespace(
                title="digest",
                body="body",
                items_count=0,
                source_breakdown={},
                topic_breakdown={},
                quality_metrics={},
            )

        async def _fake_send_digest_messages(_bot, _title, _body, **_kwargs):  # noqa: ANN001
            return {"status": "success", "total_chunks": 1, "sent_chunks": 1, "failed_chunks": []}

        service.summarizer.build_digest = _fake_build_digest
        service._send_digest_messages = _fake_send_digest_messages

        start_dt = datetime(2026, 1, 1, 0, 0, 0)
        end_dt = datetime(2026, 1, 2, 0, 0, 0)

        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)
        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)

        rejected = list((await session.execute(select(RejectedNews).order_by(RejectedNews.raw_news_id.asc()))).scalars().all())

        assert [row.raw_news_id for row in rejected] == [1, 2]
        assert [row.reason for row in rejected] == ["noise", "noise"]

    await engine.dispose()


@pytest.mark.asyncio
async def test_publish_period_does_not_create_record_when_delivery_failed() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        session.add(
            RawNews(
                source_id=1,
                title="r1",
                summary="s",
                url="https://example.com/1",
                external_id="r1",
                published_at=datetime(2026, 1, 1, 10, 0, 0),
            )
        )
        await session.commit()

        service = DigestService(_settings())
        service.filter.evaluate = lambda _title, _summary, **_kwargs: SimpleNamespace(accepted=True, reason="", decision_trace=[], is_high_confidence=True, score=7, topic="general")

        async def _fake_build_digest(_period, _accepted):  # noqa: ANN001
            return SimpleNamespace(
                title="digest",
                body="body",
                items_count=1,
                source_breakdown={"1": 1},
                topic_breakdown={"general": 1},
                quality_metrics={},
            )

        send_attempts = 0

        async def _fake_send_digest_messages(_bot, _title, _body, **_kwargs):  # noqa: ANN001
            nonlocal send_attempts
            send_attempts += 1
            return {"status": "partial", "total_chunks": 1, "sent_chunks": 0, "failed_chunks": [1]}

        service.summarizer.build_digest = _fake_build_digest
        service._send_digest_messages = _fake_send_digest_messages

        start_dt = datetime(2026, 1, 1, 0, 0, 0)
        end_dt = datetime(2026, 1, 2, 0, 0, 0)

        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)
        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)

        published = list((await session.execute(select(PublishedNews).order_by(PublishedNews.id.asc()))).scalars().all())

        assert send_attempts == 2
        assert len(published) == 2
        assert all((row.quality_metrics or {}).get("delivery_status") == "partial" for row in published)

    await engine.dispose()


@pytest.mark.asyncio
async def test_publish_period_skips_duplicate_period_unless_manual_republish() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        session.add(
            RawNews(
                source_id=1,
                title="r1",
                summary="s",
                url="https://example.com/1",
                external_id="r1",
                published_at=datetime(2026, 1, 1, 10, 0, 0),
            )
        )
        await session.commit()

        service = DigestService(_settings())
        service.filter.evaluate = lambda _title, _summary, **_kwargs: SimpleNamespace(accepted=True, reason="", decision_trace=[], is_high_confidence=True, score=7, topic="general")

        sent_payloads: list[tuple[str, str]] = []

        async def _fake_build_digest(_period, _accepted):  # noqa: ANN001
            return SimpleNamespace(
                title="digest",
                body="body",
                items_count=1,
                source_breakdown={"1": 1},
                topic_breakdown={"general": 1},
                quality_metrics={},
            )

        async def _fake_send_digest_messages(_bot, _title, _body, **_kwargs):  # noqa: ANN001
            sent_payloads.append((_title, _body))
            return {"status": "success", "total_chunks": 1, "sent_chunks": 1, "failed_chunks": []}

        service.summarizer.build_digest = _fake_build_digest
        service._send_digest_messages = _fake_send_digest_messages

        start_dt = datetime(2026, 1, 1, 0, 0, 0)
        end_dt = datetime(2026, 1, 2, 0, 0, 0)

        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)
        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)

        published = list((await session.execute(select(PublishedNews).order_by(PublishedNews.id.asc()))).scalars().all())

        assert len(sent_payloads) == 1
        assert len(published) == 1
        assert published[0].period_type == "daily"
        assert published[0].period_start == start_dt
        assert published[0].period_end == end_dt

        await service.republish_period(
            bot=object(),
            session=session,
            period_type="daily",
            start_dt=start_dt,
            end_dt=end_dt,
        )

        republished = list((await session.execute(select(PublishedNews).order_by(PublishedNews.id.asc()))).scalars().all())

        assert len(sent_payloads) == 2
        assert len(republished) == 2

    await engine.dispose()


@pytest.mark.asyncio
async def test_publish_period_saves_filter_rule_aggregates() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        session.add_all(
            [
                RawNews(
                    source_id=1,
                    title="ok",
                    summary="s",
                    url="https://example.com/1",
                    external_id="ok",
                    published_at=datetime(2026, 1, 1, 10, 0, 0),
                ),
                RawNews(
                    source_id=1,
                    title="bad",
                    summary="s",
                    url="https://example.com/2",
                    external_id="bad",
                    published_at=datetime(2026, 1, 1, 11, 0, 0),
                ),
            ]
        )
        await session.commit()

        service = DigestService(_settings())

        def _fake_evaluate(title, _summary, **_kwargs):  # noqa: ANN001
            if title == "ok":
                return SimpleNamespace(
                    accepted=True,
                    reason="",
                    is_high_confidence=True,
                    score=8,
                    topic="general",
                    decision_trace=[
                        {"rule": "topic_match", "delta": 2},
                        {"rule": "strategic_verb", "delta": 2},
                        {"rule": "publishable", "delta": 0},
                    ],
                )
            return SimpleNamespace(
                accepted=False,
                reason="noise",
                is_high_confidence=False,
                score=0,
                topic="general",
                decision_trace=[
                    {"rule": "low_priority", "delta": -4},
                    {"rule": "below_floor", "delta": 0},
                ],
            )

        service.filter.evaluate = _fake_evaluate

        async def _fake_build_digest(_period, _accepted):  # noqa: ANN001
            return SimpleNamespace(
                title="digest",
                body="body",
                items_count=1,
                source_breakdown={"1": 1},
                topic_breakdown={"general": 1},
                quality_metrics={},
            )

        async def _fake_send_digest_messages(_bot, _title, _body, **_kwargs):  # noqa: ANN001
            return {"status": "success", "total_chunks": 1, "sent_chunks": 1, "failed_chunks": []}

        service.summarizer.build_digest = _fake_build_digest
        service._send_digest_messages = _fake_send_digest_messages

        start_dt = datetime(2026, 1, 1, 0, 0, 0)
        end_dt = datetime(2026, 1, 2, 0, 0, 0)

        await service._publish_period(bot=object(), session=session, period_type="daily", start_dt=start_dt, end_dt=end_dt)

        published = list((await session.execute(select(PublishedNews).order_by(PublishedNews.id.asc()))).scalars().all())

        assert len(published) == 1
        metrics = published[0].quality_metrics
        assert metrics["filter_rule_hits"] == {
            "topic_match": 1,
            "strategic_verb": 1,
            "publishable": 1,
            "low_priority": 1,
            "below_floor": 1,
        }
        assert metrics["filter_rule_score_impact"] == {
            "topic_match": 2,
            "strategic_verb": 2,
            "publishable": 0,
            "low_priority": -4,
            "below_floor": 0,
        }
        assert metrics["suspicious_rules_rejection_share"] == 1.0

    await engine.dispose()


@pytest.mark.asyncio
async def test_send_digest_messages_logs_retry_and_success_attempts() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        service = DigestService(_settings())
        service.TELEGRAM_MAX_CHARS = 6

        class _Bot:
            def __init__(self):
                self.calls = 0

            async def send_message(self, chat_id, text):  # noqa: ANN001
                self.calls += 1
                if self.calls == 1:
                    raise TelegramNetworkError(method="sendMessage", message="temporary")
                return None

        bot = _Bot()
        repo = NewsRepository(session, timezone="UTC")
        result = await service._send_digest_messages(bot, "t", "b", news_repo=repo, digest_id=101)

        attempts = list((await session.execute(select(DeliveryAttempt).order_by(DeliveryAttempt.id.asc()))).scalars().all())

        assert result["status"] == "sent"
        assert len(attempts) == 4
        assert [a.status for a in attempts] == ["attempt", "retry", "attempt", "success"]
        assert attempts[0].digest_id == 101

    await engine.dispose()


@pytest.mark.asyncio
async def test_send_digest_messages_partial_success_marks_failed_chunks() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        service = DigestService(_settings())

        class _Bot:
            async def send_message(self, chat_id, text):  # noqa: ANN001
                raise TelegramForbiddenError(method="sendMessage", message="forbidden")

        repo = NewsRepository(session, timezone="UTC")
        result = await service._send_digest_messages(_Bot(), "t", "abcdef ghijkl", news_repo=repo, digest_id=202)

        attempts = list((await session.execute(select(DeliveryAttempt).order_by(DeliveryAttempt.chunk_idx.asc(), DeliveryAttempt.id.asc()))).scalars().all())

        assert result["status"] == "failed"
        assert result["sent_chunks"] == 0
        assert result["failed_chunks"] == [1]
        assert len(attempts) == 2
        assert attempts[0].status == "attempt"
        assert attempts[1].status == "failed"
        assert attempts[1].chunk_idx == 1

    await engine.dispose()


@pytest.mark.asyncio
async def test_redeliver_digest_skips_already_delivered_chunks() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    Session = async_sessionmaker(engine, expire_on_commit=False)

    async with Session() as session:
        repo = NewsRepository(session, timezone="UTC")
        digest = await repo.publish_digest(
            period_type="daily",
            period_start=datetime(2026, 1, 1, 0, 0, 0),
            period_end=datetime(2026, 1, 2, 0, 0, 0),
            title="digest",
            body="part1\n\npart2",
            items_count=2,
            source_breakdown={"1": 2},
            topic_breakdown={"general": 2},
            quality_metrics={"delivery_status": "partial"},
        )
        await repo.add_delivery_attempt(digest_id=digest.id, chunk_idx=1, status="success")

        service = DigestService(_settings())
        sent_texts: list[str] = []
        service._split_body = lambda _body: ["part1", "part2"]

        class _Bot:
            async def send_message(self, chat_id, text):  # noqa: ANN001
                sent_texts.append(text)
                return None

        result = await service.redeliver_digest(bot=_Bot(), session=session, digest_id=digest.id)
        second_result = await service.redeliver_digest(bot=_Bot(), session=session, digest_id=digest.id)

        attempts = await repo.get_delivery_attempts_by_digest(digest.id)

        assert result["status"] == "sent"
        assert result["skipped_chunks"] == [1]
        assert result["sent_chunks"] == 2
        assert len(sent_texts) == 1
        assert "2/2" in sent_texts[0]
        assert second_result["status"] == "sent"
        assert second_result["skipped_chunks"] == [1, 2]
        assert second_result["sent_chunks"] == 2
        assert len([a for a in attempts if a.status == "success" and a.chunk_idx == 1]) == 1

    await engine.dispose()
