from datetime import date, datetime

from sqlalchemy import JSON, Boolean, CheckConstraint, Date, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    type: Mapped[str] = mapped_column(String(32), nullable=False)  # rss, site, api
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    meta: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class RawNews(Base):
    __tablename__ = "raw_news"
    __table_args__ = (
        UniqueConstraint("source_id", "external_id", name="uix_source_external"),
        Index("ix_raw_news_published_at", "published_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    title: Mapped[str] = mapped_column(String(1024), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    external_id: Mapped[str] = mapped_column(String(512), nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    tags: Mapped[list] = mapped_column(JSON, default=list)


class PublishedNews(Base):
    __tablename__ = "published_news"
    __table_args__ = (
        Index("ix_published_news_period", "period_type", "period_start", "period_end"),
        CheckConstraint(
            "status IN ('prepared', 'sending', 'sent', 'partial', 'failed')",
            name="ck_published_news_status",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    period_type: Mapped[str] = mapped_column(String(16), nullable=False)  # daily, weekly
    period_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    period_end: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    items_count: Mapped[int] = mapped_column(Integer, nullable=False)
    source_breakdown: Mapped[dict] = mapped_column(JSON, default=dict)
    topic_breakdown: Mapped[dict] = mapped_column(JSON, default=dict)
    quality_metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="prepared")
    published_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class RejectedNews(Base):
    __tablename__ = "rejected_news"
    __table_args__ = (
        UniqueConstraint("raw_news_id", name="uix_rejected_news_raw_news_id"),
        Index("ix_rejected_news_rejected_at", "rejected_at"),
        Index("ix_rejected_news_raw_news_id", "raw_news_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_news_id: Mapped[int] = mapped_column(ForeignKey("raw_news.id"), nullable=False)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), nullable=False)
    reason: Mapped[str] = mapped_column(String(255), nullable=False)
    rejected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class DailyStats(Base):
    __tablename__ = "stats_daily"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    stat_date: Mapped[date] = mapped_column(Date, unique=True, nullable=False)
    published_count: Mapped[int] = mapped_column(Integer, default=0)
    source_usage: Mapped[dict] = mapped_column(JSON, default=dict)
    rejected_count: Mapped[int] = mapped_column(Integer, default=0)
    rejection_breakdown: Mapped[dict] = mapped_column(JSON, default=dict)
    quality_metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    __table_args__ = (
        Index("ix_delivery_attempts_digest_chunk", "digest_id", "chunk_idx"),
        Index("ix_delivery_attempts_attempted_at", "attempted_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    digest_id: Mapped[int | None] = mapped_column(ForeignKey("published_news.id"), nullable=True)
    chunk_idx: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempted_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class WeeklyStats(Base):
    __tablename__ = "stats_weekly"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    week_start: Mapped[date] = mapped_column(Date, unique=True, nullable=False)
    published_count: Mapped[int] = mapped_column(Integer, default=0)
    source_usage: Mapped[dict] = mapped_column(JSON, default=dict)
    rejected_count: Mapped[int] = mapped_column(Integer, default=0)
    rejection_breakdown: Mapped[dict] = mapped_column(JSON, default=dict)
    quality_metrics: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
