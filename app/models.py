from datetime import date, datetime

from sqlalchemy import JSON, Boolean, Date, DateTime, Integer, String, Text, UniqueConstraint
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
    __table_args__ = (UniqueConstraint("source_id", "external_id", name="uix_source_external"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(1024), nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(String(1024), nullable=False)
    external_id: Mapped[str] = mapped_column(String(512), nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    tags: Mapped[list] = mapped_column(JSON, default=list)


class PublishedNews(Base):
    __tablename__ = "published_news"

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
    published_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class RejectedNews(Base):
    __tablename__ = "rejected_news"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    raw_news_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
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
