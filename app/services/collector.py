from __future__ import annotations

import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import feedparser
import httpx
from bs4 import BeautifulSoup

from app.config import Settings
from app.models import Source

logger = logging.getLogger(__name__)


class NewsCollector:
    def __init__(self, settings: Settings):
        self.settings = settings

    async def collect_from_source(self, source: Source) -> list[dict]:
        try:
            if source.type == "rss":
                return await self._fetch_rss(source)
            if source.type == "site":
                return await self._fetch_site(source)
            if source.type == "api":
                return await self._fetch_api(source)
            return []
        except Exception:
            logger.exception("Ошибка сбора источника id=%s name=%s", source.id, source.name)
            return []

    async def _fetch_rss(self, source: Source) -> list[dict]:
        parsed = feedparser.parse(source.url)
        out = []
        for entry in parsed.entries[:80]:
            ext_id = (entry.get("id") or entry.get("link") or entry.get("title") or "")
            if not ext_id:
                continue
            published_raw = entry.get("published") or entry.get("updated")
            published_at = self._parse_dt(published_raw)
            out.append(
                {
                    "source_id": source.id,
                    "title": entry.get("title", "Без заголовка")[:1024],
                    "summary": self._strip_html(entry.get("summary", ""))[:4000],
                    "url": entry.get("link", source.url),
                    "external_id": ext_id[:500],
                    "published_at": published_at,
                    "tags": [tag.get("term", "") for tag in entry.get("tags", []) if isinstance(tag, dict)],
                }
            )
        return out

    async def _fetch_site(self, source: Source) -> list[dict]:
        selector = source.meta.get("selector", "article")
        title_selector = source.meta.get("title_selector", "h1, h2, h3")
        async with httpx.AsyncClient(timeout=self.settings.fetch_timeout_seconds) as client:
            response = await client.get(source.url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

        now = datetime.now(timezone.utc).replace(tzinfo=None)
        items = []
        for idx, article in enumerate(soup.select(selector)[:50]):
            title_tag = article.select_one(title_selector)
            title = (title_tag.get_text(strip=True) if title_tag else article.get_text(" ", strip=True))[:1024]
            if not title:
                continue
            summary = article.get_text(" ", strip=True)[:4000]
            items.append(
                {
                    "source_id": source.id,
                    "title": title,
                    "summary": summary,
                    "url": source.url,
                    "external_id": f"{source.id}-{idx}-{title[:64]}",
                    "published_at": now,
                    "tags": [],
                }
            )
        return items

    async def _fetch_api(self, source: Source) -> list[dict]:
        async with httpx.AsyncClient(timeout=self.settings.fetch_timeout_seconds) as client:
            response = await client.get(source.url)
            response.raise_for_status()
            payload = response.json()

        items_data = payload.get("items", payload if isinstance(payload, list) else [])
        out = []
        for idx, item in enumerate(items_data[:80]):
            title = str(item.get("title") or item.get("name") or "Без заголовка")
            summary = str(item.get("summary") or item.get("description") or "")
            url = str(item.get("url") or source.url)
            ext_id = str(item.get("id") or item.get("guid") or f"{source.id}-{idx}-{title[:32]}")
            published_at = self._parse_dt(item.get("published_at") or item.get("date"))
            out.append(
                {
                    "source_id": source.id,
                    "title": title[:1024],
                    "summary": summary[:4000],
                    "url": url,
                    "external_id": ext_id[:500],
                    "published_at": published_at,
                    "tags": item.get("tags", []),
                }
            )
        return out

    @staticmethod
    def _parse_dt(raw: str | None) -> datetime:
        if not raw:
            return datetime.utcnow()
        try:
            dt = parsedate_to_datetime(raw)
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return datetime.utcnow()

    @staticmethod
    def _strip_html(value: str) -> str:
        return BeautifulSoup(value or "", "html.parser").get_text(" ", strip=True)
