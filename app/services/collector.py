from __future__ import annotations

import logging
import re
import warnings
from hashlib import sha256
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from urllib.parse import urljoin, urlsplit, urlunsplit

import feedparser
import httpx
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

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
        for article in soup.select(selector)[:50]:
            title_tag = article.select_one(title_selector)
            title = (title_tag.get_text(strip=True) if title_tag else article.get_text(" ", strip=True))[:1024]
            if not title:
                continue
            summary = article.get_text(" ", strip=True)[:4000]
            article_link = self._extract_article_link(source.url, article)
            external_id = self._site_external_id(article_link=article_link, title=title, summary=summary)
            items.append(
                {
                    "source_id": source.id,
                    "title": title,
                    "summary": summary,
                    "url": article_link or source.url,
                    "external_id": external_id,
                    "published_at": now,
                    "tags": [],
                }
            )
        return items

    @staticmethod
    def _extract_article_link(base_url: str, article: BeautifulSoup) -> str | None:
        anchor = article.select_one("a[href]")
        href = anchor.get("href") if anchor else None
        if not href:
            return None
        return urljoin(base_url, href)

    @staticmethod
    def _site_external_id(article_link: str | None, title: str, summary: str) -> str:
        if article_link:
            normalized_url = NewsCollector._normalize_url(article_link)
            return f"site-url-{sha256(normalized_url.encode('utf-8')).hexdigest()}"

        content_fingerprint = NewsCollector._normalize_text(title) + "\n" + NewsCollector._normalize_text(summary)
        return f"site-content-{sha256(content_fingerprint.encode('utf-8')).hexdigest()}"

    @staticmethod
    def _normalize_url(url: str) -> str:
        parts = urlsplit(url.strip())
        path = re.sub(r"/+", "/", parts.path or "/")
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path.rstrip("/") or "/", parts.query, ""))

    @staticmethod
    def _normalize_text(value: str) -> str:
        return re.sub(r"\s+", " ", value).strip().lower()

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
    def _parse_dt(raw: str | int | float | None) -> datetime:
        if not raw:
            return datetime.utcnow()
        try:
            if isinstance(raw, (int, float)):
                dt = datetime.fromtimestamp(raw, tz=timezone.utc)
            else:
                try:
                    dt = parsedate_to_datetime(raw)
                except Exception:
                    normalized_raw = raw.strip()
                    if normalized_raw.endswith("Z"):
                        normalized_raw = normalized_raw[:-1] + "+00:00"
                    try:
                        dt = datetime.fromisoformat(normalized_raw)
                    except Exception:
                        if normalized_raw.isdigit() or (
                            normalized_raw.startswith("-") and normalized_raw[1:].isdigit()
                        ):
                            dt = datetime.fromtimestamp(int(normalized_raw), tz=timezone.utc)
                        else:
                            raise

            if dt.tzinfo:
                return dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return datetime.utcnow()

    @staticmethod
    def _strip_html(value: str) -> str:
        raw = value or ""
        if "<" not in raw and ">" not in raw:
            return re.sub(r"\s+", " ", unescape(raw)).strip()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", MarkupResemblesLocatorWarning)
            text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
        return re.sub(r"\s+", " ", text).strip()
