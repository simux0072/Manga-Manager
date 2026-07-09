from __future__ import annotations

import re
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

from app.adapters.base import ChapterTemporarilyUnavailable, SourceAdapter
from app.adapters.http import HttpSourceClient
from app.adapters.parsing import clean_chapter_title, extract_image_urls, nearby_cover_attr, parse_source_date
from app.domain import ChapterItem, SeriesItem, normalize_chapter_number
from app.settings import settings


class AsuraAdapter(SourceAdapter):
    source = "asura"
    base_url = "https://asurascans.com"

    def __init__(self) -> None:
        self.client = HttpSourceClient(
            self.base_url,
            throttle_seconds=settings.asura_request_interval_seconds,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def list_recent(self) -> list[SeriesItem]:
        soup = await self.client.get_soup("/")
        return self.parse_recent_series(soup)

    def parse_recent_series(self, soup) -> list[SeriesItem]:
        items: list[SeriesItem] = []
        for link in soup.select("a[href*='/comics/']")[:120]:
            href = link.get("href", "")
            if "/chapter/" in href:
                continue
            title = link.get("title") or link.get_text(" ", strip=True)
            title = clean_series_title(title)
            if not title or not href:
                continue
            url = urljoin(self.base_url, href)
            source_id = urlparse(url).path.strip("/")
            cover = nearby_cover_attr(link)
            items.append(
                SeriesItem(
                    source=self.source,
                    source_id=source_id,
                    title=title,
                    url=url,
                    cover_url=urljoin(self.base_url, cover) if cover else "",
                )
            )
        return dedupe_series(items)

    async def get_chapters(self, source_series: SeriesItem) -> list[ChapterItem]:
        soup = await self.client.get_soup(source_series.url)
        return self.parse_chapters(soup, source_series)

    def parse_chapters(self, soup, source_series: SeriesItem) -> list[ChapterItem]:
        chapters: list[ChapterItem] = []
        series_path = urlparse(source_series.url).path.rstrip("/")
        for link in soup.select("a[href*='/chapter/']"):
            title = link.get_text(" ", strip=True)
            href = link.get("href", "")
            path = urlparse(urljoin(self.base_url, href)).path.rstrip("/")
            if not href or not path.startswith(f"{series_path}/chapter/"):
                continue
            number = normalize_chapter_number(title or href)
            if not href or not number:
                continue
            container_text = link.parent.get_text(" ", strip=True) if link.parent else title
            published_at = parse_source_date(container_text)
            chapters.append(
                ChapterItem(
                    source=self.source,
                    source_series_id=source_series.source_id,
                    number=number,
                    title=clean_chapter_title(number, title, published_at),
                    url=urljoin(self.base_url, href),
                    published_at=published_at,
                )
            )
        return dedupe_chapters(chapters)

    async def download_chapter_pages(self, chapter: ChapterItem) -> list[bytes]:
        return [page async for page in self.iter_chapter_pages(chapter)]

    async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
        soup = await self.client.get_soup(chapter.url)
        urls = self.parse_chapter_image_urls(soup)
        if not urls and is_premium_or_locked(soup):
            retry_after = datetime.now(timezone.utc) + timedelta(hours=settings.asura_delay_hours or 1)
            raise ChapterTemporarilyUnavailable("Asura chapter is still premium", retry_after)
        for url in urls:
            yield await self.client.get_bytes(url, referer=chapter.url)

    def parse_chapter_image_urls(self, soup) -> list[str]:
        return [
            url
            for url in extract_image_urls(soup, self.base_url)
            if "cdn.asurascans.com/asura-images/chapters/" in url
        ]

    @staticmethod
    def downloadable_after(published_at):
        return published_at + timedelta(hours=settings.asura_delay_hours) if published_at else None


def dedupe_series(items: list[SeriesItem]) -> list[SeriesItem]:
    seen: set[str] = set()
    result: list[SeriesItem] = []
    for item in items:
        if item.source_id not in seen:
            seen.add(item.source_id)
            result.append(item)
    return result


def dedupe_chapters(items: list[ChapterItem]) -> list[ChapterItem]:
    seen: set[str] = set()
    result: list[ChapterItem] = []
    for item in items:
        if item.number not in seen:
            seen.add(item.number)
            result.append(item)
    return result


def clean_series_title(title: str) -> str:
    title = re.sub(r"\b(Just now|today|yesterday|last week|\d+\s+\w+\s+ago)\b.*", "", title, flags=re.I)
    title = re.sub(r"\b(Chapter|Ch\.)\s+\d+(?:\.\d+)?\b.*", "", title, flags=re.I)
    return " ".join(title.split())


def is_premium_or_locked(soup) -> bool:
    text = " ".join(
        part
        for part in [
            soup.title.get_text(" ", strip=True) if soup.title else "",
            soup.get_text(" ", strip=True)[:5000],
        ]
        if part
    ).lower()
    return "premium" in text or "early access" in text or "locked" in text
