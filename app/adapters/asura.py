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

    async def get_series_detail(self, source_series: SeriesItem) -> SeriesItem:
        soup = await self.client.get_soup(source_series.url)
        return self.parse_series_detail(soup, source_series)

    def parse_series_detail(self, soup, source_series: SeriesItem) -> SeriesItem:
        title = clean_series_title(
            (soup.select_one("h1") or soup.select_one("[class*='title']")).get_text(" ", strip=True)
            if soup.select_one("h1") or soup.select_one("[class*='title']")
            else source_series.title
        )
        description = ""
        for selector in ("meta[name='description']", "meta[property='og:description']"):
            tag = soup.select_one(selector)
            if tag and tag.get("content"):
                description = str(tag["content"]).strip()
                break
        cover = ""
        image = soup.select_one("meta[property='og:image']")
        if image and image.get("content"):
            cover = str(image["content"])
        page_text = soup.get_text(" ", strip=True)
        metadata = compact_metadata(
            {
                "rating": first_number_after(page_text, "rating"),
                "follows": first_count_after(page_text, "bookmark"),
                "chapters": first_count_after(page_text, "chapter"),
                "views": first_count_after(page_text, "views"),
                "status": first_word_after(page_text, "status"),
                "type": first_word_after(page_text, "type"),
            }
        )
        genres = tuple(chip.get_text(" ", strip=True) for chip in soup.select("a[href*='genre'], a[href*='genres']") if chip.get_text(" ", strip=True))
        aliases = parse_aliases(page_text, title)
        return SeriesItem(
            source=source_series.source,
            source_id=source_series.source_id,
            title=title or source_series.title,
            url=source_series.url,
            aliases=aliases or source_series.aliases,
            description=description or source_series.description,
            cover_url=urljoin(self.base_url, cover or source_series.cover_url),
            genres=genres or source_series.genres,
            popularity=source_series.popularity,
            external_ids=source_series.external_ids,
            metadata={**source_series.metadata, **metadata},
        )

    def parse_chapters(self, soup, source_series: SeriesItem) -> list[ChapterItem]:
        chapters: list[ChapterItem] = []
        series_path = urlparse(source_series.url).path.rstrip("/")
        for link in soup.select("a[href*='/chapter/']"):
            title = link.get_text(" ", strip=True)
            href = link.get("href", "")
            path = urlparse(urljoin(self.base_url, href)).path.rstrip("/")
            if not href or not path.startswith(f"{series_path}/chapter/"):
                continue
            if is_helper_chapter_link(title):
                continue
            number = normalize_chapter_number(title or href)
            if not re.search(r"\d", number):
                number = normalize_chapter_number(href)
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
            if is_asura_chapter_image(url)
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
    title = re.sub(r"^\s*\d+(?:\.\d+)?\s+(?=[A-Z])", "", title)
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


def is_helper_chapter_link(title: str) -> bool:
    normalized = " ".join((title or "").split()).lower()
    return normalized in {"first chapter", "latest chapter", "first", "latest"}


def is_asura_chapter_image(url: str) -> bool:
    return (
        "cdn.asurascans.com/asura-images/chapters/" in url
        or "cdn.asurascans.com/asura-images/chapters-stitched/" in url
    )


def compact_metadata(values: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in values.items() if value not in (None, "", [], {})}


def first_number_after(text: str, label: str) -> float | None:
    match = re.search(rf"{label}\s+(\d+(?:\.\d+)?)", text, flags=re.I)
    if not match:
        match = re.search(rf"(\d+(?:\.\d+)?)\s+{label}", text, flags=re.I)
    return float(match.group(1)) if match else None


def first_count_after(text: str, label: str) -> str | None:
    match = re.search(rf"(\d+(?:\.\d+)?[KMB]?)\s+{label}s?", text, flags=re.I)
    if not match:
        match = re.search(rf"{label}s?\s+(\d+(?:\.\d+)?[KMB]?)", text, flags=re.I)
    return match.group(1) if match else None


def first_word_after(text: str, label: str) -> str:
    match = re.search(rf"{label}\s+([A-Za-z][A-Za-z -]{{1,30}})", text, flags=re.I)
    return " ".join(match.group(1).split()[:3]) if match else ""


def parse_aliases(text: str, title: str) -> tuple[str, ...]:
    match = re.search(rf"{re.escape(title)}\s+(.{{10,220}}?)\s+(?:In a world|Read |Bookmark|First Chapter)", text)
    if not match:
        return ()
    aliases = [
        value.strip(" ·•")
        for value in re.split(r"\s+[•·]\s+", match.group(1))
        if value.strip(" ·•") and len(value.strip(" ·•")) > 2
    ]
    return tuple(aliases[:5])
