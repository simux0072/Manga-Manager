from __future__ import annotations

import re
import httpx
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse

from app.adapters.base import ChapterTemporarilyUnavailable, FrontierSentinel, SourceAdapter
from app.adapters.http import (
    HttpSourceClient,
    enumerate_async,
    iter_ordered_bytes,
    page_concurrency_for_source,
)
from app.adapters.parsing import (
    clean_chapter_title,
    extract_image_urls,
    nearby_cover_attr,
    parse_source_date,
)
from app.domain import ChapterItem, SeriesItem, normalize_chapter_number
from app.settings import settings


class AsuraAdapter(SourceAdapter):
    source = "asura"
    base_url = "https://asurascans.com"

    def __init__(self) -> None:
        self.client = HttpSourceClient(
            self.base_url,
            throttle_seconds=settings.asura_request_interval_seconds,
            source=self.source,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def list_recent(self) -> list[SeriesItem]:
        return await self.list_recent_frontier([])

    async def list_recent_frontier(self, sentinels: list[FrontierSentinel]) -> list[SeriesItem]:
        items: list[SeriesItem] = []
        sentinel_map = {sentinel.source_id: sentinel.latest_chapter for sentinel in sentinels}
        required_hits = min(settings.source_frontier_required_hits, len(sentinel_map))
        hits = 0
        max_pages = min(3, settings.asura_recent_pages)
        for page in range(1, max_pages + 1):
            path = "/" if page == 1 else f"/page/{page}/"
            try:
                soup = await self.client.get_soup(path)
            except httpx.HTTPStatusError as exc:
                # Asura currently returns 404 when a listing has no next page.  A
                # successful first page followed by that response is pagination
                # exhaustion, not provider degradation.
                if page > 1 and exc.response.status_code == 404:
                    break
                raise
            parsed = self.parse_recent_series(soup)
            if not parsed:
                break
            items.extend(parsed)
            hits += frontier_hits(parsed, sentinel_map)
            if required_hits and hits >= required_hits:
                break
        return dedupe_series(items)

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
            observed_id = urlparse(url).path.strip("/")
            source_id, revision = split_asura_source_id(observed_id)
            cover = nearby_cover_attr(link)
            recent_chapters = self.parse_card_chapters(link, source_id, url)
            items.append(
                SeriesItem(
                    source=self.source,
                    source_id=source_id,
                    title=title,
                    url=url,
                    cover_url=urljoin(self.base_url, cover) if cover else "",
                    metadata={
                        "asura_revision": revision,
                        "recent_chapters": [
                            {"number": chapter.number, "title": chapter.title, "url": chapter.url}
                            for chapter in recent_chapters
                        ]
                    }
                    if recent_chapters
                    else {"asura_revision": revision},
                )
            )
        return dedupe_series(items)

    def parse_card_chapters(self, link, source_id: str, series_url: str) -> list[ChapterItem]:
        container = link
        for parent in link.parents:
            if getattr(parent, "name", None) in {"article", "li", "div"}:
                container = parent
                break
        source = SeriesItem(self.source, source_id, link.get_text(" ", strip=True), series_url)
        return self.parse_chapters(container, source)

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
        for selector in (
            ".summary__content",
            ".description",
            ".entry-content",
            "[class*='summary']",
            "meta[name='description']",
            "meta[property='og:description']",
        ):
            tag = soup.select_one(selector)
            if tag:
                description = str(tag.get("content") or tag.get_text(" ", strip=True)).strip()
            if description:
                break
        cover = ""
        from app.adapters.parsing import image_attr

        cover_image = soup.select_one(
            ".summary_image img, .tab-summary img, .post-thumbnail img, "
            "img.wp-post-image, img[data-src*='covers'], img[src*='covers']"
        )
        if cover_image:
            cover = image_attr(cover_image)
        if not cover:
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
        genres = tuple(
            chip.get_text(" ", strip=True)
            for chip in soup.select("a[href*='genre'], a[href*='genres']")
            if chip.get_text(" ", strip=True)
        )
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

    async def iter_chapter_pages(self, chapter: ChapterItem, progress=None) -> AsyncIterator[bytes]:
        soup = await self.client.get_soup(chapter.url)
        urls = self.parse_chapter_image_urls(soup)
        if not urls and is_premium_or_locked(soup):
            retry_after = datetime.now(timezone.utc) + timedelta(
                hours=settings.asura_delay_hours or 1
            )
            raise ChapterTemporarilyUnavailable("Asura chapter is still premium", retry_after)
        total_bytes = 0
        async for index, page in enumerate_async(
            iter_ordered_bytes(
                self.client,
                urls,
                referer=chapter.url,
                concurrency=page_concurrency_for_source(self.source),
            )
        ):
            total_bytes += len(page)
            if progress:
                progress(index, len(urls), total_bytes)
            yield page

    def parse_chapter_image_urls(self, soup) -> list[str]:
        return [
            url for url in extract_image_urls(soup, self.base_url) if is_asura_chapter_image(url)
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


ASURA_REVISION_RE = re.compile(r"^(?P<stable>comics/.+)-(?P<revision>[0-9a-fA-F]{8})$")


def split_asura_source_id(value: str) -> tuple[str, str]:
    """Return revision-independent identity and the observed rotating suffix."""
    normalized = urlparse(value).path.strip("/") if "://" in value else value.strip("/")
    match = ASURA_REVISION_RE.fullmatch(normalized)
    if not match:
        return normalized, ""
    return match.group("stable"), match.group("revision").lower()


def asura_series_url(source_id: str, revision: str = "") -> str:
    stable, embedded = split_asura_source_id(source_id)
    selected = revision or embedded
    path = f"{stable}-{selected}" if selected else stable
    return urljoin(AsuraAdapter.base_url, f"/{path}")


def dedupe_chapters(items: list[ChapterItem]) -> list[ChapterItem]:
    seen: set[str] = set()
    result: list[ChapterItem] = []
    for item in items:
        if item.number not in seen:
            seen.add(item.number)
            result.append(item)
    return result


def clean_series_title(title: str) -> str:
    title = re.sub(
        r"\b(Just now|today|yesterday|last week|\d+\s+\w+\s+ago)\b.*", "", title, flags=re.I
    )
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
    return normalized in {
        "first chapter",
        "latest chapter",
        "first",
        "latest",
        "manhwa",
        "text mode",
    }


def frontier_hits(items: list[SeriesItem], sentinels: dict[str, str]) -> int:
    hits = 0
    for item in items:
        known = sentinels.get(item.source_id)
        latest = latest_recent_chapter(item)
        if (
            known is not None
            and latest is not None
            and chapter_number_key(latest) <= chapter_number_key(known)
        ):
            hits += 1
    return hits


def latest_recent_chapter(item: SeriesItem) -> str | None:
    rows = item.metadata.get("recent_chapters")
    if not isinstance(rows, list):
        return None
    numbers = [str(row.get("number") or "") for row in rows if isinstance(row, dict)]
    numbers = [number for number in numbers if number]
    return max(numbers, key=chapter_number_key) if numbers else None


def chapter_number_key(value: str) -> tuple[int, float, str]:
    try:
        return (1, float(normalize_chapter_number(value)), "")
    except ValueError:
        return (0, 0.0, value)


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
    match = re.search(
        rf"{re.escape(title)}\s+(.{{10,220}}?)\s+(?:In a world|Read |Bookmark|First Chapter)", text
    )
    if not match:
        return ()
    aliases = [
        value.strip(" ·•")
        for value in re.split(r"\s+[•·]\s+", match.group(1))
        if value.strip(" ·•") and len(value.strip(" ·•")) > 2
    ]
    polluted = {"asura scans home", "asura scans", "home"}
    return tuple(alias for alias in aliases[:5] if alias.lower() not in polluted)
