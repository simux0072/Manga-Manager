from __future__ import annotations

from collections.abc import AsyncIterator
import re
from urllib.parse import unquote, urljoin, urlparse

from app.adapters.asura import clean_series_title, dedupe_chapters, dedupe_series
from app.adapters.base import FrontierSentinel, SourceAdapter
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


class KingOfShojoAdapter(SourceAdapter):
    source = "kingofshojo"
    base_url = "https://kingofshojo.com"

    def __init__(self) -> None:
        self.client = HttpSourceClient(
            self.base_url,
            timeout=settings.kingofshojo_timeout_seconds,
            throttle_seconds=settings.kingofshojo_request_interval_seconds,
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
        max_pages = min(3, settings.kingofshojo_recent_pages)
        for page in range(1, max_pages + 1):
            path = "/" if page == 1 else f"/page/{page}/"
            soup = await self.client.get_soup(path)
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
        for link in soup.select("a[href*='/manga/']")[:120]:
            title = link.get("title") or link.get_text(" ", strip=True)
            href = link.get("href", "")
            if not title or not href:
                continue
            url = urljoin(self.base_url, href)
            if is_non_series_link(url, title):
                continue
            cover = nearby_cover_attr(link)
            recent_chapters = self.parse_card_chapters(link, urlparse(url).path.strip("/"), url)
            items.append(
                SeriesItem(
                    source=self.source,
                    source_id=urlparse(url).path.strip("/"),
                    title=title,
                    url=url,
                    cover_url=urljoin(self.base_url, cover)
                    if valid_kingofshojo_cover(cover)
                    else "",
                    metadata={
                        "recent_chapters": [
                            {"number": chapter.number, "title": chapter.title, "url": chapter.url}
                            for chapter in recent_chapters
                        ]
                    }
                    if recent_chapters
                    else {},
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
        title_tag = soup.select_one("h1, .post-title, .entry-title")
        title = (
            clean_series_title(title_tag.get_text(" ", strip=True))
            if title_tag
            else source_series.title
        )
        description_tag = soup.select_one(".summary__content, .description, .entry-content")
        description = (
            description_tag.get_text(" ", strip=True)
            if description_tag
            else source_series.description
        )
        aliases, description = extract_description_aliases(description, title)
        cover = ""
        from app.adapters.parsing import image_attr

        for selector in (
            ".summary_image img",
            ".tab-summary img",
            ".post-content_item img",
            "img.wp-post-image",
            "img[data-src*='king-bucket/images']",
            "img[data-lazy-src*='king-bucket/images']",
        ):
            cover_image = soup.select_one(selector)
            candidate = image_attr(cover_image) if cover_image else ""
            if valid_kingofshojo_cover(candidate):
                cover = candidate
                break
        if not cover:
            image = soup.select_one("meta[property='og:image']")
            candidate = str(image.get("content") or "") if image else ""
            if valid_kingofshojo_cover(candidate):
                cover = candidate
        if not cover and soup.select_one("a[href*='/manga/']"):
            candidate = nearby_cover_attr(soup.select_one("a[href*='/manga/']"))
            if valid_kingofshojo_cover(candidate):
                cover = candidate
        text = soup.get_text(" ", strip=True)
        follows = ""
        follow_match = re.search(r"followed by\s+([\d,.]+[KMB]?)", text, flags=re.I)
        if follow_match:
            follows = follow_match.group(1)
        metadata = {key: value for key, value in {"follows": follows}.items() if value}
        genres = tuple(
            tag.get_text(" ", strip=True)
            for tag in soup.select("a[href*='genre'], a[href*='genres']")
            if tag.get_text(" ", strip=True)
        )
        return SeriesItem(
            source=source_series.source,
            source_id=source_series.source_id,
            title=title or source_series.title,
            url=source_series.url,
            aliases=aliases or source_series.aliases,
            description=description,
            cover_url=urljoin(self.base_url, cover or source_series.cover_url)
            if valid_kingofshojo_cover(cover or source_series.cover_url)
            else "",
            genres=genres or source_series.genres,
            popularity=source_series.popularity,
            external_ids=source_series.external_ids,
            metadata={**source_series.metadata, **metadata},
        )

    def parse_chapters(self, soup, source_series: SeriesItem) -> list[ChapterItem]:
        chapters: list[ChapterItem] = []
        for link in soup.select("a[href*='chapter']"):
            title = link.get_text(" ", strip=True)
            href = link.get("href", "")
            if is_template_or_empty_link(href, title):
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

    async def iter_chapter_pages(self, chapter: ChapterItem, progress=None) -> AsyncIterator[bytes]:
        soup = await self.client.get_soup(chapter.url)
        urls = self.parse_chapter_image_urls(soup)
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
            url
            for url in extract_image_urls(soup, self.base_url)
            if "cdn.kingofshojo.com/king-bucket/" in url
        ]


def is_template_or_empty_link(href: str, title: str) -> bool:
    combined = unquote(f"{href} {title}").lower()
    if not href:
        return True
    if href.startswith("#") or "{{" in combined or "number" in combined and "date" in combined:
        return True
    if "first chapter" in combined or "latest chapter" in combined:
        return True
    path = urlparse(urljoin(KingOfShojoAdapter.base_url, href)).path.strip("/")
    return not path or "chapter" not in path


def valid_kingofshojo_cover(url: str) -> bool:
    value = (url or "").lower()
    if not value:
        return False
    if any(token in value for token in ("wewtwt.png", "logo", "favicon", "banner", "default")):
        return False
    if "cdn.kingofshojo.com/king-bucket/images/" in value:
        return True
    return any(token in value for token in ("/wp-content/uploads/", "/covers/", "king-bucket"))


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


def is_non_series_link(url: str, title: str) -> bool:
    path = urlparse(url).path.strip("/").lower()
    title = " ".join(title.lower().split())
    if path in {"manga", "manga/list-mode"}:
        return True
    if path == "manga/" or not path.startswith("manga/"):
        return True
    if re.search(r"\b(text mode|list mode|manhwa|manga|manhua|genres?|filter)\b", title):
        return True
    return False


def extract_description_aliases(description: str, title: str) -> tuple[tuple[str, ...], str]:
    text = " ".join((description or "").split())
    match = re.match(
        r"Read\s+(?:manhwa|manga|manhua)\s+(.{3,260}?)(?:\s+(?:Plot|Summary|Synopsis|Description)\b|$)",
        text,
        flags=re.I,
    )
    if not match:
        return (), description
    prefix = match.group(1).strip()
    aliases = [
        value.strip(" /")
        for value in re.split(r"\s*/\s*", prefix)
        if value.strip(" /") and value.strip(" /").lower() != title.lower()
    ]
    cleaned = text[match.end() :].strip(" :-")
    return tuple(aliases[:8]), cleaned or description
