from __future__ import annotations

from collections.abc import AsyncIterator
import re
from urllib.parse import unquote, urljoin, urlparse

from app.adapters.asura import clean_series_title, dedupe_chapters, dedupe_series
from app.adapters.base import SourceAdapter
from app.adapters.http import HttpSourceClient, iter_ordered_bytes, page_concurrency_for_source
from app.adapters.parsing import clean_chapter_title, extract_image_urls, nearby_cover_attr, parse_source_date
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
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def list_recent(self) -> list[SeriesItem]:
        items: list[SeriesItem] = []
        for page in range(1, settings.kingofshojo_recent_pages + 1):
            path = "/" if page == 1 else f"/page/{page}/"
            soup = await self.client.get_soup(path)
            items.extend(self.parse_recent_series(soup))
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
            items.append(
                SeriesItem(
                    source=self.source,
                    source_id=urlparse(url).path.strip("/"),
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
        title_tag = soup.select_one("h1, .post-title, .entry-title")
        title = clean_series_title(title_tag.get_text(" ", strip=True)) if title_tag else source_series.title
        description_tag = soup.select_one(".summary__content, .description, .entry-content")
        description = description_tag.get_text(" ", strip=True) if description_tag else source_series.description
        aliases, description = extract_description_aliases(description, title)
        image = soup.select_one("meta[property='og:image']")
        cover = str(image.get("content") or "") if image else ""
        if not cover:
            cover_image = soup.select_one(
                ".summary_image img, .tab-summary img, .post-content_item img, "
                "img.wp-post-image, img[data-src], img[data-lazy-src]"
            )
            if cover_image:
                from app.adapters.parsing import image_attr

                cover = image_attr(cover_image)
        if not cover and soup.select_one("a[href*='/manga/']"):
            cover = nearby_cover_attr(soup.select_one("a[href*='/manga/']"))
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
            cover_url=urljoin(self.base_url, cover or source_series.cover_url),
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

    async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
        soup = await self.client.get_soup(chapter.url)
        urls = self.parse_chapter_image_urls(soup)
        async for page in iter_ordered_bytes(
            self.client,
            urls,
            referer=chapter.url,
            concurrency=page_concurrency_for_source(self.source),
        ):
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
    path = urlparse(urljoin(KingOfShojoAdapter.base_url, href)).path.strip("/")
    return not path or "chapter" not in path


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
    match = re.match(r"Read\s+(?:manhwa|manga|manhua)\s+(.{3,260}?)(?:\s+(?:Plot|Summary|Synopsis|Description)\b|$)", text, flags=re.I)
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
