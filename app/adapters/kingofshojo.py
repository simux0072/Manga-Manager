from __future__ import annotations

from collections.abc import AsyncIterator
import re
from urllib.parse import unquote, urljoin, urlparse

from app.adapters.asura import dedupe_chapters, dedupe_series
from app.adapters.base import SourceAdapter
from app.adapters.http import HttpSourceClient
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
        for url in urls:
            yield await self.client.get_bytes(url, referer=chapter.url)

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
