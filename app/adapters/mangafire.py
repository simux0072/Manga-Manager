from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
import logging
import re
from urllib.parse import urlencode, urljoin, urlparse

from bs4 import BeautifulSoup

from app.adapters.asura import dedupe_chapters, dedupe_series
from app.adapters.base import FrontierSentinel, SourceAdapter
from app.adapters.http import HttpSourceClient, enumerate_async, iter_ordered_bytes, page_concurrency_for_source
from app.adapters.parsing import image_attr, parse_source_date
from app.domain import ChapterItem, SeriesItem, normalize_chapter_number
from app.settings import settings


logger = logging.getLogger(__name__)

MANGAFIRE_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


class MangaFireAdapter(SourceAdapter):
    source = "mangafire"
    base_url = "https://mangafire.to"

    def __init__(self) -> None:
        self.client = HttpSourceClient(
            self.base_url,
            throttle_seconds=settings.mangafire_request_interval_seconds,
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def list_recent(self) -> list[SeriesItem]:
        return await self.list_recent_frontier([])

    async def list_recent_frontier(self, sentinels: list[FrontierSentinel]) -> list[SeriesItem]:
        try:
            items = await self.list_recent_frontier_api(sentinels)
        except Exception as exc:
            logger.warning("failed to fetch MangaFire latest updates from API: %s", exc)
            return await self.list_recent_frontier_html(sentinels)
        if items:
            return items
        return await self.list_recent_frontier_html(sentinels)

    async def list_recent_frontier_api(self, sentinels: list[FrontierSentinel]) -> list[SeriesItem]:
        items: list[SeriesItem] = []
        sentinel_map = {sentinel.source_id: sentinel.latest_chapter for sentinel in sentinels}
        required_hits = min(settings.source_frontier_required_hits, len(sentinel_map))
        hits = 0
        max_pages = settings.mangafire_recent_pages if sentinels else min(3, settings.mangafire_recent_pages)
        for page in range(1, max_pages + 1):
            parsed = self.parse_recent_series(
                await self.fetch_recent_titles_page(page=page, limit=settings.mangafire_recent_limit)
            )
            if not parsed:
                break
            items.extend(parsed)
            hits += frontier_hits(parsed, sentinel_map)
            if required_hits and hits >= required_hits:
                break
        return dedupe_series(items)

    async def list_recent_frontier_html(self, sentinels: list[FrontierSentinel]) -> list[SeriesItem]:
        items: list[SeriesItem] = []
        sentinel_map = {sentinel.source_id: sentinel.latest_chapter for sentinel in sentinels}
        required_hits = min(settings.source_frontier_required_hits, len(sentinel_map))
        hits = 0
        max_pages = settings.mangafire_recent_pages if sentinels else min(3, settings.mangafire_recent_pages)
        for page in range(1, max_pages + 1):
            path = "/" if page == 1 else f"/latest-updates?page={page}"
            parsed = self.parse_updated_page(await self.client.get_soup(path))
            if not parsed:
                break
            items.extend(parsed)
            hits += frontier_hits(parsed, sentinel_map)
            if required_hits and hits >= required_hits:
                break
        return dedupe_series(items)

    async def fetch_recent_titles_page(self, *, page: int, limit: int):
        response = await self.client.request(
            "GET",
            f"{self.base_url}/api/titles?{self.recent_titles_query(page=page, limit=limit)}",
            headers=mangafire_api_headers(),
        )
        return response.json()

    def recent_titles_query(self, *, page: int = 1, limit: int | None = None) -> str:
        params: dict[str, object] = {
            "order[chapter_updated_at]": "desc",
            "page": page,
            "limit": limit or settings.mangafire_recent_limit,
        }
        if settings.mangafire_discovery_mode == "hot":
            params["hot"] = 1
        return urlencode(params)

    def parse_recent_series(self, payload) -> list[SeriesItem]:
        items: list[SeriesItem] = []
        for entry in api_items(payload):
            parsed = self.parse_series_entry(entry)
            if parsed:
                items.append(parsed)
        return dedupe_series(items)

    def parse_series_detail(self, payload) -> SeriesItem | None:
        return self.parse_series_entry(api_title(payload))

    async def get_series_detail(self, source_series: SeriesItem) -> SeriesItem:
        hid = source_series.source_id or hid_from_url(source_series.url)
        try:
            detail = self.parse_series_detail(await self.client.get_json(f"/api/titles/{hid}"))
        except Exception:
            detail = None
        if detail:
            return merge_series_items(source_series, detail)
        return merge_series_items(
            source_series,
            self.parse_series_detail_html(await self.client.get_soup(source_series.url), source_series),
        )

    def parse_updated_page(self, soup: BeautifulSoup) -> list[SeriesItem]:
        items: list[SeriesItem] = []
        for link in soup.select("a[href*='/title/'], a[href*='/manga/']"):
            href = link.get("href", "")
            source_id = hid_from_url(href)
            if not source_id:
                continue
            title = clean_updated_title(link.get("title") or link.get_text(" ", strip=True))
            if not title:
                continue
            cover = ""
            image = link.select_one("img")
            if image:
                cover = image_attr(image)
            if not cover and link.parent:
                image = link.parent.select_one("img")
                cover = image_attr(image) if image else ""
            recent_chapters = self.parse_updated_chapters_for_link(link, source_id, href)
            items.append(
                SeriesItem(
                    source=self.source,
                    source_id=source_id,
                    title=title,
                    url=urljoin(self.base_url, href),
                    cover_url=urljoin(self.base_url, cover) if cover else "",
                    metadata={
                        "recent_chapters": [
                            {
                                "number": chapter.number,
                                "title": chapter.title,
                                "url": chapter.url,
                                "published_at": chapter.published_at.isoformat()
                                if chapter.published_at
                                else "",
                            }
                            for chapter in recent_chapters
                        ]
                    }
                    if recent_chapters
                    else {},
                )
            )
        return dedupe_series(items)

    def parse_updated_chapters_for_link(self, link, source_id: str, series_href: str) -> list[ChapterItem]:
        container = link
        for parent in link.parents:
            if getattr(parent, "name", None) in {"article", "li", "div"}:
                container = parent
                break
        series_url = urljoin(self.base_url, series_href)
        chapters: list[ChapterItem] = []
        for chapter_link in container.select("a[href*='/chapter/']"):
            href = chapter_link.get("href", "")
            chapter_id = urlparse(href).path.rstrip("/").split("/")[-1]
            label = chapter_link.get_text(" ", strip=True) or href
            if not is_english_chapter(chapter_language_text(chapter_link)):
                continue
            number = normalize_chapter_number(label)
            if not chapter_id or not number:
                continue
            container_text = chapter_link.parent.get_text(" ", strip=True) if chapter_link.parent else label
            chapters.append(
                ChapterItem(
                    source=self.source,
                    source_series_id=source_id,
                    number=number,
                    title=label,
                    url=f"{series_url.rstrip('/')}/chapter/{chapter_id}",
                    published_at=parse_source_date(container_text),
                )
            )
        return dedupe_chapters(chapters)

    def parse_series_detail_html(self, soup: BeautifulSoup, source_series: SeriesItem) -> SeriesItem:
        title_tag = soup.select_one("h1, [class*='title']")
        title = clean_updated_title(title_tag.get_text(" ", strip=True)) if title_tag else source_series.title
        description = ""
        for selector in (
            ".synopsis",
            ".description",
            "[class*='synopsis']",
            "[class*='description']",
            "meta[name='description']",
            "meta[property='og:description']",
        ):
            tag = soup.select_one(selector)
            if tag:
                description = str(tag.get("content") or tag.get_text(" ", strip=True)).strip()
            if description:
                break
        cover = ""
        image = soup.select_one("meta[property='og:image']")
        if image and image.get("content"):
            cover = str(image["content"])
        if not cover:
            image = soup.select_one("img")
            cover = image_attr(image) if image else ""
        aliases = clean_aliases(parse_html_aliases(soup.get_text(" ", strip=True), title), title)
        genres = tuple(
            tag.get_text(" ", strip=True)
            for tag in soup.select("a[href*='genre'], a[href*='genres']")
            if tag.get_text(" ", strip=True)
        )
        return SeriesItem(
            source=self.source,
            source_id=source_series.source_id,
            title=title or source_series.title,
            url=source_series.url,
            aliases=aliases,
            description=description,
            cover_url=urljoin(self.base_url, cover) if cover else "",
            genres=genres,
        )

    def parse_series_entry(self, entry: dict) -> SeriesItem | None:
        title = entry.get("title") or entry.get("name") or ""
        hid = entry.get("hid") or hid_from_url(entry.get("url", ""))
        if not title or not hid:
            return None
        poster = entry.get("poster") or {}
        cover = poster.get("large") or poster.get("medium") or poster.get("small") or ""
        genres = names_from_terms(entry.get("genres") or [])
        aliases = clean_aliases((str(alias) for alias in entry.get("altTitles") or []), title)
        external_ids = {}
        if entry.get("malId"):
            external_ids["mal"] = str(entry["malId"])
        if entry.get("anilistId"):
            external_ids["anilist"] = str(entry["anilistId"])
        popularity = entry.get("follows") or entry.get("ratingCount") or entry.get("rank") or 0
        latest = latest_chapter_number(entry)
        chapter_updated_at = first_present(
            entry,
            "chapterUpdatedAt",
            "chapter_updated_at",
            "latestChapterUpdatedAt",
            "updatedAt",
            "updated_at",
        )
        metadata = compact_metadata(
            {
                "follows": entry.get("follows"),
                "rating": entry.get("rating") or entry.get("score"),
                "rating_count": entry.get("ratingCount"),
                "rank": entry.get("rank"),
                "status": entry.get("status"),
                "type": entry.get("type"),
                "year": entry.get("year"),
                "languages": entry.get("languages"),
                "has_volumes": entry.get("hasVolumes"),
                "themes": list(names_from_terms(entry.get("themes") or [])),
                "demographics": list(names_from_terms(entry.get("demographics") or [])),
                "authors": list(names_from_terms(entry.get("authors") or [])),
                "artists": list(names_from_terms(entry.get("artists") or [])),
                "latest_chapter": latest,
                "chapter_updated_at": chapter_updated_at,
                "recent_chapters": recent_chapters_metadata(latest, chapter_updated_at),
            }
        )
        return SeriesItem(
            source=self.source,
            source_id=str(hid),
            title=title,
            url=urljoin(self.base_url, entry.get("url") or f"/title/{hid}"),
            aliases=aliases,
            description=html_text(entry.get("synopsisHtml") or entry.get("description") or ""),
            cover_url=cover,
            genres=genres,
            popularity=float(popularity or 0),
            external_ids=external_ids,
            metadata=metadata,
        )

    async def get_chapters(self, source_series: SeriesItem) -> list[ChapterItem]:
        hid = source_series.source_id or hid_from_url(source_series.url)
        chapters: list[ChapterItem] = []
        page = 1
        try:
            while True:
                payload = await self.client.get_json(
                    f"/api/titles/{hid}/chapters?limit=100&page={page}&sort=number&order=desc"
                )
                chapters.extend(self.parse_chapters(payload, source_series))
                meta = api_meta(payload)
                if not meta.get("hasNext"):
                    break
                page += 1
        except Exception:
            fallback = chapters_from_recent_metadata(source_series)
            if fallback:
                return fallback
            raise
        return dedupe_chapters(chapters)

    def parse_chapters(self, payload, source_series: SeriesItem) -> list[ChapterItem]:
        chapters: list[ChapterItem] = []
        for entry in api_chapter_items(payload):
            if not is_english_chapter(entry.get("language") or entry.get("lang")):
                continue
            chapter_id = (
                entry.get("id")
                or entry.get("hid")
                or entry.get("chapterId")
                or entry.get("chapter_id")
            )
            raw_number = first_present(
                entry,
                "number",
                "chapter",
                "chapterNumber",
                "name",
                "title",
            )
            if raw_number is None:
                raw_number = ""
            number = normalize_chapter_number(str(raw_number))
            if not chapter_id or number == "":
                continue
            title = entry.get("name") or entry.get("title") or f"Chapter {number}"
            created_at = unix_datetime(
                entry.get("createdAt")
                or entry.get("created_at")
                or entry.get("uploadedAt")
                or entry.get("publishedAt")
            )
            chapters.append(
                ChapterItem(
                    source=self.source,
                    source_series_id=source_series.source_id,
                    number=number,
                    title=title,
                    url=f"{source_series.url.rstrip('/')}/chapter/{chapter_id}",
                    published_at=created_at,
                )
            )
        return dedupe_chapters(chapters)

    async def download_chapter_pages(self, chapter: ChapterItem) -> list[bytes]:
        return [page async for page in self.iter_chapter_pages(chapter)]

    async def iter_chapter_pages(self, chapter: ChapterItem, progress=None) -> AsyncIterator[bytes]:
        chapter_id = urlparse(chapter.url).path.rstrip("/").split("/")[-1]
        payload = await self.client.get_json(f"/api/chapters/{chapter_id}")
        urls = self.parse_chapter_image_urls(payload)
        total_bytes = 0
        async for index, page in enumerate_async(iter_ordered_bytes(
            self.client,
            urls,
            referer=chapter.url,
            concurrency=page_concurrency_for_source(self.source),
        )):
            total_bytes += len(page)
            if progress:
                progress(index, len(urls), total_bytes)
            yield page

    def parse_chapter_image_urls(self, payload) -> list[str]:
        pages = (payload.get("data") or {}).get("pages") or []
        return [page.get("url", "") for page in pages if page.get("url")]


def api_items(payload) -> list[dict]:
    if isinstance(payload, list):
        return payload
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return data["items"]
    if isinstance(data, list):
        return data
    if isinstance(payload, dict) and isinstance(payload.get("items"), list):
        return payload["items"]
    return []


def api_chapter_items(payload) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("items", "chapters", "data"):
            if isinstance(data.get(key), list):
                return [item for item in data[key] if isinstance(item, dict)]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    for key in ("items", "chapters"):
        if isinstance(payload.get(key), list):
            return [item for item in payload[key] if isinstance(item, dict)]
    return []


def is_english_chapter(value) -> bool:
    if value in (None, ""):
        return True
    if isinstance(value, dict):
        value = value.get("code") or value.get("name") or value.get("language") or ""
    normalized = str(value).strip().lower().replace("_", "-")
    return normalized in {"en", "eng", "english", "en-us", "en-gb"}


def first_present(entry: dict, *keys: str):
    for key in keys:
        if key in entry and entry[key] not in (None, ""):
            return entry[key]
    return None


def mangafire_api_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Referer": "https://mangafire.to/",
        "User-Agent": MANGAFIRE_USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
    }


def latest_chapter_number(entry: dict) -> object:
    latest = first_present(
        entry,
        "latestChapter",
        "latest_chapter",
        "lastChapter",
        "last_chapter",
    )
    if isinstance(latest, list):
        latest = latest[0] if latest else None
    if isinstance(latest, dict):
        return first_present(
            latest,
            "number",
            "chapter",
            "chapterNumber",
            "chapter_number",
            "name",
            "title",
        )
    return latest


def recent_chapters_metadata(latest: object, updated_at: object) -> list[dict[str, object]]:
    if latest in (None, ""):
        return []
    number = normalize_chapter_number(str(latest))
    if not number:
        return []
    return [
        compact_metadata(
            {
                "number": number,
                "title": f"Chapter {number}",
                "updated_at_text": updated_at,
                "url": "",
            }
        )
    ]


def api_title(payload) -> dict:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def api_meta(payload) -> dict:
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("meta"), dict):
        return data["meta"]
    meta = payload.get("meta")
    return meta if isinstance(meta, dict) else {}


def hid_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    if path.startswith("manga/"):
        slug = path.split("/", 1)[1].strip("/")
        if "." in slug:
            return slug.rsplit(".", 1)[-1]
        return slug
    if not path.startswith("title/"):
        return ""
    slug = path.split("/", 1)[1]
    return slug.split("-", 1)[0]


def frontier_hits(items: list[SeriesItem], sentinels: dict[str, str]) -> int:
    hits = 0
    for item in items:
        latest = latest_recent_chapter(item)
        known = sentinels.get(item.source_id)
        if known is None or latest is None:
            continue
        if chapter_not_newer(latest, known):
            hits += 1
    return hits


def latest_recent_chapter(item: SeriesItem) -> str | None:
    rows = item.metadata.get("recent_chapters")
    if not isinstance(rows, list):
        return None
    numbers = [str(row.get("number") or "") for row in rows if isinstance(row, dict)]
    numbers = [number for number in numbers if number]
    if not numbers:
        return None
    return max(numbers, key=chapter_number_key)


def chapter_not_newer(left: str, right: str) -> bool:
    return chapter_number_key(left) <= chapter_number_key(right)


def chapter_number_key(value: str) -> tuple[int, float, str]:
    try:
        return (1, float(normalize_chapter_number(value)), "")
    except ValueError:
        return (0, 0.0, value)


def unix_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def names_from_terms(values) -> tuple[str, ...]:
    names: list[str] = []
    for value in values:
        if isinstance(value, dict):
            name = value.get("name") or value.get("title") or value.get("slug")
        else:
            name = value
        if name:
            names.append(str(name))
    return tuple(names)


def html_text(value: str) -> str:
    return BeautifulSoup(value, "html.parser").get_text(" ", strip=True) if value else ""


def compact_metadata(values: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in values.items() if value not in (None, "", [], {})}


def clean_updated_title(value: str) -> str:
    value = re.sub(r"\bChapter\s+\d+(?:\.\d+)?\b.*", "", value or "", flags=re.I)
    return " ".join(value.split())


def chapter_language_text(tag) -> str:
    text = tag.get_text(" ", strip=True)
    match = re.search(r"\b(English|EN|ENG|Spanish|ES|French|FR|Portuguese|PT)\b", text, flags=re.I)
    return match.group(1) if match else ""


def parse_html_aliases(text: str, title: str) -> tuple[str, ...]:
    for pattern in (
        r"(?:Alternative|Other)\s+Titles?\s*[:\-]\s*(.{3,180}?)(?:\s+(?:Status|Author|Genre|Summary|Synopsis)\b|$)",
        rf"{re.escape(title)}\s*/\s*(.{{3,160}}?)(?:\s+(?:Status|Author|Genre|Summary|Synopsis)\b|$)",
    ):
        match = re.search(pattern, text, flags=re.I)
        if not match:
            continue
        aliases = [alias.strip(" /,;·•") for alias in re.split(r"\s*/\s*|[;]\s*", match.group(1))]
        return tuple(alias for alias in aliases if alias)
    return ()


def clean_aliases(values, title: str) -> tuple[str, ...]:
    cleaned: list[str] = []
    seen = {normalize_alias_key(title)}
    for value in values:
        alias = " ".join(str(value or "").strip(" /,;·•").split())
        key = normalize_alias_key(alias)
        if not alias or not key or key in seen:
            continue
        cleaned.append(alias)
        seen.add(key)
    return tuple(cleaned[:8])


def normalize_alias_key(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().casefold())


def merge_series_items(base: SeriesItem, detail: SeriesItem) -> SeriesItem:
    return SeriesItem(
        source=base.source,
        source_id=base.source_id,
        title=detail.title or base.title,
        url=base.url or detail.url,
        aliases=detail.aliases or base.aliases,
        description=detail.description or base.description,
        cover_url=detail.cover_url or base.cover_url,
        genres=detail.genres or base.genres,
        popularity=max(base.popularity, detail.popularity),
        external_ids=detail.external_ids or base.external_ids,
        metadata={**base.metadata, **detail.metadata},
    )


def chapters_from_recent_metadata(source_series: SeriesItem) -> list[ChapterItem]:
    rows = source_series.metadata.get("recent_chapters")
    if not isinstance(rows, list):
        return []
    chapters: list[ChapterItem] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        published_at = None
        if row.get("published_at"):
            try:
                published_at = datetime.fromisoformat(str(row["published_at"]))
            except ValueError:
                published_at = None
        number = str(row.get("number") or "")
        url = str(row.get("url") or "")
        if not number or not url:
            continue
        chapters.append(
            ChapterItem(
                source=source_series.source,
                source_series_id=source_series.source_id,
                number=number,
                title=str(row.get("title") or f"Chapter {number}"),
                url=url,
                published_at=published_at,
            )
        )
    return dedupe_chapters(chapters)
