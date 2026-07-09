from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.adapters.asura import dedupe_chapters, dedupe_series
from app.adapters.base import SourceAdapter
from app.adapters.http import HttpSourceClient
from app.domain import ChapterItem, SeriesItem, normalize_chapter_number
from app.settings import settings


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
        query = self.recent_titles_query()
        payload = await self.client.get_json(f"/api/titles?{query}")
        items: list[SeriesItem] = []
        for entry in api_items(payload):
            hid = entry.get("hid") or hid_from_url(entry.get("url", ""))
            if hid:
                try:
                    detail = api_title(await self.client.get_json(f"/api/titles/{hid}"))
                except Exception:
                    detail = {}
                entry = {**entry, **detail}
            parsed = self.parse_series_entry(entry)
            if parsed:
                items.append(parsed)
        return dedupe_series(items)

    def recent_titles_query(self) -> str:
        query = (
            "sort=chapter_updated_at:desc"
            f"&page=1&limit={settings.mangafire_recent_limit}"
        )
        if settings.mangafire_discovery_mode == "hot":
            query += "&hot=1"
        return query

    def parse_recent_series(self, payload) -> list[SeriesItem]:
        items: list[SeriesItem] = []
        for entry in api_items(payload):
            parsed = self.parse_series_entry(entry)
            if parsed:
                items.append(parsed)
        return dedupe_series(items)

    def parse_series_detail(self, payload) -> SeriesItem | None:
        return self.parse_series_entry(api_title(payload))

    def parse_series_entry(self, entry: dict) -> SeriesItem | None:
        title = entry.get("title") or entry.get("name") or ""
        hid = entry.get("hid") or hid_from_url(entry.get("url", ""))
        if not title or not hid:
            return None
        poster = entry.get("poster") or {}
        cover = poster.get("large") or poster.get("medium") or poster.get("small") or ""
        genres = names_from_terms(entry.get("genres") or [])
        aliases = tuple(str(alias) for alias in entry.get("altTitles") or [] if alias)
        external_ids = {}
        if entry.get("malId"):
            external_ids["mal"] = str(entry["malId"])
        if entry.get("anilistId"):
            external_ids["anilist"] = str(entry["anilistId"])
        popularity = entry.get("follows") or entry.get("ratingCount") or entry.get("rank") or 0
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
        )

    async def get_chapters(self, source_series: SeriesItem) -> list[ChapterItem]:
        hid = source_series.source_id or hid_from_url(source_series.url)
        chapters: list[ChapterItem] = []
        page = 1
        while True:
            payload = await self.client.get_json(
                f"/api/titles/{hid}/chapters?limit=100&page={page}&sort=number&order=desc"
            )
            chapters.extend(self.parse_chapters(payload, source_series))
            meta = api_meta(payload)
            if not meta.get("hasNext"):
                break
            page += 1
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
            raw_number = (
                entry.get("number")
                or entry.get("chapter")
                or entry.get("chapterNumber")
                or entry.get("name")
                or entry.get("title")
                or ""
            )
            number = normalize_chapter_number(str(raw_number))
            if not chapter_id or not number:
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

    async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
        chapter_id = urlparse(chapter.url).path.rstrip("/").split("/")[-1]
        payload = await self.client.get_json(f"/api/chapters/{chapter_id}")
        urls = self.parse_chapter_image_urls(payload)
        for url in urls:
            yield await self.client.get_bytes(url, referer=chapter.url)

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
    if not path.startswith("title/"):
        return ""
    slug = path.split("/", 1)[1]
    return slug.split("-", 1)[0]


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
