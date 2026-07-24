from __future__ import annotations

from collections import OrderedDict
from collections.abc import AsyncIterator
from datetime import datetime
from urllib.parse import urlencode

from app.adapters.asura import listing_diagnostics
from app.adapters.base import FrontierSentinel, SourceAdapter
from app.adapters.http import (
    HttpSourceClient,
    enumerate_async,
    iter_ordered_bytes,
    page_concurrency_for_source,
)
from app.domain import ChapterItem, SeriesItem, chapter_quality_rank, normalize_chapter_number
from app.settings import settings


class MangaDexAdapter(SourceAdapter):
    source = "mangadex"
    base_url = "https://api.mangadex.org"
    website_url = "https://mangadex.org"

    def __init__(self) -> None:
        self.listing_diagnostics: dict[str, int | bool] = {}
        self.client = HttpSourceClient(
            self.base_url,
            throttle_seconds=settings.mangadex_request_interval_seconds,
            source=self.source,
        )
        self.at_home_client = HttpSourceClient(
            self.base_url,
            throttle_seconds=settings.mangadex_at_home_interval_seconds,
            source=self.source,
        )

    async def aclose(self) -> None:
        await self.client.aclose()
        await self.at_home_client.aclose()

    async def list_recent(self) -> list[SeriesItem]:
        return await self.list_recent_frontier([])

    async def list_recent_frontier(self, sentinels: list[FrontierSentinel]) -> list[SeriesItem]:
        sentinel_map = {sentinel.source_id: sentinel.latest_chapter for sentinel in sentinels}
        required_hits = min(settings.source_frontier_required_hits, len(sentinel_map))
        hits: set[str] = set()
        grouped: OrderedDict[str, SeriesItem] = OrderedDict()
        pages_fetched = 0
        frontier_reached = False
        exhausted = False
        limit = settings.mangadex_recent_limit

        for page in range(settings.mangadex_recent_pages):
            payload = await self._get_json(
                "/chapter",
                {
                    "limit": limit,
                    "offset": page * limit,
                    "translatedLanguage[]": [settings.mangadex_language],
                    "contentRating[]": ["safe", "suggestive", "erotica"],
                    "includeExternalUrl": 0,
                    "includes[]": ["manga", "scanlation_group"],
                    "order[readableAt]": "desc",
                },
            )
            pages_fetched = page + 1
            rows = payload.get("data") or []
            if not rows:
                exhausted = True
                break
            for row in rows:
                item = series_from_chapter(row)
                if item is None:
                    continue
                grouped[item.source_id] = merge_recent_series(grouped.get(item.source_id), item)
                recent = item.metadata.get("recent_chapters") or []
                latest = str(recent[0].get("number") or "") if recent else ""
                known = sentinel_map.get(item.source_id)
                if known and latest and chapter_key(latest) <= chapter_key(known):
                    hits.add(item.source_id)
            if required_hits and len(hits) >= required_hits:
                frontier_reached = True
                break
            total = int(payload.get("total") or 0)
            if len(rows) < limit or (total and (page + 1) * limit >= total):
                exhausted = True
                break

        self.listing_diagnostics = listing_diagnostics(
            pages_fetched,
            settings.mangadex_recent_pages,
            frontier_reached,
            exhausted,
        )
        return list(grouped.values())

    async def get_series_detail(self, source_series: SeriesItem) -> SeriesItem:
        payload = await self._get_json(
            f"/manga/{source_series.source_id}",
            {"includes[]": ["cover_art", "author", "artist"]},
        )
        row = payload.get("data") or {}
        attributes = row.get("attributes") or {}
        title = localized(attributes.get("title"), "en") or source_series.title
        aliases = localized_values(attributes.get("altTitles") or [])
        description = localized(attributes.get("description"), "en")
        relationships = row.get("relationships") or []
        cover_name = relationship_attribute(relationships, "cover_art", "fileName")
        cover_url = (
            f"https://uploads.mangadex.org/covers/{source_series.source_id}/{cover_name}.512.jpg"
            if cover_name
            else source_series.cover_url
        )
        tags = tuple(
            localized(tag.get("attributes", {}).get("name"), "en")
            for tag in attributes.get("tags") or []
        )
        tags = tuple(tag for tag in tags if tag)
        links = attributes.get("links") or {}
        external_ids = {
            target: str(links[key])
            for key, target in (
                ("al", "anilist"),
                ("mal", "mal"),
                ("mu", "mangaupdates"),
                ("kt", "kitsu"),
            )
            if links.get(key)
        }
        metadata = {
            **source_series.metadata,
            "status": attributes.get("status"),
            "year": attributes.get("year"),
            "original_language": attributes.get("originalLanguage"),
            "content_rating": attributes.get("contentRating"),
            "last_chapter": attributes.get("lastChapter"),
            "authors": relationship_names(relationships, "author"),
            "artists": relationship_names(relationships, "artist"),
        }
        return SeriesItem(
            source=self.source,
            source_id=source_series.source_id,
            title=title,
            url=f"{self.website_url}/title/{source_series.source_id}",
            aliases=tuple(dict.fromkeys((*source_series.aliases, *aliases))),
            description=description or source_series.description,
            cover_url=cover_url,
            genres=tags or source_series.genres,
            popularity=source_series.popularity,
            external_ids={**source_series.external_ids, **external_ids},
            metadata={key: value for key, value in metadata.items() if value not in (None, "")},
        )

    async def get_chapters(self, source_series: SeriesItem) -> list[ChapterItem]:
        rows: list[dict] = []
        offset = 0
        while True:
            payload = await self._get_json(
                f"/manga/{source_series.source_id}/feed",
                {
                    "limit": 100,
                    "offset": offset,
                    "translatedLanguage[]": [settings.mangadex_language],
                    "contentRating[]": ["safe", "suggestive", "erotica"],
                    "includeExternalUrl": 0,
                    "includes[]": ["scanlation_group"],
                    "order[readableAt]": "desc",
                },
            )
            page = payload.get("data") or []
            rows.extend(row for row in page if isinstance(row, dict))
            offset += len(page)
            total = int(payload.get("total") or 0)
            if not page or len(page) < 100 or (total and offset >= total):
                break
        return select_chapter_releases(rows, source_series)

    async def download_chapter_pages(self, chapter: ChapterItem) -> list[bytes]:
        return [page async for page in self.iter_chapter_pages(chapter)]

    async def iter_chapter_pages(self, chapter: ChapterItem, progress=None) -> AsyncIterator[bytes]:
        chapter_id = str(chapter.metadata.get("release_id") or "").strip()
        if not chapter_id:
            chapter_id = chapter.url.rstrip("/").split("/")[-1]
        payload = await self.at_home_client.get_json(
            f"/at-home/server/{chapter_id}",
            traffic_class="at_home",
        )
        base_url = str(payload.get("baseUrl") or "").rstrip("/")
        chapter_data = payload.get("chapter") or {}
        chapter_hash = str(chapter_data.get("hash") or "")
        filenames = chapter_data.get("data") or []
        if not base_url or not chapter_hash or not filenames:
            raise RuntimeError(f"MangaDex chapter {chapter_id} has no original pages")
        urls = [f"{base_url}/data/{chapter_hash}/{filename}" for filename in filenames]
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

    async def _get_json(self, path: str, params: dict[str, object]):
        query = urlencode(params, doseq=True)
        return await self.client.get_json(f"{path}?{query}")


def series_from_chapter(row: dict) -> SeriesItem | None:
    attributes = row.get("attributes") or {}
    if attributes.get("translatedLanguage") != settings.mangadex_language:
        return None
    if attributes.get("externalUrl") or attributes.get("isUnavailable"):
        return None
    manga = relationship(row.get("relationships") or [], "manga")
    if manga is None:
        return None
    manga_id = str(manga.get("id") or "")
    manga_attributes = manga.get("attributes") or {}
    title = localized(manga_attributes.get("title"), "en")
    if not manga_id or not title:
        return None
    chapter = chapter_from_row(row, manga_id)
    if chapter is None:
        return None
    return SeriesItem(
        source="mangadex",
        source_id=manga_id,
        title=title,
        url=f"https://mangadex.org/title/{manga_id}",
        aliases=localized_values(manga_attributes.get("altTitles") or []),
        description=localized(manga_attributes.get("description"), "en"),
        genres=tuple(
            name
            for tag in manga_attributes.get("tags") or []
            if (name := localized(tag.get("attributes", {}).get("name"), "en"))
        ),
        metadata={
            "latest_chapter": chapter.number,
            "recent_chapters": [
                {
                    "number": chapter.number,
                    "title": chapter.title,
                    "url": chapter.url,
                    "published_at": (
                        chapter.published_at.isoformat() if chapter.published_at else ""
                    ),
                }
            ],
        },
    )


def merge_recent_series(current: SeriesItem | None, candidate: SeriesItem) -> SeriesItem:
    if current is None:
        return candidate
    rows = [
        *(current.metadata.get("recent_chapters") or []),
        *(candidate.metadata.get("recent_chapters") or []),
    ]
    unique: dict[str, dict] = {}
    for row in rows:
        number = str(row.get("number") or "")
        if number and number not in unique:
            unique[number] = row
    recent = sorted(unique.values(), key=lambda row: chapter_key(str(row["number"])), reverse=True)
    metadata = {**current.metadata, "recent_chapters": recent[:6]}
    if recent:
        metadata["latest_chapter"] = recent[0]["number"]
    return SeriesItem(
        source=current.source,
        source_id=current.source_id,
        title=current.title,
        url=current.url,
        aliases=tuple(dict.fromkeys((*current.aliases, *candidate.aliases))),
        description=current.description or candidate.description,
        cover_url=current.cover_url or candidate.cover_url,
        genres=current.genres or candidate.genres,
        popularity=max(current.popularity, candidate.popularity),
        external_ids={**candidate.external_ids, **current.external_ids},
        metadata=metadata,
    )


def select_chapter_releases(rows: list[dict], source_series: SeriesItem) -> list[ChapterItem]:
    grouped: OrderedDict[str, list[ChapterItem]] = OrderedDict()
    for row in rows:
        chapter = chapter_from_row(row, source_series.source_id)
        if chapter is not None:
            grouped.setdefault(chapter.number, []).append(chapter)
    selected: list[ChapterItem] = []
    for alternatives in grouped.values():
        alternatives.sort(
            key=lambda item: (
                chapter_quality_rank(item),
                item.published_at.timestamp() if item.published_at else 0,
                str(item.metadata.get("release_id") or ""),
            ),
            reverse=True,
        )
        winner = alternatives[0]
        metadata = {
            **winner.metadata,
            "alternate_releases": [
                {
                    "release_id": item.metadata.get("release_id"),
                    "group": item.metadata.get("group"),
                    "official": item.metadata.get("official", False),
                    "verified": item.metadata.get("verified", False),
                    "pages": item.metadata.get("pages", 0),
                }
                for item in alternatives[1:]
            ],
        }
        selected.append(
            ChapterItem(
                source=winner.source,
                source_series_id=winner.source_series_id,
                number=winner.number,
                title=winner.title,
                url=winner.url,
                published_at=winner.published_at,
                metadata=metadata,
            )
        )
    return selected


def chapter_from_row(row: dict, manga_id: str) -> ChapterItem | None:
    attributes = row.get("attributes") or {}
    if attributes.get("translatedLanguage") != settings.mangadex_language:
        return None
    if attributes.get("externalUrl") or attributes.get("isUnavailable"):
        return None
    release_id = str(row.get("id") or "")
    raw_number = attributes.get("chapter")
    number = normalize_chapter_number(str(raw_number or "oneshot"))
    if not release_id or not number:
        return None
    group = relationship(row.get("relationships") or [], "scanlation_group")
    group_attributes = group.get("attributes") or {} if group else {}
    official = bool(group_attributes.get("official"))
    verified = bool(group_attributes.get("verified"))
    pages = int(attributes.get("pages") or 0)
    # Preserve source trust as the dominant signal, then prefer the most
    # complete release within the same trust tier.  A newer zero-page or
    # truncated upload must not displace an older complete chapter.
    quality_rank = (
        int(official) * 10_000
        + int(verified) * 1_000
        + min(max(pages, 0), 999)
    )
    title_suffix = str(attributes.get("title") or "").strip()
    title = "Oneshot" if number == "oneshot" else f"Chapter {number}"
    if title_suffix:
        title = f"{title} - {title_suffix}"
    return ChapterItem(
        source="mangadex",
        source_series_id=manga_id,
        number=number,
        title=title,
        url=f"https://mangadex.org/chapter/{release_id}",
        published_at=parse_datetime(attributes.get("readableAt") or attributes.get("publishAt")),
        metadata={
            "release_id": release_id,
            "language": attributes.get("translatedLanguage"),
            "volume": attributes.get("volume"),
            "pages": pages,
            "group_id": group.get("id") if group else "",
            "group": localized(group_attributes.get("name"), "en")
            or str(group_attributes.get("name") or ""),
            "official": official,
            "verified": verified,
            "quality_rank": quality_rank,
        },
    )


def localized(value, preferred: str) -> str:
    if isinstance(value, str):
        return value.strip()
    if not isinstance(value, dict):
        return ""
    selected = value.get(preferred) or value.get("en")
    if selected:
        return str(selected).strip()
    return str(next(iter(value.values()), "")).strip()


def localized_values(values: list[dict]) -> tuple[str, ...]:
    result: list[str] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        for text in value.values():
            cleaned = str(text or "").strip()
            if cleaned and cleaned not in result:
                result.append(cleaned)
    return tuple(result[:24])


def relationship(values: list[dict], kind: str) -> dict | None:
    return next((value for value in values if value.get("type") == kind), None)


def relationship_attribute(values: list[dict], kind: str, key: str) -> str:
    row = relationship(values, kind)
    return str((row or {}).get("attributes", {}).get(key) or "")


def relationship_names(values: list[dict], kind: str) -> list[str]:
    result = []
    for row in values:
        if row.get("type") != kind:
            continue
        name = localized(row.get("attributes", {}).get("name"), "en")
        if name:
            result.append(name)
    return result


def parse_datetime(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def chapter_key(value: str) -> tuple[int, float, str]:
    try:
        return (1, float(normalize_chapter_number(value)), "")
    except ValueError:
        return (0, 0.0, value)
