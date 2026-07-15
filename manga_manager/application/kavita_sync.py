from __future__ import annotations

import asyncio
import base64
import hashlib
import io
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

import httpx
from PIL import Image
from sqlalchemy import exists, or_, select

from app.kavita import (
    KavitaChapter,
    KavitaReadProgress,
    KavitaSeries,
    configured_kavita_client,
)
from manga_manager.application.job_handlers import (
    DeferredJobError,
    JobContext,
    RetryableJobError,
    exception_message,
)
from manga_manager.domain.catalog import canonical_chapter_number, normalize_title
from manga_manager.domain.jobs import JobKind, KavitaSyncPayload
from manga_manager.domain.providers import SOURCE_PRIORITY
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogCoverAsset,
    CatalogExternalIdentifier,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    KavitaProjection,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.worker.runtime import SessionFactory


class KavitaClientProtocol(Protocol):
    @property
    def configured(self) -> bool: ...

    async def scan_folder_or_all(self, folder_path: Path) -> None: ...

    async def list_series(self) -> list[KavitaSeries]: ...

    async def series_detail(self, series_id: int) -> list[KavitaChapter]: ...

    async def chapter_progress(
        self, chapter_id: int, pages_total: int = 0
    ) -> KavitaReadProgress | None: ...

    async def add_want_to_read(self, series_ids: list[int]) -> None: ...

    async def remove_want_to_read(self, series_ids: list[int]) -> None: ...

    async def upload_series_cover(self, series_id: int, data_url: str) -> None: ...

    async def upload_chapter_cover(self, chapter_id: int, data_url: str) -> None: ...


ClientFactory = Callable[[], KavitaClientProtocol]
CoverFetcher = Callable[[str], Awaitable[bytes]]


class KavitaSyncPlanner:
    def __init__(self, queue: JobQueue | None = None) -> None:
        self.queue = queue or JobQueue()

    def enqueue_pending(
        self,
        session,
        *,
        limit: int = 100,
        reading_refresh_after: timedelta | None = None,
    ) -> tuple[int, int]:
        tracked = {"interested", "reading", "caught_up", "paused"}
        stale_reading = (
            CatalogSeries.kavita_synced_at
            < datetime.now(timezone.utc) - reading_refresh_after
            if reading_refresh_after is not None
            else False
        )
        rows = session.scalars(
            select(CatalogSeries)
            .where(CatalogSeries.status.in_(tracked))
            .where(
                exists(
                    select(KavitaProjection.chapter_id)
                    .join(CatalogChapter, CatalogChapter.id == KavitaProjection.chapter_id)
                    .where(CatalogChapter.series_id == CatalogSeries.id)
                )
            )
            .where(
                or_(
                    CatalogSeries.kavita_series_id.is_(None),
                    CatalogSeries.kavita_synced_at.is_(None),
                    CatalogSeries.kavita_synced_at < CatalogSeries.updated_at,
                    stale_reading,
                )
            )
            .order_by(CatalogSeries.kavita_synced_at.asc().nullsfirst(), CatalogSeries.id)
            .limit(limit)
        ).all()
        created = 0
        for series in rows:
            job, was_created = self.queue.enqueue(
                session,
                kind=JobKind.KAVITA_SYNC,
                dedupe_key=f"series:{series.id}",
                payload=KavitaSyncPayload(series_id=series.id),
                priority=70,
            )
            woke_configured_job = False
            if (
                not was_created
                and job.status == "retry_wait"
                and job.error_code == "kavita_unconfigured"
            ):
                job.available_at = datetime.now(timezone.utc)
                job.error_code = ""
                job.error_message = ""
                job.updated_at = datetime.now(timezone.utc)
                woke_configured_job = True
            created += int(was_created or woke_configured_job)
        return len(rows), created


@dataclass(frozen=True, slots=True)
class KavitaSnapshot:
    series_id: int
    title: str
    existing_kavita_id: int | None
    folder_path: Path
    tracked: bool
    aliases: tuple[str, ...]
    external_ids: dict[str, str]
    cover_urls: tuple[str, ...]
    cover_checksum: str
    kavita_cover_checksum: str
    chapter_cover_checksums: dict[str, str]
    expected_chapters: tuple[str, ...] = ()
    cached_cover_paths: tuple[str, ...] = ()


class KavitaSyncHandler:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        library_root: Path,
        client_factory: ClientFactory | None = None,
        cover_fetcher: CoverFetcher | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.library_root = library_root
        self.client_factory = client_factory or (
            lambda: configured_kavita_client(local_library_root=self.library_root)
        )
        self.cover_fetcher = cover_fetcher or self._fetch_cover

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, KavitaSyncPayload):
            raise RuntimeError("Kavita sync handler received the wrong payload")
        with self.session_factory() as session:
            snapshot = self._snapshot(session, payload.series_id, payload.folder_path)
        if snapshot is None:
            raise RetryableJobError("series_missing", "series does not exist")
        client = self.client_factory()
        if not client.configured:
            raise DeferredJobError(
                "kavita_unconfigured",
                "Kavita is not configured",
                retry_after=timedelta(hours=1),
            )

        cover_error = ""
        try:
            self._progress(context, 0, "requesting Kavita library scan")
            await client.scan_folder_or_all(snapshot.folder_path)
            mapper = getattr(client, "kavita_path_for_local", None)
            kavita_folder = mapper(snapshot.folder_path) if mapper else snapshot.folder_path
            match, chapters = await self._await_scan(
                client, snapshot, str(kavita_folder), context
            )
            if match is None:
                self._invalidate_kavita_mapping(
                    snapshot.series_id,
                    expected_kavita_id=snapshot.existing_kavita_id,
                )
                raise RetryableJobError("kavita_match_missing", "Kavita series match not found")
            self._progress(context, 2, f"matched Kavita series and {len(chapters)} chapters")
            if snapshot.tracked:
                await client.add_want_to_read([match.id])
            else:
                await client.remove_want_to_read([match.id])
            try:
                cover = await self._cover(snapshot)
            except ValueError as exc:
                cover = None
                cover_error = str(exc)
            if cover is not None:
                checksum, relative_path, content_type, content = cover
                same_kavita_series = snapshot.existing_kavita_id == match.id
                data_url = f"data:{content_type};base64," + base64.b64encode(content).decode(
                    "ascii"
                )
                if not same_kavita_series or snapshot.kavita_cover_checksum != checksum:
                    await self._retry_cover_write(
                        lambda: client.upload_series_cover(match.id, data_url)
                    )
                for remote_chapter in chapters:
                    number = canonical_chapter_number(remote_chapter.number)
                    if (
                        not same_kavita_series
                        or snapshot.chapter_cover_checksums.get(number, "") != checksum
                    ):
                        await self._retry_cover_write(
                            lambda chapter_id=remote_chapter.id: client.upload_chapter_cover(
                                chapter_id, data_url
                            )
                        )
            else:
                checksum = ""
                relative_path = ""
            self._progress(context, 3, "Kavita cover metadata synchronized")
            reading_progress = await self._reading_progress(client, chapters)
        except (RetryableJobError, DeferredJobError):
            raise
        except Exception as exc:
            raise RetryableJobError("kavita_unavailable", exception_message(exc)) from exc

        context.ensure_lease()
        by_number = {canonical_chapter_number(chapter.number): chapter for chapter in chapters}
        with self.session_factory() as session, session.begin():
            series = session.get(CatalogSeries, snapshot.series_id)
            if series is None:
                raise RetryableJobError("series_missing", "series disappeared during sync")
            synchronized_at = datetime.now(timezone.utc)
            series.kavita_series_id = match.id
            series.kavita_library_id = match.library_id
            series.kavita_synced_at = synchronized_at
            if cover is not None:
                series.cover_checksum = checksum
                series.cover_relative_path = relative_path
                series.kavita_cover_checksum = checksum
            catalog_chapter_ids: list[int] = []
            for chapter in session.scalars(
                select(CatalogChapter).where(CatalogChapter.series_id == series.id)
            ):
                catalog_chapter_ids.append(chapter.id)
                mapped = by_number.get(chapter.canonical_number)
                if mapped is None:
                    chapter.kavita_chapter_id = None
                    chapter.kavita_volume_id = None
                    chapter.kavita_mapped_at = None
                    continue
                chapter.kavita_chapter_id = mapped.id
                chapter.kavita_volume_id = mapped.volume_id
                chapter.kavita_mapped_at = synchronized_at
                if cover is not None:
                    chapter.kavita_cover_checksum = checksum
                progress = reading_progress.get(mapped.id)
                if progress is None:
                    continue
                reading = session.get(CatalogChapterReadingState, chapter.id)
                if reading is None:
                    reading = CatalogChapterReadingState(chapter_id=chapter.id)
                    session.add(reading)
                complete = bool(
                    progress.pages_total > 0
                    and progress.pages_read >= progress.pages_total
                )
                if complete:
                    reading.status = "read"
                    reading.read_at = reading.read_at or synchronized_at
                elif progress.pages_read > 0:
                    reading.status = "reading"
                    reading.read_at = None
                else:
                    reading.status = "unread"
                    reading.read_at = None
                reading.updated_at = synchronized_at
            # Runtime sessions disable autoflush. Persist newly imported progress before the
            # aggregate query that promotes the series reading state.
            session.flush()
            reading_rows = (
                session.scalars(
                    select(CatalogChapterReadingState).where(
                        CatalogChapterReadingState.chapter_id.in_(catalog_chapter_ids)
                    )
                ).all()
                if catalog_chapter_ids
                else []
            )
            if reading_progress and series.status not in {"paused", "untracked"}:
                read_count = sum(row.status == "read" for row in reading_rows)
                if read_count == len(catalog_chapter_ids) and catalog_chapter_ids:
                    next_status = "caught_up"
                elif any(row.status in {"read", "reading"} for row in reading_rows):
                    next_status = "reading"
                else:
                    next_status = "interested"
                if series.status != next_status:
                    series.status = next_status
                    series.updated_at = synchronized_at
        self._progress(
            context,
            4,
            "Kavita synchronization complete"
            if not cover_error
            else "Kavita synchronized; series cover remains pending",
        )

    def _progress(self, context: JobContext, current: int, message: str) -> None:
        context.ensure_lease()
        with self.session_factory() as session, session.begin():
            JobQueue().progress(
                session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message=message,
                details={
                    "phase": "kavita",
                    "processed": current,
                    "total": 4,
                    "unit": "phases",
                },
            )

    def _invalidate_kavita_mapping(
        self,
        series_id: int,
        *,
        expected_kavita_id: int | None,
    ) -> None:
        with self.session_factory() as session, session.begin():
            series = session.get(CatalogSeries, series_id)
            if series is None:
                return
            if expected_kavita_id is not None and series.kavita_series_id not in {
                None,
                expected_kavita_id,
            }:
                return
            series.kavita_series_id = None
            series.kavita_library_id = None
            series.kavita_synced_at = None
            series.kavita_cover_checksum = ""
            for chapter in session.scalars(
                select(CatalogChapter).where(CatalogChapter.series_id == series_id)
            ):
                chapter.kavita_chapter_id = None
                chapter.kavita_volume_id = None
                chapter.kavita_mapped_at = None
                chapter.kavita_cover_checksum = ""

    @staticmethod
    async def _retry_cover_write(operation: Callable[[], Awaitable[None]]) -> None:
        for attempt in range(3):
            try:
                await operation()
                return
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500 and exc.response.status_code != 429:
                    raise
                if attempt == 2:
                    raise
            except httpx.TransportError:
                if attempt == 2:
                    raise
            await asyncio.sleep(0.5 * (2**attempt))

    async def _cover(self, snapshot: KavitaSnapshot) -> tuple[str, str, str, bytes] | None:
        for relative_path in snapshot.cached_cover_paths:
            path = self.library_root.parent / relative_path
            try:
                return self._validated_cover_content(path.read_bytes())
            except (OSError, ValueError):
                continue
        if not snapshot.cover_urls:
            return None
        last_error: Exception | None = None
        for cover_url in snapshot.cover_urls:
            try:
                return await self._validated_cover(cover_url)
            except Exception as exc:
                last_error = exc
        raise ValueError("no source supplied a valid series cover") from last_error

    @staticmethod
    async def _reading_progress(
        client: KavitaClientProtocol,
        chapters: list[KavitaChapter],
    ) -> dict[int, KavitaReadProgress]:
        semaphore = asyncio.Semaphore(4)

        async def fetch(chapter: KavitaChapter) -> KavitaReadProgress | None:
            if chapter.pages_total <= 0:
                return None
            try:
                async with semaphore:
                    return await client.chapter_progress(chapter.id, chapter.pages_total)
            except (httpx.HTTPError, ValueError, TypeError):
                return None

        rows = await asyncio.gather(*(fetch(chapter) for chapter in chapters))
        return {row.chapter_id: row for row in rows if row is not None}

    async def _validated_cover(self, cover_url: str) -> tuple[str, str, str, bytes]:
        content = await self.cover_fetcher(cover_url)
        return self._validated_cover_content(content)

    def _validated_cover_content(self, content: bytes) -> tuple[str, str, str, bytes]:
        if not content or len(content) > 5 * 1024 * 1024:
            raise ValueError("series cover is empty or exceeds 5 MiB")
        try:
            with Image.open(io.BytesIO(content)) as image:
                image.verify()
                image_format = (image.format or "").lower()
        except Exception as exc:
            raise ValueError("series cover is not a valid image") from exc
        extension, content_type = {
            "jpeg": ("jpg", "image/jpeg"),
            "jpg": ("jpg", "image/jpeg"),
            "png": ("png", "image/png"),
            "webp": ("webp", "image/webp"),
            "gif": ("gif", "image/gif"),
        }.get(image_format, ("", ""))
        if not extension:
            raise ValueError(f"unsupported series cover format: {image_format}")
        checksum = hashlib.sha256(content).hexdigest()
        relative = Path("covers") / checksum[:2] / f"{checksum}.{extension}"
        destination = self.library_root.parent / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            temporary = destination.with_suffix(destination.suffix + ".tmp")
            temporary.write_bytes(content)
            temporary.replace(destination)
        return checksum, relative.as_posix(), content_type, content

    @staticmethod
    async def _fetch_cover(url: str) -> bytes:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content

    def _snapshot(
        self,
        session,
        series_id: int,
        requested_folder: str,
    ) -> KavitaSnapshot | None:
        series = session.get(CatalogSeries, series_id)
        if series is None:
            return None
        if requested_folder:
            folder = Path(requested_folder)
        else:
            relative = session.scalar(
                select(KavitaProjection.relative_path)
                .join(CatalogChapter, CatalogChapter.id == KavitaProjection.chapter_id)
                .where(CatalogChapter.series_id == series_id)
                .limit(1)
            )
            folder = self.library_root / Path(relative).parent if relative else self.library_root
        aliases = tuple(
            session.scalars(
                select(CatalogSeriesAlias.display_value).where(
                    CatalogSeriesAlias.series_id == series.id
                )
            ).all()
        )
        external_ids = dict(
            session.execute(
                select(CatalogExternalIdentifier.provider, CatalogExternalIdentifier.value).where(
                    CatalogExternalIdentifier.series_id == series.id
                )
            ).all()
        )
        source_covers = session.execute(
            select(CatalogSourceSeries.source, CatalogSourceSeries.cover_url).where(
                CatalogSourceSeries.series_id == series.id,
                CatalogSourceSeries.cover_url != "",
            )
        ).all()
        priorities = {
            source: len(SOURCE_PRIORITY) - index for index, source in enumerate(SOURCE_PRIORITY)
        }
        ordered_covers = [
            row[1]
            for row in sorted(
                source_covers, key=lambda row: priorities.get(row[0], 0), reverse=True
            )
            if row[1]
        ]
        if series.cover_url and series.cover_url not in ordered_covers:
            ordered_covers.append(series.cover_url)
        cached_covers = session.execute(
            select(
                CatalogSourceSeries.source,
                CatalogCoverAsset.relative_path,
            )
            .join(
                CatalogCoverAsset,
                CatalogCoverAsset.source_series_id == CatalogSourceSeries.id,
            )
            .where(CatalogSourceSeries.series_id == series.id)
        ).all()
        ordered_cached_covers = tuple(
            row[1]
            for row in sorted(
                cached_covers, key=lambda row: priorities.get(row[0], 0), reverse=True
            )
        )
        if series.cover_relative_path:
            ordered_cached_covers = (
                series.cover_relative_path,
                *(path for path in ordered_cached_covers if path != series.cover_relative_path),
            )
        chapter_cover_checksums = dict(
            session.execute(
                select(
                    CatalogChapter.canonical_number,
                    CatalogChapter.kavita_cover_checksum,
                )
                .join(KavitaProjection, KavitaProjection.chapter_id == CatalogChapter.id)
                .where(CatalogChapter.series_id == series.id)
            ).all()
        )
        return KavitaSnapshot(
            series.id,
            series.title,
            series.kavita_series_id,
            folder,
            series.status in {"interested", "reading", "caught_up", "paused"},
            aliases,
            external_ids,
            tuple(ordered_covers),
            series.cover_checksum,
            series.kavita_cover_checksum,
            chapter_cover_checksums,
            tuple(sorted(chapter_cover_checksums)),
            ordered_cached_covers,
        )

    async def _await_scan(
        self,
        client: KavitaClientProtocol,
        snapshot: KavitaSnapshot,
        kavita_folder: str,
        context: JobContext,
    ) -> tuple[KavitaSeries | None, list[KavitaChapter]]:
        delay = 1.0
        for _ in range(60):
            context.ensure_lease()
            candidates = await client.list_series()
            match = match_series(snapshot, candidates, kavita_folder)
            if match is not None:
                chapters = await client.series_detail(match.id)
                available = {canonical_chapter_number(chapter.number) for chapter in chapters}
                if not snapshot.tracked or set(snapshot.expected_chapters).issubset(available):
                    return match, chapters
            await asyncio.sleep(delay)
            delay = min(delay * 1.35, 15.0)
        return None, []


def match_series(
    snapshot: KavitaSnapshot,
    candidates: list[KavitaSeries],
    kavita_folder: str = "",
) -> KavitaSeries | None:
    anilist = snapshot.external_ids.get("anilist") or snapshot.external_ids.get("aniList")
    mal = snapshot.external_ids.get("mal") or snapshot.external_ids.get("myanimelist")
    normalized_values = {normalize_title(snapshot.title)} | {
        normalize_title(alias) for alias in snapshot.aliases
    }

    def same_folder(candidate: KavitaSeries) -> bool:
        return bool(
            kavita_folder
            and candidate.folder_path
            and Path(candidate.folder_path) == Path(kavita_folder)
        )

    def same_external_id(candidate: KavitaSeries) -> bool:
        return bool(
            (anilist and candidate.anilist_id == anilist)
            or (mal and candidate.mal_id == mal)
        )

    def same_title(candidate: KavitaSeries) -> bool:
        return normalize_title(candidate.name) in normalized_values

    def conflicts_with_snapshot(candidate: KavitaSeries) -> bool:
        folder_conflict = bool(
            kavita_folder
            and candidate.folder_path
            and Path(candidate.folder_path) != Path(kavita_folder)
        )
        external_id_conflict = bool(
            (anilist and candidate.anilist_id and candidate.anilist_id != anilist)
            or (mal and candidate.mal_id and candidate.mal_id != mal)
        )
        return folder_conflict or external_id_conflict

    # A persisted Kavita ID is only a hint. IDs can become stale after a Kavita
    # library is recreated, so never allow the numeric ID alone to override the
    # current folder, external-ID, or title evidence.
    if snapshot.existing_kavita_id:
        for candidate in candidates:
            if candidate.id == snapshot.existing_kavita_id and (
                not conflicts_with_snapshot(candidate)
                and (
                    same_external_id(candidate)
                    or same_folder(candidate)
                    or same_title(candidate)
                )
            ):
                return candidate
    identifier_matches = [
        candidate
        for candidate in candidates
        if same_external_id(candidate)
    ]
    if len(identifier_matches) == 1:
        return identifier_matches[0]
    if kavita_folder:
        folder_matches = [candidate for candidate in candidates if same_folder(candidate)]
        if len(folder_matches) == 1:
            return folder_matches[0]
    matches = [candidate for candidate in candidates if same_title(candidate)]
    return matches[0] if len(matches) == 1 else None
