from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol

from sqlalchemy import select

from app.kavita import KavitaChapter, KavitaSeries, configured_kavita_client
from manga_manager.application.job_handlers import DeferredJobError, JobContext, RetryableJobError
from manga_manager.domain.catalog import canonical_chapter_number, normalize_title
from manga_manager.domain.jobs import KavitaSyncPayload
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogExternalIdentifier,
    CatalogSeries,
    CatalogSeriesAlias,
    LibraryProjection,
)
from manga_manager.worker.runtime import SessionFactory


class KavitaClientProtocol(Protocol):
    @property
    def configured(self) -> bool: ...

    async def scan_folder_or_all(self, folder_path: Path) -> None: ...

    async def list_series(self) -> list[KavitaSeries]: ...

    async def series_detail(self, series_id: int) -> list[KavitaChapter]: ...

    async def add_want_to_read(self, series_ids: list[int]) -> None: ...

    async def remove_want_to_read(self, series_ids: list[int]) -> None: ...


ClientFactory = Callable[[], KavitaClientProtocol]


@dataclass(frozen=True, slots=True)
class KavitaSnapshot:
    series_id: int
    title: str
    existing_kavita_id: int | None
    folder_path: Path
    tracked: bool
    aliases: tuple[str, ...]
    external_ids: dict[str, str]


class KavitaSyncHandler:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        library_root: Path,
        client_factory: ClientFactory | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.library_root = library_root
        self.client_factory = client_factory or (
            lambda: configured_kavita_client(local_library_root=self.library_root)
        )

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

        try:
            await client.scan_folder_or_all(snapshot.folder_path)
            candidates = await client.list_series()
            mapper = getattr(client, "kavita_path_for_local", None)
            kavita_folder = mapper(snapshot.folder_path) if mapper else snapshot.folder_path
            match = match_series(snapshot, candidates, str(kavita_folder))
            if match is None:
                raise RetryableJobError("kavita_match_missing", "Kavita series match not found")
            chapters = await client.series_detail(match.id)
            if snapshot.tracked:
                await client.add_want_to_read([match.id])
            else:
                await client.remove_want_to_read([match.id])
        except RetryableJobError:
            raise
        except Exception as exc:
            raise RetryableJobError("kavita_unavailable", str(exc)) from exc

        context.ensure_lease()
        by_number = {canonical_chapter_number(chapter.number): chapter for chapter in chapters}
        with self.session_factory() as session, session.begin():
            series = session.get(CatalogSeries, snapshot.series_id)
            if series is None:
                raise RetryableJobError("series_missing", "series disappeared during sync")
            series.kavita_series_id = match.id
            series.kavita_library_id = match.library_id
            series.kavita_synced_at = datetime.now(timezone.utc)
            for chapter in session.scalars(
                select(CatalogChapter).where(CatalogChapter.series_id == series.id)
            ):
                mapped = by_number.get(chapter.canonical_number)
                if mapped is None:
                    chapter.kavita_chapter_id = None
                    chapter.kavita_volume_id = None
                    chapter.kavita_mapped_at = None
                    continue
                chapter.kavita_chapter_id = mapped.id
                chapter.kavita_volume_id = mapped.volume_id
                chapter.kavita_mapped_at = datetime.now(timezone.utc)

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
                select(LibraryProjection.relative_path)
                .join(CatalogChapter, CatalogChapter.id == LibraryProjection.chapter_id)
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
        return KavitaSnapshot(
            series.id,
            series.title,
            series.kavita_series_id,
            folder,
            series.status in {"interested", "reading", "caught_up", "paused"},
            aliases,
            external_ids,
        )


def match_series(
    snapshot: KavitaSnapshot,
    candidates: list[KavitaSeries],
    kavita_folder: str = "",
) -> KavitaSeries | None:
    if snapshot.existing_kavita_id:
        for candidate in candidates:
            if candidate.id == snapshot.existing_kavita_id:
                return candidate
    anilist = snapshot.external_ids.get("anilist") or snapshot.external_ids.get("aniList")
    mal = snapshot.external_ids.get("mal") or snapshot.external_ids.get("myanimelist")
    identifier_matches = [
        candidate
        for candidate in candidates
        if (anilist and candidate.anilist_id == anilist) or (mal and candidate.mal_id == mal)
    ]
    if len(identifier_matches) == 1:
        return identifier_matches[0]
    if kavita_folder:
        folder_matches = [
            candidate
            for candidate in candidates
            if candidate.folder_path and Path(candidate.folder_path) == Path(kavita_folder)
        ]
        if len(folder_matches) == 1:
            return folder_matches[0]
    normalized_values = {normalize_title(snapshot.title)} | {
        normalize_title(alias) for alias in snapshot.aliases
    }
    matches = [
        candidate
        for candidate in candidates
        if normalize_title(candidate.name) in normalized_values
    ]
    return matches[0] if len(matches) == 1 else None
