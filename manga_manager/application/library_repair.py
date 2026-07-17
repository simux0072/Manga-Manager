from __future__ import annotations

import shutil
import zipfile
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from sqlalchemy import delete, func, or_, select
from sqlalchemy.orm import Session

from manga_manager.application.job_handlers import DeferredJobError, JobContext, RetryableJobError
from manga_manager.domain.jobs import (
    ACTIVE_JOB_STATES,
    JobKind,
    JobState,
    KavitaSyncPayload,
    LibraryRepairPayload,
)
from manga_manager.infrastructure.db_models import (
    ArtifactBlob,
    ArtifactMetadataRewrite,
    CatalogChapter,
    CatalogSeries,
    ChapterArtifact,
    KavitaProjection,
    LibraryProjection,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.storage import ContentAddressedStorage
from manga_manager.infrastructure.storage_capacity import StorageCapacityCoordinator
from manga_manager.worker.runtime import SessionFactory


TRACKED = {"interested", "reading", "caught_up", "paused"}
ACTIVE_REPAIR_STATES = tuple(state.value for state in ACTIVE_JOB_STATES)
REPAIR_REASON_PRIORITY = {
    "merge": 60,
    "manual_repair": 50,
    "legacy_recovery": 40,
    "tracking": 30,
    "download": 20,
    "automatic_projection_repair": 10,
}


@dataclass(frozen=True, slots=True)
class RepairChapter:
    chapter_id: int
    artifact_id: int
    blob_checksum: str
    blob_path: str
    projection_path: str
    series_title: str
    series_description: str
    series_storage_key: str
    chapter_number: str
    chapter_title: str


def _merged_repair_payload(
    series_id: int, payloads: list[LibraryRepairPayload]
) -> LibraryRepairPayload:
    obsolete_storage_keys = tuple(
        sorted({key for payload in payloads for key in payload.obsolete_storage_keys if key})
    )
    reason = max(
        (payload.reason for payload in payloads),
        key=lambda value: REPAIR_REASON_PRIORITY.get(value, 0),
        default="metadata",
    )
    if obsolete_storage_keys:
        reason = "merge"
    return LibraryRepairPayload(
        series_id=series_id,
        reason=reason,
        obsolete_storage_keys=obsolete_storage_keys,
    )


def enqueue_library_repair(
    session: Session,
    *,
    series_id: int,
    reason: str,
    priority: int,
    obsolete_storage_keys: tuple[str, ...] = (),
    queue: JobQueue | None = None,
) -> tuple[WorkJob, bool]:
    """Coalesce all new repair requests into one active job per canonical series."""

    work_queue = queue or JobQueue()
    dedupe_key = f"series:{series_id}:repair"
    incoming = LibraryRepairPayload(
        series_id=series_id,
        reason=reason,
        obsolete_storage_keys=obsolete_storage_keys,
    )
    existing = session.scalar(
        select(WorkJob)
        .where(
            WorkJob.kind == JobKind.LIBRARY_REPAIR.value,
            WorkJob.dedupe_key == dedupe_key,
            WorkJob.status.in_(ACTIVE_REPAIR_STATES),
        )
        .with_for_update()
    )

    def merge_into(job: WorkJob) -> tuple[WorkJob, bool]:
        job.priority = min(job.priority, priority)
        payloads = [LibraryRepairPayload.model_validate(job.payload), incoming]
        if job.pending_payload:
            payloads.append(LibraryRepairPayload.model_validate(job.pending_payload))
        payload = _merged_repair_payload(series_id, payloads)
        current = (
            LibraryRepairPayload.model_validate(job.pending_payload)
            if job.status == JobState.LEASED.value and job.pending_payload
            else LibraryRepairPayload.model_validate(job.payload)
        )
        if payload == current:
            return job, False
        if job.status == JobState.LEASED.value:
            job.pending_payload = payload.model_dump(mode="json")
        else:
            job.payload = payload.model_dump(mode="json")
            job.error_code = ""
            job.error_message = ""
        return job, False

    if existing is not None:
        return merge_into(existing)

    job, created = work_queue.enqueue(
        session,
        kind=JobKind.LIBRARY_REPAIR,
        dedupe_key=dedupe_key,
        payload=incoming,
        priority=priority,
        series_key=str(series_id),
    )
    if created:
        return job, True
    # A concurrent transaction may have inserted the canonical job after the
    # SELECT above. Lock and merge instead of dropping cleanup information.
    existing = session.scalar(
        select(WorkJob).where(WorkJob.id == job.id).with_for_update()
    )
    if existing is None:
        raise RuntimeError("coalesced library repair job disappeared")
    return merge_into(existing)


class LibraryRepairPlanner:
    """Queue bounded repair work for tracked artifacts not yet projected to Kavita."""

    def __init__(self, queue: JobQueue | None = None) -> None:
        self.queue = queue or JobQueue()

    def reconcile_active_jobs(self, session: Session) -> tuple[int, int]:
        """Collapse legacy per-artifact repair jobs without losing merge cleanup work."""

        jobs = session.scalars(
            select(WorkJob)
            .where(
                WorkJob.kind == JobKind.LIBRARY_REPAIR.value,
                WorkJob.status.in_(ACTIVE_REPAIR_STATES),
                WorkJob.series_key != "",
            )
            .order_by(WorkJob.series_key, WorkJob.id)
            .with_for_update()
        ).all()
        grouped: dict[str, list[WorkJob]] = {}
        for job in jobs:
            grouped.setdefault(job.series_key, []).append(job)

        cancelled = 0
        canonicalized = 0
        for series_key, group in grouped.items():
            try:
                series_id = int(series_key)
            except ValueError:
                continue
            canonical_key = f"series:{series_id}:repair"
            cycle_id = group[0].cycle_id or 0
            repair_group_key = f"cycle:{cycle_id}:maintenance:library_repair"
            if len(group) == 1 and group[0].dedupe_key == canonical_key:
                group[0].group_key = repair_group_key
                continue
            keeper = next(
                (job for job in group if job.status == JobState.LEASED.value),
                group[0],
            )
            payloads: list[LibraryRepairPayload] = []
            for job in group:
                payloads.append(LibraryRepairPayload.model_validate(job.payload))
                if job.pending_payload:
                    payloads.append(LibraryRepairPayload.model_validate(job.pending_payload))
            merged = _merged_repair_payload(series_id, payloads)
            for job in group:
                if job.id == keeper.id:
                    continue
                if self.queue.cancel(
                    session,
                    job_id=job.id,
                    reason=f"coalesced into library repair job {keeper.id}",
                ):
                    cancelled += 1
            session.flush()
            if keeper.status == JobState.LEASED.value:
                current = LibraryRepairPayload.model_validate(keeper.payload)
                pending = (
                    LibraryRepairPayload.model_validate(keeper.pending_payload)
                    if keeper.pending_payload
                    else current
                )
                current_keys = set(current.obsolete_storage_keys) | set(
                    pending.obsolete_storage_keys
                )
                if set(merged.obsolete_storage_keys) - current_keys:
                    keeper.pending_payload = merged.model_dump(mode="json")
            else:
                keeper.payload = merged.model_dump(mode="json")
                keeper.pending_payload = {}
            if keeper.dedupe_key != canonical_key:
                keeper.dedupe_key = canonical_key
                canonicalized += 1
            keeper.group_key = repair_group_key
        session.flush()
        return canonicalized, cancelled

    def enqueue_pending(self, session, *, limit: int = 25) -> tuple[int, int]:
        active_artifact = (
            select(ChapterArtifact.id)
            .join(CatalogChapter, CatalogChapter.id == ChapterArtifact.chapter_id)
            .where(
                CatalogChapter.series_id == CatalogSeries.id,
                ChapterArtifact.state == "active",
            )
            .exists()
        )
        missing_projection = (
            select(ChapterArtifact.id)
            .join(CatalogChapter, CatalogChapter.id == ChapterArtifact.chapter_id)
            .outerjoin(
                KavitaProjection,
                (KavitaProjection.chapter_id == CatalogChapter.id)
                & (KavitaProjection.artifact_id == ChapterArtifact.id),
            )
            .where(
                CatalogChapter.series_id == CatalogSeries.id,
                ChapterArtifact.state == "active",
                KavitaProjection.chapter_id.is_(None),
            )
            .exists()
        )
        missing_library_projection = (
            select(ChapterArtifact.id)
            .join(CatalogChapter, CatalogChapter.id == ChapterArtifact.chapter_id)
            .outerjoin(
                LibraryProjection,
                (LibraryProjection.chapter_id == CatalogChapter.id)
                & (LibraryProjection.artifact_id == ChapterArtifact.id),
            )
            .where(
                CatalogChapter.series_id == CatalogSeries.id,
                ChapterArtifact.state == "active",
                LibraryProjection.chapter_id.is_(None),
            )
            .exists()
        )
        rows = session.scalars(
            select(CatalogSeries.id)
            .where(
                CatalogSeries.status.in_(TRACKED),
                active_artifact,
                or_(missing_projection, missing_library_projection),
            )
            .order_by(CatalogSeries.id)
            .limit(limit)
        ).all()
        created = 0
        for series_id in rows:
            _, was_created = enqueue_library_repair(
                session,
                series_id=series_id,
                reason="automatic_projection_repair",
                priority=80,
                queue=self.queue,
            )
            created += int(was_created)
        return len(rows), created


def canonical_comic_info(existing: bytes, row: RepairChapter) -> bytes:
    try:
        root = ElementTree.fromstring(existing)
    except ElementTree.ParseError:
        root = ElementTree.Element("ComicInfo")
    if root.tag != "ComicInfo":
        root = ElementTree.Element("ComicInfo")

    def set_text(name: str, value: str) -> None:
        node = root.find(name)
        if node is None:
            node = ElementTree.SubElement(root, name)
        node.text = value

    set_text("Series", row.series_title)
    set_text("SeriesSort", row.series_title)
    set_text("Number", row.chapter_number)
    set_text("Title", row.chapter_title or f"Chapter {row.chapter_number}")
    set_text("Summary", row.series_description)
    set_text(
        "Notes",
        f"Manga Manager series={row.series_storage_key}; chapter={row.chapter_id}; schema=v2",
    )
    set_text("LanguageISO", "en")
    set_text("Manga", "YesAndRightToLeft")
    return ElementTree.tostring(root, encoding="utf-8", xml_declaration=True)


class LibraryRepairHandler:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        storage: ContentAddressedStorage,
        queue: JobQueue | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.storage = storage
        self.queue = queue or JobQueue()
        self.capacity = StorageCapacityCoordinator(storage.root, storage.min_free_bytes)
        bind = getattr(session_factory, "kw", {}).get("bind")
        # SQLite's StaticPool test setup owns one connection and cannot safely
        # move its session work between threads. Production PostgreSQL workers can.
        self._offload_blocking = bind is None or bind.dialect.name != "sqlite"

    async def _blocking(self, function, *args, **kwargs) -> Any:
        if self._offload_blocking:
            return await self.storage.run_blocking(function, *args, **kwargs)
        return function(*args, **kwargs)

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, LibraryRepairPayload):
            raise RuntimeError("library repair handler received the wrong payload")
        rows = await self._blocking(self._snapshot, payload.series_id)
        if rows:
            with self.session_factory() as session, session.begin():
                reserved = self.capacity.reserve(
                    session,
                    job_id=context.lease.id,
                    owner=context.lease.owner,
                    requested_bytes=self.storage.max_chapter_bytes,
                    lease_expires_at=context.lease.expires_at,
                )
            if not reserved:
                raise DeferredJobError(
                    "storage_capacity",
                    "metadata repair paused until the storage reserve is restored",
                    retry_after=timedelta(minutes=5),
                )
        for index, row in enumerate(rows, 1):
            context.ensure_lease()
            await self._blocking(self._repair_one, row, payload.reason)
            if index == len(rows) or index % 5 == 0:
                with self.session_factory() as session, session.begin():
                    self.queue.progress(
                        session,
                        job_id=context.lease.id,
                        owner=context.lease.owner,
                        details={
                            "phase": "metadata",
                            "current": index,
                            "total": len(rows),
                            "unit": "archives",
                        },
                        message=f"Normalized {index} of {len(rows)} archives",
                    )
        if await self._blocking(self._reconcile_kavita, payload.series_id):
            with self.session_factory() as session, session.begin():
                self.queue.enqueue(
                    session,
                    kind=JobKind.KAVITA_SYNC,
                    dedupe_key=f"series:{payload.series_id}",
                    payload=KavitaSyncPayload(series_id=payload.series_id),
                    priority=80,
                    series_key=str(payload.series_id),
                )
        for storage_key in payload.obsolete_storage_keys:
            if not storage_key or "/" in storage_key or storage_key in {".", ".."}:
                continue
            for root in (self.storage.library_root, self.storage.kavita_root):
                await self._blocking(
                    shutil.rmtree,
                    root / "Manga" / storage_key,
                    ignore_errors=True,
                )

    def _snapshot(self, series_id: int) -> list[RepairChapter]:
        with self.session_factory() as session:
            series = session.get(CatalogSeries, series_id)
            if series is None:
                return []
            rows = session.execute(
                select(CatalogChapter, ChapterArtifact, ArtifactBlob, LibraryProjection)
                .join(ChapterArtifact, ChapterArtifact.chapter_id == CatalogChapter.id)
                .join(ArtifactBlob, ArtifactBlob.checksum == ChapterArtifact.blob_checksum)
                .outerjoin(LibraryProjection, LibraryProjection.chapter_id == CatalogChapter.id)
                .where(
                    CatalogChapter.series_id == series_id,
                    ChapterArtifact.state == "active",
                )
                .order_by(CatalogChapter.sort_number, CatalogChapter.id)
            ).all()
            return [
                RepairChapter(
                    chapter.id,
                    artifact.id,
                    blob.checksum,
                    blob.relative_path,
                    projection.relative_path if projection is not None else "",
                    series.title,
                    series.description,
                    series.storage_key,
                    chapter.display_number,
                    chapter.title,
                )
                for chapter, artifact, blob, projection in rows
            ]

    def _repair_one(self, row: RepairChapter, reason: str) -> None:
        source = self.storage.root / row.blob_path
        if not source.is_file():
            raise RetryableJobError("repair_blob_missing", f"artifact blob is missing: {source}")
        self.storage.ensure_directories()
        staging = self.storage.staging_root / f"metadata-{row.artifact_id}.cbz.tmp"
        try:
            with zipfile.ZipFile(source) as current:
                try:
                    old_xml = current.read("ComicInfo.xml")
                except KeyError:
                    old_xml = b""
                new_xml = canonical_comic_info(old_xml, row)
                target_projection = self.storage.projection_path(
                    row.series_storage_key, row.chapter_id, row.chapter_number
                ).as_posix()
                if (
                    old_xml == new_xml
                    and row.projection_path == target_projection
                    and (self.storage.library_root / target_projection).is_file()
                ):
                    return
                self.storage.require_free_space(source.stat().st_size)
                with zipfile.ZipFile(staging, "w", compression=zipfile.ZIP_STORED) as rewritten:
                    rewritten.writestr("ComicInfo.xml", new_xml)
                    for info in current.infolist():
                        if info.is_dir() or info.filename.lower() == "comicinfo.xml":
                            continue
                        with current.open(info) as source_entry, rewritten.open(
                            info, "w", force_zip64=True
                        ) as target_entry:
                            shutil.copyfileobj(source_entry, target_entry, length=1024 * 1024)
            new_blob = self.storage.store_existing(staging)
            self.storage.materialize(new_blob.relative_path, target_projection)
            with self.session_factory() as session, session.begin():
                artifact = session.get(ChapterArtifact, row.artifact_id)
                projection = session.get(LibraryProjection, row.chapter_id)
                if artifact is None:
                    raise RetryableJobError("repair_state_changed", "artifact changed during repair")
                if artifact.blob_checksum != row.blob_checksum:
                    return
                if session.get(ArtifactBlob, new_blob.checksum) is None:
                    session.add(
                        ArtifactBlob(
                            checksum=new_blob.checksum,
                            relative_path=new_blob.relative_path,
                            byte_count=new_blob.byte_count,
                        )
                    )
                    session.flush()
                artifact.blob_checksum = new_blob.checksum
                artifact.provenance = "metadata_repair"
                if projection is None:
                    projection = LibraryProjection(
                        chapter_id=row.chapter_id,
                        artifact_id=row.artifact_id,
                        relative_path=target_projection,
                    )
                    session.add(projection)
                projection.artifact_id = row.artifact_id
                projection.relative_path = target_projection
                if new_blob.checksum != row.blob_checksum:
                    session.add(
                        ArtifactMetadataRewrite(
                            chapter_id=row.chapter_id,
                            old_blob_checksum=row.blob_checksum,
                            new_blob_checksum=new_blob.checksum,
                            old_comic_info=old_xml,
                            new_comic_info=new_xml,
                            reason=reason,
                        )
                    )
            old_projection = self.storage.library_root / row.projection_path
            if row.projection_path and row.projection_path != target_projection:
                old_projection.unlink(missing_ok=True)
                self._remove_empty_parents(old_projection.parent, self.storage.library_root)
            self._remove_unreferenced_blob(row.blob_checksum, row.blob_path)
        finally:
            staging.unlink(missing_ok=True)

    def _remove_unreferenced_blob(self, checksum: str, relative_path: str) -> None:
        with self.session_factory() as session, session.begin():
            count = session.scalar(
                select(func.count()).select_from(ChapterArtifact).where(
                    ChapterArtifact.blob_checksum == checksum
                )
            )
            if count:
                return
            session.execute(delete(ArtifactBlob).where(ArtifactBlob.checksum == checksum))
        (self.storage.root / relative_path).unlink(missing_ok=True)

    def _reconcile_kavita(self, series_id: int) -> bool:
        with self.session_factory() as session, session.begin():
            series = session.get(CatalogSeries, series_id)
            if series is None:
                return False
            rows = session.execute(
                select(CatalogChapter.id, ChapterArtifact.id, ArtifactBlob.relative_path)
                .join(ChapterArtifact, ChapterArtifact.chapter_id == CatalogChapter.id)
                .join(ArtifactBlob, ArtifactBlob.checksum == ChapterArtifact.blob_checksum)
                .where(CatalogChapter.series_id == series_id, ChapterArtifact.state == "active")
            ).all()
            existing = {
                row.chapter_id: row
                for row in session.scalars(
                    select(KavitaProjection)
                    .join(CatalogChapter, CatalogChapter.id == KavitaProjection.chapter_id)
                    .where(CatalogChapter.series_id == series_id)
                )
            }
            if series.status not in TRACKED:
                for projection in existing.values():
                    (self.storage.kavita_root / projection.relative_path).unlink(missing_ok=True)
                    session.delete(projection)
                return series.kavita_series_id is not None
            for chapter_id, artifact_id, blob_path in rows:
                relative = Path("Manga") / series.storage_key / f"ch-{chapter_id}.cbz"
                self.storage.materialize_kavita(blob_path, relative.as_posix())
                projection = existing.pop(chapter_id, None) or KavitaProjection(
                    chapter_id=chapter_id,
                    artifact_id=artifact_id,
                    relative_path=relative.as_posix(),
                )
                projection.artifact_id = artifact_id
                projection.relative_path = relative.as_posix()
                session.add(projection)
            for projection in existing.values():
                (self.storage.kavita_root / projection.relative_path).unlink(missing_ok=True)
                session.delete(projection)
            return bool(rows)

    @staticmethod
    def _remove_empty_parents(path: Path, stop: Path) -> None:
        while path != stop:
            try:
                path.rmdir()
            except OSError:
                return
            path = path.parent
