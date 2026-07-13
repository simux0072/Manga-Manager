from __future__ import annotations

import shutil
import zipfile
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from xml.etree import ElementTree

from sqlalchemy import delete, func, or_, select

from manga_manager.application.job_handlers import DeferredJobError, JobContext, RetryableJobError
from manga_manager.domain.jobs import JobKind, KavitaSyncPayload, LibraryRepairPayload
from manga_manager.infrastructure.db_models import (
    ArtifactBlob,
    ArtifactMetadataRewrite,
    CatalogChapter,
    CatalogSeries,
    ChapterArtifact,
    KavitaProjection,
    LibraryProjection,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.storage import ContentAddressedStorage
from manga_manager.infrastructure.storage_capacity import StorageCapacityCoordinator
from manga_manager.worker.runtime import SessionFactory


TRACKED = {"interested", "reading", "caught_up", "paused"}


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


class LibraryRepairPlanner:
    """Queue bounded repair work for tracked artifacts not yet projected to Kavita."""

    def __init__(self, queue: JobQueue | None = None) -> None:
        self.queue = queue or JobQueue()

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
            _, was_created = self.queue.enqueue(
                session,
                kind=JobKind.LIBRARY_REPAIR,
                dedupe_key=f"series:{series_id}:automatic-repair",
                payload=LibraryRepairPayload(
                    series_id=series_id,
                    reason="automatic_projection_repair",
                ),
                priority=80,
                series_key=str(series_id),
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

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, LibraryRepairPayload):
            raise RuntimeError("library repair handler received the wrong payload")
        rows = self._snapshot(payload.series_id)
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
            self._repair_one(row, payload.reason)
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
        if self._reconcile_kavita(payload.series_id):
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
                shutil.rmtree(root / "Manga" / storage_key, ignore_errors=True)

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
