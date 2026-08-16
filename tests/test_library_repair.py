from __future__ import annotations

import asyncio
import io
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree

import pytest
from PIL import Image
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from manga_manager.application.cbz_import import LegacyCbzImporter
from manga_manager.application.job_handlers import JobContext
from manga_manager.application.library_repair import (
    LibraryRepairHandler,
    LibraryRepairPlanner,
    enqueue_library_repair,
)
from manga_manager.domain.jobs import JobKind, LibraryRepairPayload
from manga_manager.infrastructure.db_models import (
    ArtifactBlob,
    ArtifactMetadataRewrite,
    CatalogChapter,
    CatalogSeries,
    ChapterArtifact,
    JobBase,
    KavitaProjection,
    LibraryProjection,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.storage import ContentAddressedStorage


def make_storage(tmp_path: Path) -> ContentAddressedStorage:
    return ContentAddressedStorage(
        tmp_path / "storage",
        max_page_bytes=1024 * 1024,
        max_chapter_bytes=10 * 1024 * 1024,
        max_pages=100,
        min_free_bytes=0,
    )


def make_archive(path: Path, *, include_metadata: bool = True) -> None:
    image = io.BytesIO()
    Image.new("RGB", (16, 24), "navy").save(image, format="PNG")
    with zipfile.ZipFile(path, "w") as archive:
        if include_metadata:
            archive.writestr(
                "ComicInfo.xml",
                "<ComicInfo><Series>9.5 Wrong title</Series><Number>1</Number>"
                "<Writer>Original Author</Writer></ComicInfo>",
            )
        archive.writestr("0001.png", image.getvalue())


@pytest.mark.asyncio
async def test_repair_rewrites_canonical_metadata_and_builds_tracked_kavita_projection(
    tmp_path: Path,
) -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    storage = make_storage(tmp_path)
    source = tmp_path / "chapter.cbz"
    make_archive(source)
    LegacyCbzImporter(session_factory=sessions, storage=storage).import_file(
        source, dry_run=False
    )

    with sessions() as session, session.begin():
        series = session.scalar(select(CatalogSeries))
        assert series is not None
        series.title = "Canonical Series"
        series.description = "Canonical summary"
        series.status = "interested"
        missing_projection = session.scalar(select(LibraryProjection))
        assert missing_projection is not None
        (storage.library_root / missing_projection.relative_path).unlink()
        session.delete(missing_projection)
        job, _ = JobQueue().enqueue(
            session,
            kind=JobKind.LIBRARY_REPAIR,
            dedupe_key=f"series:{series.id}:test",
            payload=LibraryRepairPayload(series_id=series.id, reason="test"),
            series_key=str(series.id),
        )
        lease = JobQueue().claim(
            session,
            owner="repair-test",
            lease_for=timedelta(minutes=5),
            now=datetime.now(timezone.utc),
            kinds={JobKind.LIBRARY_REPAIR},
        )
        assert lease is not None and lease.id == job.id
        series_id = series.id

    handler = LibraryRepairHandler(session_factory=sessions, storage=storage)
    await handler(JobContext(lease=lease, lease_lost=asyncio.Event()))

    with sessions() as session:
        chapter = session.scalar(select(CatalogChapter))
        artifact = session.scalar(
            select(ChapterArtifact).where(ChapterArtifact.state == "active")
        )
        assert chapter is not None and artifact is not None
        blob = session.get(ArtifactBlob, artifact.blob_checksum)
        projection = session.get(KavitaProjection, chapter.id)
        library_projection = session.get(LibraryProjection, chapter.id)
        assert blob is not None and projection is not None and library_projection is not None
        assert projection.artifact_id == artifact.id
        assert (storage.library_root / library_projection.relative_path).is_file()
        assert session.scalar(select(func.count()).select_from(ArtifactMetadataRewrite)) == 1
        with zipfile.ZipFile(storage.root / blob.relative_path) as archive:
            metadata = ElementTree.fromstring(archive.read("ComicInfo.xml"))
        assert metadata.findtext("Series") == "Canonical Series"
        assert metadata.findtext("SeriesSort") == "Canonical Series"
        assert metadata.findtext("Number") == "1"
        assert metadata.findtext("Summary") == "Canonical summary"
        assert metadata.findtext("Writer") == "Original Author"
        assert (storage.kavita_root / projection.relative_path).is_file()

    # A second pass is idempotent and does not rewrite another  archive.
    handler._reconcile_kavita(series_id)
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(ArtifactMetadataRewrite)) == 1


def test_untracked_series_is_removed_from_kavita_projection(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    storage = make_storage(tmp_path)
    source = tmp_path / "chapter.cbz"
    make_archive(source)
    LegacyCbzImporter(session_factory=sessions, storage=storage).import_file(
        source, dry_run=False
    )
    with sessions() as session, session.begin():
        series = session.scalar(select(CatalogSeries))
        assert series is not None
        series.status = "interested"
        series_id = series.id
    handler = LibraryRepairHandler(session_factory=sessions, storage=storage)
    handler._reconcile_kavita(series_id)
    with sessions() as session, session.begin():
        series = session.get(CatalogSeries, series_id)
        assert series is not None
        series.status = "untracked"
    handler._reconcile_kavita(series_id)
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(KavitaProjection)) == 0
    assert not any(storage.kavita_root.rglob("*.cbz"))


def test_planner_queues_missing_tracked_projection_once(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    storage = make_storage(tmp_path)
    source = tmp_path / "chapter.cbz"
    make_archive(source)
    LegacyCbzImporter(session_factory=sessions, storage=storage).import_file(
        source, dry_run=False
    )
    with sessions() as session, session.begin():
        series = session.scalar(select(CatalogSeries))
        assert series is not None
        series.status = "interested"
        assert LibraryRepairPlanner().enqueue_pending(session) == (1, 1)
    with sessions() as session, session.begin():
        assert LibraryRepairPlanner().enqueue_pending(session) == (1, 0)
        assert session.scalar(select(func.count()).select_from(WorkJob)) == 1


def test_repair_enqueue_coalesces_by_series_and_preserves_merge_cleanup() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        first, created = enqueue_library_repair(
            session, series_id=12, reason="download", priority=100
        )
        assert created is True
        second, created = enqueue_library_repair(
            session, series_id=12, reason="download", priority=100
        )
        assert created is False
        assert second.id == first.id
        merged, created = enqueue_library_repair(
            session,
            series_id=12,
            reason="merge",
            priority=90,
            obsolete_storage_keys=("old-b", "old-a"),
        )
        assert created is False
        assert merged.id == first.id

    with sessions() as session:
        jobs = session.scalars(select(WorkJob)).all()
        assert len(jobs) == 1
        assert jobs[0].dedupe_key == "series:12:repair"
        payload = LibraryRepairPayload.model_validate(jobs[0].payload)
        assert payload.reason == "merge"
        assert payload.obsolete_storage_keys == ("old-a", "old-b")


def test_reconciler_collapses_legacy_artifact_jobs_per_series() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        queue = JobQueue()
        for key, reason, obsolete in (
            ("artifact:101", "download", ()),
            ("artifact:102", "download", ()),
            ("series:21:merge", "merge", ("old-title",)),
        ):
            queue.enqueue(
                session,
                kind=JobKind.LIBRARY_REPAIR,
                dedupe_key=key,
                payload=LibraryRepairPayload(
                    series_id=21,
                    reason=reason,
                    obsolete_storage_keys=obsolete,
                ),
                series_key="21",
            )
        canonicalized, cancelled = LibraryRepairPlanner(queue).reconcile_active_jobs(session)
        assert (canonicalized, cancelled) == (1, 2)

    with sessions() as session:
        active = session.scalars(
            select(WorkJob).where(WorkJob.status.in_(("queued", "leased", "retry_wait")))
        ).all()
        assert len(active) == 1
        assert active[0].dedupe_key == "series:21:repair"
        payload = LibraryRepairPayload.model_validate(active[0].payload)
        assert payload.reason == "merge"
        assert payload.obsolete_storage_keys == ("old-title",)
        assert session.scalar(
            select(func.count()).select_from(WorkJob).where(WorkJob.status == "cancelled")
        ) == 2


def test_reconciler_preserves_new_cleanup_request_while_repair_is_leased() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    queue = JobQueue()
    now = datetime.now(timezone.utc)
    with sessions() as session, session.begin():
        queue.enqueue(
            session,
            kind=JobKind.LIBRARY_REPAIR,
            dedupe_key="artifact:201",
            payload=LibraryRepairPayload(series_id=31, reason="download"),
            series_key="31",
            available_at=now,
        )
        lease = queue.claim(
            session,
            owner="repair-worker",
            lease_for=timedelta(minutes=5),
            now=now,
            kinds={JobKind.LIBRARY_REPAIR},
        )
        assert lease is not None
        queue.enqueue(
            session,
            kind=JobKind.LIBRARY_REPAIR,
            dedupe_key="series:31:merge",
            payload=LibraryRepairPayload(
                series_id=31,
                reason="merge",
                obsolete_storage_keys=("retired-folder",),
            ),
            series_key="31",
            available_at=now,
        )
        assert LibraryRepairPlanner(queue).reconcile_active_jobs(session) == (1, 1)
        current = session.get(WorkJob, lease.id)
        assert current is not None
        pending = LibraryRepairPayload.model_validate(current.pending_payload)
        assert pending.obsolete_storage_keys == ("retired-folder",)
        assert queue.succeed(
            session,
            job_id=lease.id,
            owner="repair-worker",
            now=now + timedelta(seconds=1),
        )
        assert current.status == "queued"
        assert current.dedupe_key == "series:31:repair"
