from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from sqlalchemy import select

from manga_manager.infrastructure.db_models import (
    ArtifactBlob,
    ChapterArtifact,
    LibraryProjection,
)
from manga_manager.infrastructure.storage import ContentAddressedStorage, file_checksum
from manga_manager.worker.runtime import SessionFactory


@dataclass(frozen=True, slots=True)
class ExpectedProjection:
    blob_path: str
    checksum: str
    projection_path: str


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    checked: int = 0
    repaired: int = 0
    missing_blobs: int = 0
    orphan_blobs: int = 0
    stale_staging_removed: int = 0

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


class StorageReconciler:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        storage: ContentAddressedStorage,
    ) -> None:
        self.session_factory = session_factory
        self.storage = storage

    def run(
        self,
        *,
        remove_staging_older_than: timedelta = timedelta(days=1),
        now: datetime | None = None,
    ) -> ReconciliationReport:
        current = now or datetime.now(timezone.utc)
        expected, known_blobs = self._snapshot()
        repaired = 0
        missing_blobs = 0
        for row in expected:
            blob = self.storage.root / row.blob_path
            if not blob.is_file() or file_checksum(blob) != row.checksum:
                missing_blobs += 1
                continue
            projection = self.storage.library_root / row.projection_path
            if not projection.is_file() or file_checksum(projection) != row.checksum:
                self.storage.materialize(row.blob_path, row.projection_path)
                repaired += 1

        disk_blobs = (
            {
                path.relative_to(self.storage.root).as_posix()
                for path in self.storage.blob_root.rglob("*.cbz")
            }
            if self.storage.blob_root.exists()
            else set()
        )
        removed = self._remove_stale_staging(current, remove_staging_older_than)
        return ReconciliationReport(
            checked=len(expected),
            repaired=repaired,
            missing_blobs=missing_blobs,
            orphan_blobs=len(disk_blobs - known_blobs),
            stale_staging_removed=removed,
        )

    def _snapshot(self) -> tuple[list[ExpectedProjection], set[str]]:
        with self.session_factory() as session:
            rows = session.execute(
                select(
                    ArtifactBlob.relative_path,
                    ArtifactBlob.checksum,
                    LibraryProjection.relative_path,
                )
                .join(ChapterArtifact, ChapterArtifact.blob_checksum == ArtifactBlob.checksum)
                .join(LibraryProjection, LibraryProjection.artifact_id == ChapterArtifact.id)
                .where(ChapterArtifact.state == "active")
            ).all()
            known = set(session.scalars(select(ArtifactBlob.relative_path)).all())
        return [ExpectedProjection(*row) for row in rows], known

    def _remove_stale_staging(self, now: datetime, maximum_age: timedelta) -> int:
        if not self.storage.staging_root.exists():
            return 0
        cutoff = now.timestamp() - maximum_age.total_seconds()
        removed = 0
        for path in self.storage.staging_root.iterdir():
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink()
                removed += 1
        return removed
