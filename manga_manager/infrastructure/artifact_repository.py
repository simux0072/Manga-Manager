from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from manga_manager.infrastructure.db_models import (
    ArtifactBlob,
    ChapterArtifact,
    LibraryProjection,
)
from manga_manager.infrastructure.storage import StoredBlob


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class ActivationResult:
    status: str
    artifact_id: int
    projection_relative_path: str


class ArtifactRepository:
    def activate(
        self,
        session: Session,
        *,
        chapter_id: int,
        chapter_release_id: int | None,
        blob: StoredBlob,
        projection_relative_path: str,
        provenance: str,
        source: str = "",
        quality_rank: int = 0,
        replace: bool = False,
    ) -> ActivationResult:
        stored_blob = session.get(ArtifactBlob, blob.checksum)
        if stored_blob is None:
            stored_blob = ArtifactBlob(
                checksum=blob.checksum,
                relative_path=blob.relative_path,
                byte_count=blob.byte_count,
            )
            session.add(stored_blob)
        active = session.scalar(
            select(ChapterArtifact).where(
                ChapterArtifact.chapter_id == chapter_id,
                ChapterArtifact.state == "active",
            )
        )
        if active is not None and active.blob_checksum == blob.checksum:
            active.chapter_release_id = chapter_release_id or active.chapter_release_id
            active.source = source or active.source
            active.quality_rank = max(active.quality_rank or 0, quality_rank)
            projection = self._upsert_projection(
                session, chapter_id, active.id, projection_relative_path
            )
            return ActivationResult("duplicate", active.id, projection.relative_path)
        if active is not None and not replace:
            quarantined = ChapterArtifact(
                chapter_id=chapter_id,
                chapter_release_id=chapter_release_id,
                blob_checksum=blob.checksum,
                state="quarantined",
                provenance=provenance,
                source=source,
                quality_rank=quality_rank,
                image_count=blob.image_count,
            )
            session.add(quarantined)
            session.flush()
            return ActivationResult("conflict", quarantined.id, projection_relative_path)
        if active is not None:
            active.state = "inactive"
            active.deactivated_at = utcnow()

        artifact = ChapterArtifact(
            chapter_id=chapter_id,
            chapter_release_id=chapter_release_id,
            blob_checksum=blob.checksum,
            provenance=provenance,
            source=source,
            quality_rank=quality_rank,
            image_count=blob.image_count,
        )
        session.add(artifact)
        session.flush()
        projection = self._upsert_projection(
            session, chapter_id, artifact.id, projection_relative_path
        )
        session.flush()
        return ActivationResult("activated", artifact.id, projection.relative_path)

    def _upsert_projection(
        self,
        session: Session,
        chapter_id: int,
        artifact_id: int,
        relative_path: str,
    ) -> LibraryProjection:
        projection = session.get(LibraryProjection, chapter_id)
        if projection is None:
            projection = LibraryProjection(
                chapter_id=chapter_id,
                artifact_id=artifact_id,
                relative_path=relative_path,
            )
            session.add(projection)
        else:
            projection.artifact_id = artifact_id
            projection.relative_path = relative_path
            projection.updated_at = utcnow()
        return projection
