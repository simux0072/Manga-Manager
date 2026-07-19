from __future__ import annotations

import hashlib

from sqlalchemy import func, or_, select

from manga_manager.application.cover_evidence import ALGORITHM, CoverEvidenceService
from manga_manager.application.job_handlers import JobContext, RetryableJobError
from manga_manager.domain.jobs import CoverBackfillPayload, JobKind
from manga_manager.infrastructure.db_models import (
    CatalogCoverSignature,
    CatalogSourceSeries,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.worker.runtime import SessionFactory


class CoverBackfillHandler:
    def __init__(self, *, session_factory: SessionFactory) -> None:
        self.service = CoverEvidenceService(session_factory)

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, CoverBackfillPayload):
            raise RuntimeError("cover backfill handler received the wrong payload")
        context.ensure_lease()
        with self.service.session_factory() as session, session.begin():
            JobQueue().progress(
                session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message="fetching and fingerprinting cover",
                details={"phase": "cover", "processed": 0, "total": 1, "unit": "covers"},
            )
        await self.service.refresh_for_source_series(payload.source_series_id)
        context.ensure_lease()
        with self.service.session_factory() as session, session.begin():
            signature = session.get(CatalogCoverSignature, payload.source_series_id)
            if signature is None or signature.algorithm_version != ALGORITHM:
                raise RetryableJobError(
                    "cover_fetch_failed",
                    "provider cover could not be downloaded or decoded",
                )
            JobQueue().progress(
                session,
                job_id=context.lease.id,
                owner=context.lease.owner,
                message="cover signature ready",
                details={"phase": "cover", "processed": 1, "total": 1, "unit": "covers"},
            )


class CoverBackfillPlanner:
    def __init__(self, queue: JobQueue | None = None) -> None:
        self.queue = queue or JobQueue()

    def enqueue_pending(self, session, *, limit: int = 10, chapter_threshold: int = 25) -> int:
        active_chapters = int(
            session.scalar(
                select(func.count())
                .select_from(WorkJob)
                .where(
                    WorkJob.kind == JobKind.CHAPTER_DOWNLOAD.value,
                    WorkJob.status.in_(("queued", "leased", "retry_wait")),
                )
            )
            or 0
        )
        if active_chapters >= chapter_threshold:
            return 0
        rows = session.execute(
            select(CatalogSourceSeries, CatalogCoverSignature)
            .outerjoin(
                CatalogCoverSignature,
                CatalogCoverSignature.source_series_id == CatalogSourceSeries.id,
            )
            .where(CatalogSourceSeries.cover_url != "")
            .where(
                or_(
                    CatalogCoverSignature.source_series_id.is_(None),
                    CatalogCoverSignature.algorithm_version != ALGORITHM,
                    CatalogCoverSignature.hash_band_0 == "",
                )
            )
            .order_by(CatalogSourceSeries.last_checked_at.desc(), CatalogSourceSeries.id)
            .limit(max(limit * 20, 100))
        ).all()
        created = 0
        for row, signature in rows:
            revision_input = row.cover_url if signature is None else f"{ALGORITHM}:{row.cover_url}"
            revision = hashlib.sha1(revision_input.encode()).hexdigest()[:12]
            dedupe_key = f"cover:{row.id}:{revision}"
            exhausted = session.scalar(
                select(WorkJob.id)
                .where(
                    WorkJob.kind == JobKind.COVER_BACKFILL.value,
                    WorkJob.dedupe_key == dedupe_key,
                    WorkJob.status == "failed",
                )
                .limit(1)
            )
            if exhausted is not None:
                continue
            _job, was_created = self.queue.enqueue(
                session,
                kind=JobKind.COVER_BACKFILL,
                dedupe_key=dedupe_key,
                payload=CoverBackfillPayload(source_series_id=row.id),
                priority=300,
                source=row.source,
                group_key="cover-backfill",
            )
            created += int(was_created)
            if created >= limit:
                break
        return created
