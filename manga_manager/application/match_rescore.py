from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import aliased

from manga_manager.application.matching_score import SCORER_VERSION
from manga_manager.domain.jobs import JobKind, MaintenancePayload
from manga_manager.infrastructure.db_models import (
    CatalogMatchDecision,
    CatalogSourceSeries,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue


class MatchRescorePlanner:
    """Queue bounded evidence upgrades without doing image work in an HTTP request."""

    def __init__(self, queue: JobQueue | None = None) -> None:
        self.queue = queue or JobQueue()

    def reconcile_active(self, session, *, limit: int = 10) -> int:
        """Bound old rescoring backlogs and remove their merge-locking hint.

        A scorer-version upgrade can make hundreds of series eligible at once.
        Only a small rolling window needs to be durable; cancelled overflow is
        eligible again after the retained window has completed.
        """
        candidates = [
            job
            for job in session.scalars(
                select(WorkJob)
                .where(
                    WorkJob.kind == JobKind.MAINTENANCE.value,
                    WorkJob.pool == "maintenance",
                    WorkJob.status.in_(("queued", "leased", "retry_wait")),
                )
                .order_by((WorkJob.status == "leased").desc(), WorkJob.id)
            ).all()
            if (job.payload or {}).get("action") == "rescore_matches"
        ]
        for job in candidates:
            # Rescoring only reads matching evidence and is safe during a merge.
            job.series_key = ""
        cancelled = 0
        for job in candidates[max(limit, 0) :]:
            if job.status == "leased":
                continue
            cancelled += int(
                self.queue.cancel(
                    session,
                    job_id=job.id,
                    reason="deferred by bounded match-rescore backlog",
                )
            )
        return cancelled

    def enqueue_pending(self, session, *, limit: int = 10) -> int:
        active = [
            job
            for job in session.scalars(
                select(WorkJob).where(
                    WorkJob.kind == JobKind.MAINTENANCE.value,
                    WorkJob.pool == "maintenance",
                    WorkJob.status.in_(("queued", "leased", "retry_wait")),
                )
            ).all()
            if (job.payload or {}).get("action") == "rescore_matches"
        ]
        capacity = max(limit - len(active), 0)
        if capacity == 0:
            return 0
        left = aliased(CatalogSourceSeries)
        evidence_version = CatalogMatchDecision.evidence_json["scorer_version"].as_string()
        rows = session.execute(
            select(left.series_id)
            .select_from(CatalogMatchDecision)
            .join(left, left.id == CatalogMatchDecision.left_source_series_id)
            .where(
                CatalogMatchDecision.decision == "pending",
                or_(evidence_version.is_(None), evidence_version != SCORER_VERSION),
            )
            .order_by(CatalogMatchDecision.id)
            .limit(max(capacity * 20, 100))
        ).all()
        created = 0
        for series_id in dict.fromkeys(row[0] for row in rows):
            _job, was_created = self.queue.enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key=f"match-rescore:{series_id}:{SCORER_VERSION}",
                payload=MaintenancePayload(action="rescore_matches", series_id=series_id),
                priority=310,
                # This is read-only evidence work and must not block a manual merge.
                series_key="",
                pool="maintenance",
                group_key="match-rescore",
            )
            created += int(was_created)
            if created >= capacity:
                break
        return created
