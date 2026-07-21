from __future__ import annotations

from sqlalchemy import or_, select
from sqlalchemy.orm import aliased

from manga_manager.application.matching_score import SCORER_VERSION
from manga_manager.domain.jobs import JobKind, MaintenancePayload
from manga_manager.infrastructure.db_models import (
    CatalogMatchDecision,
    CatalogSourceSeries,
)
from manga_manager.infrastructure.job_queue import JobQueue


class MatchRescorePlanner:
    """Queue bounded evidence upgrades without doing image work in an HTTP request."""

    def __init__(self, queue: JobQueue | None = None) -> None:
        self.queue = queue or JobQueue()

    def enqueue_pending(self, session, *, limit: int = 10) -> int:
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
            .limit(max(limit * 20, 100))
        ).all()
        created = 0
        for series_id in dict.fromkeys(row[0] for row in rows):
            _job, was_created = self.queue.enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key=f"match-rescore:{series_id}:{SCORER_VERSION}",
                payload=MaintenancePayload(action="rescore_matches", series_id=series_id),
                priority=310,
                series_key=str(series_id),
                pool="maintenance",
                group_key="match-rescore",
            )
            created += int(was_created)
            if created >= limit:
                break
        return created
