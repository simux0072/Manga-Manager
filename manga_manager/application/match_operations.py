from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException
from sqlalchemy import delete, select

from manga_manager.application.job_handlers import JobContext, PermanentJobError
from manga_manager.application.match_training import record_training_label
from manga_manager.domain.jobs import JobKind, MatchOperationPayload
from manga_manager.infrastructure.db_models import (
    CatalogMatchDecision,
    CatalogSourceSeries,
    MatchOperation,
    MatchOperationSeries,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.worker.runtime import SessionFactory


ACTIVE_OPERATION_STATUSES = ("queued", "running")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def serialize_match_operation(operation: MatchOperation) -> dict[str, Any]:
    return {
        "id": operation.id,
        "action": operation.action,
        "status": operation.status,
        "representative_id": operation.representative_id,
        "decision_ids": list(operation.decision_ids or []),
        "proposal_ids": list(operation.proposal_ids or []),
        "series_ids": list(operation.series_ids or []),
        "job_id": operation.job_id,
        "error_code": operation.error_code,
        "error_message": operation.error_message,
        "created_at": operation.created_at.isoformat(),
        "updated_at": operation.updated_at.isoformat(),
        "completed_at": operation.completed_at.isoformat()
        if operation.completed_at
        else None,
    }


def find_equivalent_active_operation(
    session,
    *,
    action: str,
    decision_ids: list[int],
    series_ids: list[int],
) -> MatchOperation | None:
    """Return the existing submission for safe double-click/request retries."""
    expected_decisions = sorted(set(decision_ids))
    expected_series = sorted(set(series_ids))
    candidates = session.scalars(
        select(MatchOperation)
        .where(MatchOperation.status.in_(ACTIVE_OPERATION_STATUSES))
        .order_by(MatchOperation.id.desc())
    ).all()
    return next(
        (
            row
            for row in candidates
            if row.action == action
            and sorted(row.decision_ids or []) == expected_decisions
            and sorted(row.series_ids or []) == expected_series
        ),
        None,
    )


def enqueue_match_operation(
    session,
    *,
    action: str,
    representative_id: int,
    decision_ids: list[int],
    proposal_ids: list[int],
    series_ids: list[int],
    priority: int = 70,
) -> tuple[MatchOperation, bool]:
    unique_decisions = sorted(set(decision_ids))
    unique_proposals = sorted(set(proposal_ids))
    unique_series = sorted(set(series_ids))
    existing = find_equivalent_active_operation(
        session,
        action=action,
        decision_ids=unique_decisions,
        series_ids=unique_series,
    )
    if existing is not None:
        return existing, False
    conflict = session.scalar(
        select(MatchOperationSeries.series_id)
        .where(MatchOperationSeries.series_id.in_(unique_series))
        .limit(1)
    )
    if conflict is not None:
        raise HTTPException(
            409,
            "another merge or split is already using one of these manga",
        )
    operation = MatchOperation(
        action=action,
        status="queued",
        representative_id=representative_id,
        decision_ids=unique_decisions,
        proposal_ids=unique_proposals,
        series_ids=unique_series,
    )
    session.add(operation)
    session.flush([operation])
    session.add_all(
        MatchOperationSeries(operation_id=operation.id, series_id=series_id)
        for series_id in unique_series
    )
    session.flush()
    job, _created = JobQueue().enqueue(
        session,
        kind=JobKind.MATCH_OPERATION,
        dedupe_key=f"match-operation:{operation.id}",
        payload=MatchOperationPayload(operation_id=operation.id),
        priority=priority,
        max_attempts=3,
        pool="catalog_mutation",
        group_key="match-operations",
    )
    operation.job_id = job.id
    operation.updated_at = utcnow()
    session.flush([operation])
    return operation, True


def reconcile_stranded_match_operations(session) -> int:
    """Release reservations whose durable queue job can no longer run."""
    operations = session.scalars(
        select(MatchOperation).where(
            MatchOperation.status.in_(ACTIVE_OPERATION_STATUSES)
        )
    ).all()
    changed = 0
    for operation in operations:
        job = session.get(WorkJob, operation.job_id) if operation.job_id else None
        if job is not None and job.status in {"queued", "leased", "retry_wait"}:
            continue
        operation.status = "failed"
        operation.error_code = "match_worker_interrupted"
        operation.error_message = (
            "The background worker stopped before this operation completed. Retry it when ready."
        )
        operation.updated_at = utcnow()
        operation.completed_at = operation.updated_at
        session.execute(
            delete(MatchOperationSeries).where(
                MatchOperationSeries.operation_id == operation.id
            )
        )
        changed += 1
    return changed


class MatchOperationHandler:
    def __init__(self, *, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, MatchOperationPayload):
            raise PermanentJobError(
                "invalid_match_operation", "match operation handler received the wrong payload"
            )
        context.ensure_lease()
        await asyncio.to_thread(self._execute, payload.operation_id)
        context.ensure_lease()

    def _execute(self, operation_id: int) -> None:
        with self.session_factory() as session, session.begin():
            operation = session.scalar(
                select(MatchOperation)
                .where(MatchOperation.id == operation_id)
                .with_for_update()
            )
            if operation is None:
                raise PermanentJobError(
                    "match_operation_missing", "the queued match operation no longer exists"
                )
            if operation.status == "succeeded":
                return
            if operation.status == "failed":
                raise PermanentJobError(
                    operation.error_code or "match_operation_failed",
                    operation.error_message or "the match operation previously failed",
                )
            operation.status = "running"
            operation.updated_at = utcnow()

        try:
            with self.session_factory() as session, session.begin():
                operation = session.scalar(
                    select(MatchOperation)
                    .where(MatchOperation.id == operation_id)
                    .with_for_update()
                )
                if operation is None:
                    raise RuntimeError("the queued match operation no longer exists")
                decisions = session.scalars(
                    select(CatalogMatchDecision)
                    .where(CatalogMatchDecision.id.in_(operation.decision_ids or [-1]))
                    .with_for_update()
                ).all()
                pending = [row for row in decisions if row.decision == "pending"]
                if len(pending) != len(operation.decision_ids):
                    raise HTTPException(
                        409, "one or more match decisions were already reviewed"
                    )
                for decision in pending:
                    record_training_label(
                        session,
                        left_source_series_id=decision.left_source_series_id,
                        right_source_series_id=decision.right_source_series_id,
                        label=int(operation.action == "accepted"),
                        origin="async_review",
                        decision=decision,
                    )
                    decision.decision = operation.action
                    decision.decided_by = "operator"
                    decision.decided_at = utcnow()
                if operation.action == "accepted" and not operation.decision_ids:
                    identities = session.scalars(
                        select(CatalogSourceSeries).where(
                            CatalogSourceSeries.series_id.in_(operation.series_ids)
                        )
                    ).all()
                    for index, left in enumerate(identities):
                        for right in identities[index + 1 :]:
                            if left.series_id == right.series_id or left.source == right.source:
                                continue
                            record_training_label(
                                session,
                                left_source_series_id=left.id,
                                right_source_series_id=right.id,
                                label=1,
                                origin="manual_merge",
                            )
                if operation.action == "accepted":
                    # Kept as a local import so the domain mutation can be extracted later
                    # without introducing an application-startup import cycle.
                    from manga_manager.web.app import merge_canonical_series

                    merge_canonical_series(session, list(operation.series_ids))
                operation.status = "succeeded"
                operation.error_code = ""
                operation.error_message = ""
                operation.updated_at = utcnow()
                operation.completed_at = operation.updated_at
                session.execute(
                    delete(MatchOperationSeries).where(
                        MatchOperationSeries.operation_id == operation.id
                    )
                )
        except Exception as error:
            detail = error.detail if isinstance(error, HTTPException) else str(error)
            message = str(detail).strip() or type(error).__name__
            code = (
                f"match_operation_http_{error.status_code}"
                if isinstance(error, HTTPException)
                else "match_operation_failed"
            )
            with self.session_factory() as session, session.begin():
                operation = session.get(MatchOperation, operation_id)
                if operation is not None:
                    operation.status = "failed"
                    operation.error_code = code
                    operation.error_message = message[:4000]
                    operation.updated_at = utcnow()
                    operation.completed_at = operation.updated_at
                    session.execute(
                        delete(MatchOperationSeries).where(
                            MatchOperationSeries.operation_id == operation_id
                        )
                    )
            raise PermanentJobError(code, message) from error
