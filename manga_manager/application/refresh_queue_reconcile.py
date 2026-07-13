from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError
from sqlalchemy import select

from manga_manager.domain.jobs import JobKind, SourceRefreshPayload
from manga_manager.infrastructure.db_models import WorkJob
from manga_manager.infrastructure.job_queue import JobQueue


COMPATIBLE_REFRESH_VERSIONS = frozenset({1, 2})


@dataclass(slots=True)
class RefreshQueueRecord:
    job_id: int
    source: str
    source_id: str
    payload_version: int
    action: str
    reason: str


class RefreshQueueReconciler:
    """Audit and repair active refresh payloads without discarding discovered work."""

    def __init__(self, queue: JobQueue | None = None) -> None:
        self.queue = queue or JobQueue()

    def audit(self, session, *, lock: bool = False) -> list[RefreshQueueRecord]:
        records: list[RefreshQueueRecord] = []
        query = (
            select(WorkJob)
            .where(
                WorkJob.kind == JobKind.SOURCE_REFRESH.value,
                WorkJob.status.in_(("queued", "leased", "retry_wait")),
            )
            .order_by(WorkJob.id)
        )
        if lock:
            query = query.with_for_update(skip_locked=True)
        rows = session.scalars(query).all()
        for row in rows:
            payload = dict(row.payload or {})
            try:
                version = int(payload.get("version") or 1)
            except (TypeError, ValueError):
                version = 0
            source_id = str(payload.get("source_id") or "")
            try:
                self._normalize_payload(payload)
                rebuildable = True
            except ValidationError:
                rebuildable = False
            if not rebuildable:
                action = "blocked"
                reason = "payload lacks fields required for a lossless rebuild"
            elif version not in COMPATIBLE_REFRESH_VERSIONS:
                action = "defer_rebuild" if row.status == "leased" else "rebuild"
                reason = f"payload version {version} is not supported"
            elif not row.workflow_key or not row.group_key:
                action = "regroup"
                reason = "compatible payload is missing durable workflow metadata"
            elif version == 1:
                action = "upgrade"
                reason = "compatible v1 payload can be normalized in place"
            else:
                action = "preserve"
                reason = "payload and workflow metadata are compatible"
            records.append(
                RefreshQueueRecord(
                    job_id=row.id,
                    source=row.source,
                    source_id=source_id,
                    payload_version=version,
                    action=action,
                    reason=reason,
                )
            )
        return records

    def apply(self, session, records: list[RefreshQueueRecord]) -> None:
        for record in records:
            if record.action in {"preserve", "blocked"}:
                continue
            job = session.get(WorkJob, record.job_id)
            if job is None or job.status not in {"queued", "leased", "retry_wait"}:
                continue
            normalized = self._normalize_payload(job.payload)
            workflow = job.workflow_key or f"pull:{job.source}:legacy"
            if record.action == "defer_rebuild":
                job.pending_payload = normalized
            elif record.action == "rebuild":
                identity = str(normalized["source_id"])
                priority = job.priority
                max_attempts = job.max_attempts
                source = job.source
                self.queue.cancel(
                    session,
                    job_id=job.id,
                    reason="incompatible refresh payload replaced by current schema",
                )
                self.queue.enqueue(
                    session,
                    kind=JobKind.SOURCE_REFRESH,
                    dedupe_key=f"refresh:{source}:{identity}",
                    payload=normalized,
                    priority=priority,
                    max_attempts=max_attempts,
                    source=source,
                    workflow_key=workflow,
                    group_key=workflow,
                    coalesce=True,
                )
            else:
                job.payload = normalized
                job.workflow_key = workflow
                job.group_key = workflow

    @staticmethod
    def _normalize_payload(raw: dict[str, Any]) -> dict[str, Any]:
        allowed = SourceRefreshPayload.model_fields
        values = {key: value for key, value in dict(raw or {}).items() if key in allowed}
        values["version"] = 2
        return SourceRefreshPayload.model_validate(values).model_dump(mode="json")
