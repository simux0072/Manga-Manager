from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from manga_manager.infrastructure.db_models import WorkerHeartbeat


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerRegistry:
    def register(
        self,
        session: Session,
        *,
        worker_id: str,
        metadata: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> WorkerHeartbeat:
        current = now or utcnow()
        worker = session.get(WorkerHeartbeat, worker_id)
        if worker is None:
            worker = WorkerHeartbeat(worker_id=worker_id, started_at=current)
            session.add(worker)
        else:
            worker.started_at = current
        worker.status = "running"
        worker.active_job_id = None
        worker.heartbeat_at = current
        worker.stopped_at = None
        worker.metadata_json = dict(metadata or {})
        session.flush()
        return worker

    def heartbeat(
        self,
        session: Session,
        *,
        worker_id: str,
        active_job_id: int | None = None,
        status: str = "running",
        now: datetime | None = None,
    ) -> bool:
        current = now or utcnow()
        worker = session.get(WorkerHeartbeat, worker_id)
        if worker is None:
            return False
        worker.status = status
        worker.active_job_id = active_job_id
        worker.heartbeat_at = current
        worker.stopped_at = current if status == "stopped" else None
        session.flush()
        return True

    def live_workers(
        self,
        session: Session,
        *,
        seen_within: timedelta,
        now: datetime | None = None,
    ) -> list[WorkerHeartbeat]:
        current = now or utcnow()
        cutoff = current - seen_within
        return list(
            session.scalars(
                select(WorkerHeartbeat)
                .where(WorkerHeartbeat.status.in_(["starting", "running", "draining"]))
                .where(WorkerHeartbeat.heartbeat_at >= cutoff)
                .order_by(WorkerHeartbeat.worker_id)
            )
        )
