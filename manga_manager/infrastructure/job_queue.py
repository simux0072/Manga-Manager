from __future__ import annotations

from datetime import datetime, timedelta, timezone
from collections.abc import Collection
from typing import Any

from sqlalchemy import Select, and_, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from manga_manager.domain.jobs import (
    ACTIVE_JOB_STATES,
    JobKind,
    JobLease,
    JobPayload,
    JobState,
    parse_job_payload,
)
from manga_manager.infrastructure.db_models import JobEvent, WorkJob


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _state_values(states) -> tuple[str, ...]:
    return tuple(state.value for state in states)


class JobQueue:
    """Transactional PostgreSQL-backed job queue.

    Methods flush changes but never commit. The application use case owns the
    transaction, which keeps enqueueing and related domain changes atomic.
    """

    def enqueue(
        self,
        session: Session,
        *,
        kind: JobKind,
        dedupe_key: str,
        payload: JobPayload | dict[str, Any],
        priority: int = 100,
        max_attempts: int = 3,
        available_at: datetime | None = None,
    ) -> tuple[WorkJob, bool]:
        key = dedupe_key.strip()
        if not key:
            raise ValueError("dedupe_key must not be empty")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        validated_payload = parse_job_payload(kind, payload)
        existing = self.active_job(session, kind=kind, dedupe_key=key)
        if existing is not None:
            return existing, False

        job = WorkJob(
            kind=kind.value,
            dedupe_key=key,
            payload=validated_payload.model_dump(mode="json"),
            priority=priority,
            max_attempts=max_attempts,
            available_at=available_at or utcnow(),
        )
        try:
            with session.begin_nested():
                session.add(job)
                session.flush()
        except IntegrityError:
            existing = self.active_job(session, kind=kind, dedupe_key=key)
            if existing is None:
                raise
            return existing, False
        self._record_event(session, job, "enqueued")
        return job, True

    def active_job(
        self,
        session: Session,
        *,
        kind: JobKind,
        dedupe_key: str,
    ) -> WorkJob | None:
        return session.scalar(
            select(WorkJob)
            .where(WorkJob.kind == kind.value)
            .where(WorkJob.dedupe_key == dedupe_key)
            .where(WorkJob.status.in_(_state_values(ACTIVE_JOB_STATES)))
            .order_by(WorkJob.id.desc())
            .limit(1)
        )

    def claim_query(
        self,
        *,
        now: datetime | None = None,
        kinds: Collection[JobKind] | None = None,
    ) -> Select[tuple[WorkJob]]:
        current = now or utcnow()
        ready = and_(
            WorkJob.status.in_([JobState.QUEUED.value, JobState.RETRY_WAIT.value]),
            WorkJob.available_at <= current,
        )
        expired = and_(
            WorkJob.status == JobState.LEASED.value,
            WorkJob.lease_expires_at.is_not(None),
            WorkJob.lease_expires_at <= current,
        )
        query = (
            select(WorkJob)
            .where(or_(ready, expired))
            .where(WorkJob.attempts < WorkJob.max_attempts)
            .order_by(WorkJob.priority.asc(), WorkJob.available_at.asc(), WorkJob.id.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if kinds is not None:
            values = [kind.value for kind in kinds]
            if not values:
                return query.where(False)
            query = query.where(WorkJob.kind.in_(values))
        return query

    def claim(
        self,
        session: Session,
        *,
        owner: str,
        lease_for: timedelta,
        now: datetime | None = None,
        kinds: Collection[JobKind] | None = None,
    ) -> JobLease | None:
        worker = owner.strip()
        if not worker:
            raise ValueError("owner must not be empty")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        current = now or utcnow()
        self.fail_exhausted_leases(session, now=current)
        job = session.scalar(self.claim_query(now=current, kinds=kinds))
        if job is None:
            return None

        expires_at = current + lease_for
        job.status = JobState.LEASED.value
        job.attempts += 1
        job.lease_owner = worker
        job.lease_expires_at = expires_at
        job.heartbeat_at = current
        job.error_code = ""
        job.error_message = ""
        job.updated_at = current
        session.flush()
        self._record_event(session, job, "leased", owner=worker)
        return JobLease(
            id=job.id,
            kind=JobKind(job.kind),
            dedupe_key=job.dedupe_key,
            payload=parse_job_payload(JobKind(job.kind), job.payload),
            priority=job.priority,
            attempt=job.attempts,
            max_attempts=job.max_attempts,
            owner=worker,
            expires_at=expires_at,
        )

    def fail_exhausted_leases(self, session: Session, *, now: datetime | None = None) -> int:
        current = now or utcnow()
        jobs = session.scalars(
            select(WorkJob)
            .where(WorkJob.status == JobState.LEASED.value)
            .where(WorkJob.lease_expires_at.is_not(None))
            .where(WorkJob.lease_expires_at <= current)
            .where(WorkJob.attempts >= WorkJob.max_attempts)
            .with_for_update(skip_locked=True)
        ).all()
        for job in jobs:
            prior_owner = job.lease_owner
            job.status = JobState.FAILED.value
            job.lease_owner = ""
            job.lease_expires_at = None
            job.heartbeat_at = None
            job.error_code = "lease_expired"
            job.error_message = "job lease expired after its final attempt"
            job.updated_at = current
            job.completed_at = current
            self._record_event(
                session,
                job,
                "lease_expired",
                owner=prior_owner,
                message=job.error_message,
            )
        session.flush()
        return len(jobs)

    def heartbeat(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        lease_for: timedelta,
        now: datetime | None = None,
    ) -> bool:
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        current = now or utcnow()
        result = session.execute(
            update(WorkJob)
            .where(WorkJob.id == job_id)
            .where(WorkJob.status == JobState.LEASED.value)
            .where(WorkJob.lease_owner == owner)
            .where(WorkJob.lease_expires_at > current)
            .values(
                heartbeat_at=current,
                lease_expires_at=current + lease_for,
                updated_at=current,
            )
        )
        return result.rowcount == 1

    def succeed(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        now: datetime | None = None,
    ) -> bool:
        return self._finish(
            session,
            job_id=job_id,
            owner=owner,
            state=JobState.SUCCEEDED,
            event_type="succeeded",
            now=now,
        )

    def fail(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        error_code: str,
        error_message: str,
        now: datetime | None = None,
    ) -> bool:
        return self._finish(
            session,
            job_id=job_id,
            owner=owner,
            state=JobState.FAILED,
            event_type="failed",
            error_code=error_code,
            error_message=error_message,
            now=now,
        )

    def retry(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        available_at: datetime,
        error_code: str,
        error_message: str,
        now: datetime | None = None,
    ) -> JobState | None:
        current = now or utcnow()
        job = session.scalar(
            select(WorkJob)
            .where(WorkJob.id == job_id)
            .where(WorkJob.status == JobState.LEASED.value)
            .where(WorkJob.lease_owner == owner)
            .where(WorkJob.lease_expires_at > current)
            .with_for_update()
        )
        if job is None:
            return None
        terminal = job.attempts >= job.max_attempts
        state = JobState.FAILED if terminal else JobState.RETRY_WAIT
        job.status = state.value
        job.available_at = available_at
        job.lease_owner = ""
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.error_code = error_code
        job.error_message = error_message
        job.updated_at = current
        job.completed_at = current if terminal else None
        self._record_event(
            session,
            job,
            "failed" if terminal else "retry_scheduled",
            owner=owner,
            message=error_message,
            details={"error_code": error_code, "available_at": available_at.isoformat()},
        )
        session.flush()
        return state

    def cancel(
        self,
        session: Session,
        *,
        job_id: int,
        reason: str,
        now: datetime | None = None,
    ) -> bool:
        current = now or utcnow()
        job = session.scalar(
            select(WorkJob)
            .where(WorkJob.id == job_id)
            .where(WorkJob.status.in_(_state_values(ACTIVE_JOB_STATES)))
            .with_for_update()
        )
        if job is None:
            return False
        owner = job.lease_owner
        job.status = JobState.CANCELLED.value
        job.lease_owner = ""
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.error_code = "cancelled"
        job.error_message = reason
        job.updated_at = current
        job.completed_at = current
        self._record_event(session, job, "cancelled", owner=owner, message=reason)
        session.flush()
        return True

    def progress(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        message: str,
        details: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool:
        current = now or utcnow()
        job = session.scalar(
            select(WorkJob)
            .where(WorkJob.id == job_id)
            .where(WorkJob.status == JobState.LEASED.value)
            .where(WorkJob.lease_owner == owner)
            .where(WorkJob.lease_expires_at > current)
        )
        if job is None:
            return False
        self._record_event(
            session,
            job,
            "progress",
            owner=owner,
            message=message,
            details=details,
        )
        session.flush()
        return True

    def release(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        reason: str,
        now: datetime | None = None,
    ) -> bool:
        current = now or utcnow()
        job = session.scalar(
            select(WorkJob)
            .where(WorkJob.id == job_id)
            .where(WorkJob.status == JobState.LEASED.value)
            .where(WorkJob.lease_owner == owner)
            .with_for_update()
        )
        if job is None:
            return False
        job.status = JobState.QUEUED.value
        job.attempts = max(job.attempts - 1, 0)
        job.available_at = current
        job.lease_owner = ""
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.error_code = "released"
        job.error_message = reason
        job.updated_at = current
        self._record_event(session, job, "released", owner=owner, message=reason)
        session.flush()
        return True

    def _finish(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        state: JobState,
        event_type: str,
        error_code: str = "",
        error_message: str = "",
        now: datetime | None = None,
    ) -> bool:
        current = now or utcnow()
        job = session.scalar(
            select(WorkJob)
            .where(WorkJob.id == job_id)
            .where(WorkJob.status == JobState.LEASED.value)
            .where(WorkJob.lease_owner == owner)
            .where(WorkJob.lease_expires_at > current)
            .with_for_update()
        )
        if job is None:
            return False
        job.status = state.value
        job.lease_owner = ""
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.error_code = error_code
        job.error_message = error_message
        job.updated_at = current
        job.completed_at = current
        self._record_event(
            session,
            job,
            event_type,
            owner=owner,
            message=error_message,
            details={"error_code": error_code} if error_code else None,
        )
        session.flush()
        return True

    def _record_event(
        self,
        session: Session,
        job: WorkJob,
        event_type: str,
        *,
        owner: str = "",
        message: str = "",
        details: dict[str, Any] | None = None,
    ) -> None:
        session.add(
            JobEvent(
                job_id=job.id,
                event_type=event_type,
                status=job.status,
                owner=owner,
                message=message[:4000],
                details=dict(details or {}),
            )
        )
