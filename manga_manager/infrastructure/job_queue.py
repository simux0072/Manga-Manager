from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from collections.abc import Collection
from typing import Any

from sqlalchemy import Select, and_, delete, func, or_, select, text, update
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
from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    JobEvent,
    JobPermit,
    ProviderPolicy,
    StorageReservation,
    StorageState,
    WorkJob,
    WorkloadCycle,
)


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
        source: str = "",
        series_key: str = "",
        pool: str = "",
        workflow_key: str = "",
        group_key: str = "",
        logical_units: int = 1,
        coalesce: bool = False,
    ) -> tuple[WorkJob, bool]:
        key = dedupe_key.strip()
        if not key:
            raise ValueError("dedupe_key must not be empty")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        validated_payload = parse_job_payload(kind, payload)
        routed_source = source.strip()
        if kind in {JobKind.SOURCE_PULL, JobKind.SOURCE_REFRESH}:
            routed_source = validated_payload.source
        routed_pool = pool.strip() or default_pool(kind, routed_source)
        existing = self.active_job(session, kind=kind, dedupe_key=key)
        if existing is not None:
            if coalesce:
                replacement = validated_payload.model_dump(mode="json")
                current_payload = (
                    existing.pending_payload
                    if existing.status == JobState.LEASED.value and existing.pending_payload
                    else existing.payload
                )
                if not self._should_coalesce(kind, current_payload, replacement):
                    return existing, False
                if existing.status == JobState.LEASED.value:
                    existing.pending_payload = replacement
                else:
                    existing.payload = replacement
                    existing.error_code = ""
                    existing.error_message = ""
                    existing.updated_at = utcnow()
            return existing, False

        cycle = self._active_cycle(session)
        routed_workflow = workflow_key.strip() or getattr(validated_payload, "workflow_key", "")
        routed_group = group_key.strip() or self._default_group_key(
            kind, key, routed_source, series_key.strip(), routed_workflow, cycle.id
        )

        job = WorkJob(
            kind=kind.value,
            dedupe_key=key,
            payload=validated_payload.model_dump(mode="json"),
            priority=priority,
            max_attempts=max_attempts,
            available_at=available_at or utcnow(),
            source=routed_source,
            series_key=series_key.strip(),
            pool=routed_pool,
            cycle_id=cycle.id,
            workflow_key=routed_workflow,
            group_key=routed_group,
            logical_units=max(logical_units, 1),
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
        cycle.total_units += job.logical_units
        cycle.added_units += job.logical_units
        cycle.updated_at = utcnow()
        return job, True

    def reroute(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        payload: JobPayload | dict[str, Any],
        source: str,
        available_at: datetime,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> bool:
        """Return a leased logical job to the queue with a different provider in place."""
        current = utcnow()
        job = session.scalar(
            select(WorkJob)
            .where(WorkJob.id == job_id)
            .where(WorkJob.status == JobState.LEASED.value)
            .where(WorkJob.lease_owner == owner)
            .with_for_update()
        )
        if job is None:
            return False
        validated = parse_job_payload(JobKind(job.kind), payload)
        job.payload = validated.model_dump(mode="json")
        job.source = source.strip()
        job.pool = default_pool(JobKind(job.kind), job.source)
        job.status = JobState.RETRY_WAIT.value
        job.available_at = available_at
        job.lease_owner = ""
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.error_code = "rerouted"
        job.error_message = message[:4000]
        job.updated_at = current
        self._release_permits(session, job.id)
        self._record_event(
            session, job, "rerouted", owner=owner, message=message, details=details
        )
        session.flush()
        return True

    def reroute_waiting(
        self,
        session: Session,
        *,
        job_id: int,
        payload: JobPayload | dict[str, Any],
        source: str,
        available_at: datetime,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> bool:
        job = session.scalar(
            select(WorkJob)
            .where(WorkJob.id == job_id)
            .where(WorkJob.status.in_((JobState.QUEUED.value, JobState.RETRY_WAIT.value)))
            .with_for_update()
        )
        if job is None:
            return False
        validated = parse_job_payload(JobKind(job.kind), payload)
        job.payload = validated.model_dump(mode="json")
        job.source = source.strip()
        job.pool = default_pool(JobKind(job.kind), job.source)
        job.status = JobState.RETRY_WAIT.value
        job.available_at = available_at
        job.error_code = "rerouted"
        job.error_message = message[:4000]
        job.updated_at = utcnow()
        self._record_event(session, job, "rerouted", message=message, details=details)
        session.flush()
        return True

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
        pools: Collection[str] | None = None,
        exclude_ids: Collection[int] | None = None,
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
            .where(
                or_(
                    WorkJob.kind != JobKind.CHAPTER_DOWNLOAD.value,
                    ~select(CatalogSourceState.source)
                    .where(CatalogSourceState.source == WorkJob.source)
                    .where(
                        or_(
                            CatalogSourceState.manual_enabled.is_(False),
                            CatalogSourceState.cooldown_until > current,
                        )
                    )
                    .exists(),
                )
            )
            .where(
                or_(
                    WorkJob.kind != JobKind.CHAPTER_DOWNLOAD.value,
                    ~select(StorageState.id)
                    .where(StorageState.id == 1)
                    .where(StorageState.paused.is_(True))
                    .exists(),
                )
            )
            .order_by(WorkJob.priority.asc(), WorkJob.available_at.asc(), WorkJob.id.asc())
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if kinds is not None:
            values = [kind.value for kind in kinds]
            if not values:
                return query.where(False)
            query = query.where(WorkJob.kind.in_(values))
        if pools is not None:
            values = [pool for pool in pools if pool]
            if not values:
                return query.where(False)
            query = query.where(WorkJob.pool.in_(values))
        if exclude_ids:
            query = query.where(WorkJob.id.not_in(tuple(exclude_ids)))
        return query

    def claim(
        self,
        session: Session,
        *,
        owner: str,
        lease_for: timedelta,
        now: datetime | None = None,
        kinds: Collection[JobKind] | None = None,
        pool_limits: dict[str, int] | None = None,
        pools: Collection[str] | None = None,
    ) -> JobLease | None:
        worker = owner.strip()
        if not worker:
            raise ValueError("owner must not be empty")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        current = now or utcnow()
        self.fail_exhausted_leases(session, now=current)
        self._expire_permits(session, current)
        limits = pool_limits or {}
        job = None
        rejected: set[int] = set()
        for _ in range(50):
            candidate = session.scalar(
                self.claim_query(
                    now=current,
                    kinds=kinds,
                    pools=pools,
                    exclude_ids=rejected,
                )
            )
            if candidate is None:
                break
            if self._acquire_permits(
                session,
                candidate,
                owner=worker,
                expires_at=current + lease_for,
                now=current,
                limits=limits,
            ):
                job = candidate
                break
            rejected.add(candidate.id)
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
            source=job.source,
            series_key=job.series_key,
            pool=job.pool,
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
            self._release_permits(session, job.id)
            self._record_terminal_units(session, job, JobState.FAILED, current)
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
        if result.rowcount == 1:
            session.execute(
                update(JobPermit)
                .where(JobPermit.job_id == job_id)
                .where(JobPermit.owner == owner)
                .values(lease_expires_at=current + lease_for)
            )
            session.execute(
                update(StorageReservation)
                .where(StorageReservation.job_id == job_id)
                .where(StorageReservation.owner == owner)
                .values(lease_expires_at=current + lease_for)
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
        self._release_permits(session, job.id)
        self._record_event(
            session,
            job,
            "failed" if terminal else "retry_scheduled",
            owner=owner,
            message=error_message,
            details={"error_code": error_code, "available_at": available_at.isoformat()},
        )
        if terminal:
            self._record_terminal_units(session, job, JobState.FAILED, current)
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
        self._release_permits(session, job.id)
        self._record_event(session, job, "cancelled", owner=owner, message=reason)
        self._record_terminal_units(session, job, JobState.CANCELLED, current)
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
        payload = dict(details or {})
        job.progress_phase = str(payload.get("phase") or job.progress_phase or "working")[:50]
        job.progress_current = max(int(payload.get("processed") or payload.get("current") or 0), 0)
        job.progress_total = max(int(payload.get("total") or 0), 0)
        job.progress_unit = str(payload.get("unit") or job.progress_unit or "items")[:30]
        job.progress_bytes = max(int(payload.get("bytes") or job.progress_bytes or 0), 0)
        job.progress_message = message[:4000]
        job.progress_updated_at = current
        job.updated_at = current
        self._record_event(
            session,
            job,
            "progress",
            owner=owner,
            message=message,
            details=payload,
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
        self._release_permits(session, job.id)
        self._record_event(session, job, "released", owner=owner, message=reason)
        session.flush()
        return True

    def defer(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        available_at: datetime,
        code: str,
        message: str,
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
        job.status = JobState.RETRY_WAIT.value
        job.attempts = max(job.attempts - 1, 0)
        job.available_at = available_at
        job.lease_owner = ""
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.error_code = code
        job.error_message = message
        job.updated_at = current
        self._release_permits(session, job.id)
        self._record_event(
            session,
            job,
            "retry_scheduled",
            owner=owner,
            message=message,
            details={"error_code": code, "available_at": available_at.isoformat(), "blocked": True},
        )
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
        if state is JobState.SUCCEEDED and job.pending_payload:
            job.payload = dict(job.pending_payload)
            job.pending_payload = {}
            job.status = JobState.QUEUED.value
            job.available_at = current
            job.lease_owner = ""
            job.lease_expires_at = None
            job.heartbeat_at = None
            job.error_code = "coalesced_observation"
            job.error_message = "a newer provider observation was coalesced while leased"
            job.updated_at = current
            self._release_permits(session, job.id)
            self._record_event(
                session,
                job,
                "released",
                owner=owner,
                message=job.error_message,
                details={"coalesced": True},
            )
            session.flush()
            return True
        job.status = state.value
        job.lease_owner = ""
        job.lease_expires_at = None
        job.heartbeat_at = None
        job.error_code = error_code
        job.error_message = error_message
        job.updated_at = current
        job.completed_at = current
        self._release_permits(session, job.id)
        self._record_event(
            session,
            job,
            event_type,
            owner=owner,
            message=error_message,
            details={"error_code": error_code} if error_code else None,
        )
        self._record_terminal_units(session, job, state, current)
        session.flush()
        return True

    def _active_cycle(self, session: Session) -> WorkloadCycle:
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            session.execute(text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": 0x4D4D4359})
        cycle = session.scalar(
            select(WorkloadCycle)
            .where(WorkloadCycle.status == "active")
            .order_by(WorkloadCycle.id.desc())
            .with_for_update()
            .limit(1)
        )
        if cycle is not None:
            active = session.scalar(
                select(func.count())
                .select_from(WorkJob)
                .where(
                    WorkJob.cycle_id == cycle.id,
                    WorkJob.status.in_(_state_values(ACTIVE_JOB_STATES)),
                )
            )
            if not active:
                cycle.status = "settled"
                cycle.settled_at = utcnow()
                cycle.updated_at = cycle.settled_at
                session.flush([cycle])
                cycle = None
        if cycle is None:
            cycle = WorkloadCycle()
            session.add(cycle)
            session.flush()
        return cycle

    def _record_terminal_units(
        self, session: Session, job: WorkJob, state: JobState, now: datetime
    ) -> None:
        if job.cycle_id is None:
            return
        cycle = session.get(WorkloadCycle, job.cycle_id)
        if cycle is None:
            return
        units = max(job.logical_units, 1)
        if state is JobState.SUCCEEDED:
            cycle.successful_units += units
        elif state is JobState.FAILED:
            cycle.failed_units += units
        elif state is JobState.CANCELLED:
            cycle.cancelled_units += units
        cycle.updated_at = now
        remaining = session.scalar(
            select(func.count())
            .select_from(WorkJob)
            .where(WorkJob.cycle_id == cycle.id)
            .where(WorkJob.id != job.id)
            .where(WorkJob.status.in_(_state_values(ACTIVE_JOB_STATES)))
        )
        if not remaining:
            cycle.status = "settled"
            cycle.settled_at = now

    @staticmethod
    def _default_group_key(
        kind: JobKind,
        dedupe_key: str,
        source: str,
        series_key: str,
        workflow_key: str,
        cycle_id: int,
    ) -> str:
        if kind is JobKind.CHAPTER_DOWNLOAD and series_key:
            return f"cycle:{cycle_id}:download:{series_key}"
        if kind in {JobKind.SOURCE_PULL, JobKind.SOURCE_REFRESH}:
            return workflow_key or f"pull:{source}"
        if kind in {
            JobKind.LIBRARY_REPAIR,
            JobKind.KAVITA_SYNC,
            JobKind.COVER_BACKFILL,
            JobKind.MAINTENANCE,
        }:
            return f"cycle:{cycle_id}:maintenance:{kind.value}"
        return f"{kind.value}:{dedupe_key}"

    @staticmethod
    def _should_coalesce(
        kind: JobKind, current: dict[str, Any], replacement: dict[str, Any]
    ) -> bool:
        if kind is not JobKind.SOURCE_REFRESH:
            return current != replacement
        old = str(current.get("observation_version") or "").strip()
        new = str(replacement.get("observation_version") or "").strip()
        if old and new:
            try:
                return Decimal(new) > Decimal(old)
            except InvalidOperation:
                if new == old:
                    return False
        return current != replacement

    def _expire_permits(self, session: Session, now: datetime) -> None:
        session.execute(delete(JobPermit).where(JobPermit.lease_expires_at <= now))

    def _acquire_permits(
        self,
        session: Session,
        job: WorkJob,
        *,
        owner: str,
        expires_at: datetime,
        now: datetime,
        limits: dict[str, int],
    ) -> bool:
        mutating_kinds = {
            JobKind.CHAPTER_DOWNLOAD.value,
            JobKind.LIBRARY_REPAIR.value,
        }
        if job.kind in mutating_kinds and job.series_key:
            series_lock = f"library-series:{job.series_key}"
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:series_lock))"),
                    {"series_lock": series_lock},
                )
            leased_for_series = session.scalar(
                select(WorkJob.id)
                .where(WorkJob.kind.in_(mutating_kinds))
                .where(WorkJob.status == JobState.LEASED.value)
                .where(WorkJob.series_key == job.series_key)
                .where(WorkJob.lease_expires_at > now)
                .limit(1)
            )
            if leased_for_series is not None:
                return False
        pools = [job.pool]
        if job.kind == JobKind.CHAPTER_DOWNLOAD.value:
            pools.append("chapter_global")
        pools = sorted(set(filter(None, pools)))
        for pool in pools:
            limit = limits.get(pool)
            if pool.startswith("download:") and job.source:
                policy = session.get(ProviderPolicy, job.source)
                if policy is not None and (
                    policy.expires_at is None or aware_datetime(policy.expires_at) > now
                ):
                    limit = policy.learned_job_limit
                    metadata = dict(policy.metadata_json or {})
                    until = metadata.get("exploration_until")
                    if until:
                        try:
                            if datetime.fromisoformat(str(until)) > now:
                                limit = max(limit, int(metadata.get("exploration_tier") or limit))
                        except ValueError:
                            pass
            if limit is None:
                continue
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:permit_pool))"),
                    {"permit_pool": pool},
                )
            active = session.scalar(
                select(func.count())
                .select_from(JobPermit)
                .where(JobPermit.pool == pool)
                .where(JobPermit.lease_expires_at > now)
            )
            if int(active or 0) >= limit:
                return False
        for pool in pools:
            if pool in limits:
                session.add(
                    JobPermit(
                        job_id=job.id,
                        pool=pool,
                        owner=owner,
                        lease_expires_at=expires_at,
                    )
                )
        session.flush()
        return True

    def _release_permits(self, session: Session, job_id: int) -> None:
        session.execute(delete(JobPermit).where(JobPermit.job_id == job_id))
        session.execute(delete(StorageReservation).where(StorageReservation.job_id == job_id))

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


def default_pool(kind: JobKind, source: str = "") -> str:
    if kind in {JobKind.SOURCE_PULL, JobKind.SOURCE_REFRESH}:
        return f"pull:{source}" if source else "pull:unknown"
    if kind is JobKind.CHAPTER_DOWNLOAD:
        return f"download:{source}" if source else "download:unknown"
    if kind is JobKind.KAVITA_SYNC:
        return "kavita"
    if kind is JobKind.COVER_BACKFILL:
        return "cover_backfill"
    if kind is JobKind.NOTIFICATION:
        return "notification"
    return "maintenance"


def aware_datetime(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
