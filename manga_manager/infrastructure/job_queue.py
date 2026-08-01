from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from collections.abc import Collection
from typing import Any

from sqlalchemy import Select, and_, case, delete, func, or_, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased

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


NETWORK_JOB_KINDS = frozenset(
    {
        JobKind.SOURCE_PULL.value,
        JobKind.SOURCE_REFRESH.value,
        JobKind.CHAPTER_DOWNLOAD.value,
    }
)
MUTATING_JOB_KINDS = frozenset(
    {
        JobKind.CHAPTER_DOWNLOAD.value,
        JobKind.LIBRARY_REPAIR.value,
    }
)


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
            if existing.status != JobState.LEASED.value and existing.pool != routed_pool:
                existing.pool = routed_pool
                existing.updated_at = utcnow()
                self._notify_workers(session, routed_pool)
            if coalesce:
                replacement = validated_payload.model_dump(mode="json")
                current_payload = (
                    existing.pending_payload
                    if existing.status == JobState.LEASED.value and existing.pending_payload
                    else existing.payload
                )
                acquisition_promotion = bool(
                    kind is JobKind.SOURCE_REFRESH
                    and replacement.get("acquisition_critical")
                    and not current_payload.get("acquisition_critical")
                )
                old_logical_units = existing.logical_units
                added_units = 0
                if kind is JobKind.KAVITA_SYNC and replacement.get("series_ids"):
                    incoming = list(dict.fromkeys(replacement["series_ids"]))
                    if existing.status == JobState.LEASED.value:
                        in_flight = set(existing.payload.get("series_ids") or [])
                        pending = list((existing.pending_payload or {}).get("series_ids") or [])
                        merged = list(dict.fromkeys([*pending, *incoming]))
                        merged = [
                            series_id for series_id in merged if series_id not in in_flight
                        ][:100]
                        added_units = len(merged) - len(pending)
                        if added_units <= 0:
                            return existing, False
                        replacement["series_ids"] = merged
                    else:
                        current = list(current_payload.get("series_ids") or [])
                        merged = list(dict.fromkeys([*current, *incoming]))[:100]
                        if merged == current:
                            return existing, False
                        replacement["series_ids"] = merged
                if not self._should_coalesce(kind, current_payload, replacement):
                    return existing, False
                if acquisition_promotion:
                    existing.priority = min(existing.priority, priority)
                    existing.max_attempts = max(existing.max_attempts, max_attempts)
                    if existing.status != JobState.LEASED.value:
                        incoming_available_at = available_at or utcnow()
                        if aware_datetime(existing.available_at) > aware_datetime(
                            incoming_available_at
                        ):
                            existing.available_at = incoming_available_at
                    self._notify_workers(session, routed_pool)
                if existing.status == JobState.LEASED.value:
                    existing.pending_payload = replacement
                    if kind is not JobKind.KAVITA_SYNC:
                        added_units = max(logical_units, 1)
                    existing.logical_units += max(added_units, 0)
                else:
                    existing.payload = replacement
                    existing.error_code = ""
                    existing.error_message = ""
                    existing.updated_at = utcnow()
                    if kind is JobKind.KAVITA_SYNC:
                        existing.logical_units = max(len(replacement.get("series_ids") or []), 1)
                        added_units = max(existing.logical_units - old_logical_units, 0)
                if added_units > 0 and existing.cycle_id is not None:
                    cycle = session.get(WorkloadCycle, existing.cycle_id)
                    if cycle is not None:
                        cycle.total_units += added_units
                        cycle.added_units += added_units
                        cycle.updated_at = utcnow()
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
        self._notify_workers(session, routed_pool)
        cycle.total_units += job.logical_units
        cycle.added_units += job.logical_units
        cycle.updated_at = utcnow()
        return job, True

    @staticmethod
    def _notify_workers(session: Session, pool: str) -> None:
        """Wake PostgreSQL workers after commit; polling remains the fallback."""
        if session.bind is None or session.bind.dialect.name != "postgresql":
            return
        session.execute(
            text("SELECT pg_notify('manga_manager_jobs', :pool)"),
            {"pool": pool[:200]},
        )

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
        self._notify_workers(session, job.pool)
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
        self._notify_workers(session, job.pool)
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
        exclude_pools: Collection[str] | None = None,
        block_network: bool = False,
        block_chapters: bool = False,
    ) -> Select[tuple[WorkJob]]:
        current = now or utcnow()
        leased_for_series = aliased(WorkJob)
        active_for_group = aliased(WorkJob)
        active_group_load = (
            select(func.count())
            .select_from(active_for_group)
            .where(active_for_group.group_key == WorkJob.group_key)
            .where(active_for_group.status == JobState.LEASED.value)
            .where(active_for_group.lease_expires_at > current)
            .correlate(WorkJob)
            .scalar_subquery()
        )
        fairness_load = case(
            (WorkJob.kind == JobKind.CHAPTER_DOWNLOAD.value, active_group_load),
            else_=0,
        )
        scheduling_tier = case(
            (WorkJob.kind == JobKind.CHAPTER_DOWNLOAD.value, 0),
            (
                and_(
                    WorkJob.kind == JobKind.SOURCE_REFRESH.value,
                    WorkJob.payload["acquisition_critical"].as_boolean().is_(True),
                ),
                1,
            ),
            (WorkJob.kind == JobKind.SOURCE_PULL.value, 2),
            (WorkJob.kind == JobKind.SOURCE_REFRESH.value, 3),
            (
                and_(
                    WorkJob.kind == JobKind.MAINTENANCE.value,
                    WorkJob.pool.like("pull:%"),
                ),
                4,
            ),
            else_=5,
        )
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
                    WorkJob.kind.not_in(NETWORK_JOB_KINDS),
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
                    WorkJob.kind.not_in(MUTATING_JOB_KINDS),
                    WorkJob.series_key == "",
                    and_(
                        WorkJob.kind == JobKind.CHAPTER_DOWNLOAD.value,
                        ~select(leased_for_series.id)
                        .where(leased_for_series.id != WorkJob.id)
                        .where(
                            leased_for_series.kind
                            == JobKind.LIBRARY_REPAIR.value
                        )
                        .where(leased_for_series.status == JobState.LEASED.value)
                        .where(leased_for_series.series_key == WorkJob.series_key)
                        .where(leased_for_series.lease_expires_at > current)
                        .exists(),
                    ),
                    and_(
                        WorkJob.kind == JobKind.LIBRARY_REPAIR.value,
                        ~select(leased_for_series.id)
                        .where(leased_for_series.id != WorkJob.id)
                        .where(leased_for_series.kind.in_(MUTATING_JOB_KINDS))
                        .where(leased_for_series.status == JobState.LEASED.value)
                        .where(leased_for_series.series_key == WorkJob.series_key)
                        .where(leased_for_series.lease_expires_at > current)
                        .exists(),
                    ),
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
            .order_by(
                scheduling_tier.asc(),
                fairness_load.asc(),
                WorkJob.priority.asc(),
                WorkJob.available_at.asc(),
                WorkJob.id.asc(),
            )
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
        if exclude_pools:
            query = query.where(WorkJob.pool.not_in(tuple(exclude_pools)))
        if block_network:
            query = query.where(
                and_(
                    WorkJob.kind.not_in(NETWORK_JOB_KINDS),
                    ~WorkJob.pool.like("pull:%"),
                )
            )
        if block_chapters:
            query = query.where(WorkJob.kind != JobKind.CHAPTER_DOWNLOAD.value)
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
        provider_pacing_window_seconds: float = 2.0,
        pools: Collection[str] | None = None,
    ) -> JobLease | None:
        worker = owner.strip()
        if not worker:
            raise ValueError("owner must not be empty")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        current = now or utcnow()
        limits = pool_limits or {}
        job = None
        rejected: set[int] = set()
        permit_counts = self._active_permit_counts(session, now=current)
        saturated_pools = self._saturated_direct_pools(limits, permit_counts)
        network_full = self._permit_pool_full(
            "network_global", limits=limits, counts=permit_counts
        )
        chapter_full = self._permit_pool_full(
            "chapter_global", limits=limits, counts=permit_counts
        )
        for _ in range(50):
            candidate = session.scalar(
                self.claim_query(
                    now=current,
                    kinds=kinds,
                    pools=pools,
                    exclude_ids=rejected,
                    exclude_pools=saturated_pools,
                    block_network=network_full,
                    block_chapters=chapter_full,
                )
            )
            if candidate is None:
                # Exhausted leases are excluded by claim_query. Preserve crash
                # recovery semantics with an indexed read and write only when
                # such a lease actually exists; empty polls no longer issue
                # unconditional permit DELETE statements.
                self.fail_exhausted_leases(session, now=current)
                break
            expected_pool = default_pool(JobKind(candidate.kind), candidate.source)
            if (
                candidate.kind == JobKind.SOURCE_REFRESH.value
                and candidate.pool != expected_pool
            ):
                candidate.pool = expected_pool
                session.flush([candidate])
            if (
                candidate.status == JobState.LEASED.value
                and candidate.lease_expires_at is not None
                and aware_datetime(candidate.lease_expires_at) <= current
            ):
                if candidate.attempts >= candidate.max_attempts:
                    self._fail_expired_job(session, candidate, current)
                    session.flush()
                    rejected.add(candidate.id)
                    continue
                # Recover only the permits attached to the expired candidate.
                # Empty worker polls therefore perform no cleanup writes.
                self._release_permits(session, candidate.id)
                session.flush()
            if self._acquire_permits(
                session,
                candidate,
                owner=worker,
                expires_at=current + lease_for,
                now=current,
                limits=limits,
                provider_pacing_window_seconds=provider_pacing_window_seconds,
            ):
                job = candidate
                break
            rejected.add(candidate.id)
            permit_counts = self._active_permit_counts(session, now=current)
            saturated_pools = self._saturated_direct_pools(limits, permit_counts)
            network_full = self._permit_pool_full(
                "network_global", limits=limits, counts=permit_counts
            )
            chapter_full = self._permit_pool_full(
                "chapter_global", limits=limits, counts=permit_counts
            )
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
            self._fail_expired_job(session, job, current)
        session.flush()
        return len(jobs)

    @staticmethod
    def cleanup_expired_permits(
        session: Session,
        *,
        now: datetime | None = None,
    ) -> int:
        """Remove inert permit rows outside the latency-sensitive claim loop."""
        result = session.execute(
            delete(JobPermit).where(JobPermit.lease_expires_at <= (now or utcnow()))
        )
        return max(int(result.rowcount or 0), 0)

    def _fail_expired_job(self, session: Session, job: WorkJob, current: datetime) -> None:
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
        previous_phase = job.progress_phase
        previous_current = job.progress_current
        previous_total = job.progress_total
        job.progress_phase = str(payload.get("phase") or job.progress_phase or "working")[:50]
        job.progress_current = max(int(payload.get("processed") or payload.get("current") or 0), 0)
        job.progress_total = max(int(payload.get("total") or 0), 0)
        job.progress_unit = str(payload.get("unit") or job.progress_unit or "items")[:30]
        job.progress_bytes = max(int(payload.get("bytes") or job.progress_bytes or 0), 0)
        job.progress_message = message[:4000]
        job.progress_updated_at = current
        job.updated_at = current
        previous_bucket = (
            min(20, previous_current * 20 // previous_total) if previous_total else -1
        )
        current_bucket = (
            min(20, job.progress_current * 20 // job.progress_total)
            if job.progress_total
            else -1
        )
        # JobEvent drives SSE refreshes and is durable history. Emitting one for
        # every downloaded page made both tables and browsers do unnecessary work.
        if (
            previous_phase != job.progress_phase
            or previous_total != job.progress_total
            or previous_bucket != current_bucket
            or (
                job.progress_total
                and job.progress_current >= job.progress_total
                and (not previous_total or previous_current < previous_total)
            )
        ):
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
        if replacement.get("acquisition_critical") and not current.get(
            "acquisition_critical"
        ):
            return True
        old = str(current.get("observation_version") or "").strip()
        new = str(replacement.get("observation_version") or "").strip()
        if old and new:
            try:
                return Decimal(new) > Decimal(old)
            except InvalidOperation:
                if new == old:
                    return False
        return current != replacement

    def _acquire_permits(
        self,
        session: Session,
        job: WorkJob,
        *,
        owner: str,
        expires_at: datetime,
        now: datetime,
        limits: dict[str, int],
        provider_pacing_window_seconds: float,
    ) -> bool:
        if job.kind in MUTATING_JOB_KINDS and job.series_key:
            series_lock = f"library-series:{job.series_key}"
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:series_lock))"),
                    {"series_lock": series_lock},
                )
            conflicting_kinds = (
                {JobKind.LIBRARY_REPAIR.value}
                if job.kind == JobKind.CHAPTER_DOWNLOAD.value
                else MUTATING_JOB_KINDS
            )
            leased_for_series = session.scalar(
                select(WorkJob.id)
                .where(WorkJob.kind.in_(conflicting_kinds))
                .where(WorkJob.status == JobState.LEASED.value)
                .where(WorkJob.series_key == job.series_key)
                .where(WorkJob.lease_expires_at > now)
                .limit(1)
            )
            if leased_for_series is not None:
                return False
        pools = [job.pool]
        if self._is_network_job(job):
            pools.append("network_global")
        if job.kind == JobKind.CHAPTER_DOWNLOAD.value:
            pools.append("chapter_global")
        pools = sorted(set(filter(None, pools)))
        for pool in pools:
            limit = limits.get(pool)
            if pool.startswith(("download:", "refresh:")) and job.source:
                policy = session.get(ProviderPolicy, job.source)
                if policy is not None and (
                    policy.expires_at is None or aware_datetime(policy.expires_at) > now
                ):
                    # A clean provider may expand to the shared network ceiling.
                    # Slow request pacing prevents workers from piling up inside
                    # the adapter, while a recently limited provider temporarily
                    # falls back to its learned safe tier until a clean poll.
                    if policy.request_interval_seconds > 0:
                        pacing_limit = max(
                            1,
                            int(
                                provider_pacing_window_seconds
                                / policy.request_interval_seconds
                            ),
                        )
                        limit = min(limit, pacing_limit) if limit is not None else pacing_limit
                    limited_at = (
                        aware_datetime(policy.last_limited_at)
                        if policy.last_limited_at is not None
                        else None
                    )
                    clean_since = (
                        aware_datetime(policy.clean_since)
                        if policy.clean_since is not None
                        else None
                    )
                    if limited_at is not None and (
                        clean_since is None or clean_since <= limited_at
                    ):
                        learned_limit = max(1, policy.learned_job_limit)
                        limit = (
                            min(limit, learned_limit)
                            if limit is not None
                            else learned_limit
                        )
                    metadata = dict(policy.metadata_json or {})
                    until = metadata.get("exploration_until")
                    if until:
                        try:
                            if datetime.fromisoformat(str(until)) > now:
                                explored = int(metadata.get("exploration_tier") or limit or 1)
                                limit = max(limit or 1, explored)
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

    @staticmethod
    def _is_network_job(job: WorkJob) -> bool:
        return job.kind in NETWORK_JOB_KINDS or job.pool.startswith("pull:")

    @staticmethod
    def _active_permit_counts(session: Session, *, now: datetime) -> dict[str, int]:
        return {
            pool: int(count)
            for pool, count in session.execute(
                select(JobPermit.pool, func.count())
                .where(JobPermit.lease_expires_at > now)
                .group_by(JobPermit.pool)
            ).all()
        }

    @staticmethod
    def _permit_pool_full(
        pool: str,
        *,
        limits: dict[str, int],
        counts: dict[str, int],
    ) -> bool:
        limit = limits.get(pool)
        if limit is None:
            return False
        return counts.get(pool, 0) >= limit

    @staticmethod
    def _saturated_direct_pools(
        limits: dict[str, int], counts: dict[str, int]
    ) -> set[str]:
        return {
            pool
            for pool, limit in limits.items()
            if pool not in {"network_global", "chapter_global"}
            and counts.get(pool, 0) >= limit
        }

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
    if kind is JobKind.SOURCE_PULL:
        return f"pull:{source}" if source else "pull:unknown"
    if kind is JobKind.SOURCE_REFRESH:
        return f"refresh:{source}" if source else "refresh:unknown"
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
