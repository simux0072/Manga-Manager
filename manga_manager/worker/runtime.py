from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable, Collection, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import ContextManager

from sqlalchemy.orm import Session

from manga_manager.application.job_handlers import (
    DeferredJobError,
    JobContext,
    JobHandler,
    LeaseLostError,
    PermanentJobError,
    ReroutedJobError,
    RetryableJobError,
    exception_message,
)
from manga_manager.domain.jobs import JobKind, JobLease, JobState
from manga_manager.infrastructure.job_queue import JobQueue


logger = logging.getLogger(__name__)
SessionFactory = Callable[[], ContextManager[Session]]
Clock = Callable[[], datetime]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WorkerRunResult(StrEnum):
    IDLE = "idle"
    SUCCEEDED = "succeeded"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"
    LEASE_LOST = "lease_lost"


@dataclass(frozen=True, slots=True)
class WorkerSettings:
    lease_for: timedelta = timedelta(minutes=5)
    heartbeat_interval: timedelta = timedelta(seconds=30)
    poll_interval: timedelta = timedelta(seconds=1)
    retry_base: timedelta = timedelta(seconds=30)
    retry_cap: timedelta = timedelta(hours=1)
    pool_limits: Mapping[str, int] | None = None

    def __post_init__(self) -> None:
        if self.lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        if self.heartbeat_interval <= timedelta(0):
            raise ValueError("heartbeat_interval must be positive")
        if self.heartbeat_interval >= self.lease_for:
            raise ValueError("heartbeat_interval must be shorter than lease_for")
        if self.poll_interval <= timedelta(0):
            raise ValueError("poll_interval must be positive")
        if self.retry_base <= timedelta(0) or self.retry_cap < self.retry_base:
            raise ValueError("retry delays are invalid")


class JobWorker:
    """Executes leased jobs without holding a database session during the handler."""

    def __init__(
        self,
        *,
        owner: str,
        session_factory: SessionFactory,
        handlers: Mapping[JobKind, JobHandler],
        queue: JobQueue | None = None,
        settings: WorkerSettings | None = None,
        clock: Clock = utcnow,
        claim_kinds: Collection[JobKind] | None = None,
        claim_pools: Collection[str] | None = None,
    ) -> None:
        self.owner = owner.strip()
        if not self.owner:
            raise ValueError("owner must not be empty")
        self.session_factory = session_factory
        self.handlers = dict(handlers)
        self.queue = queue or JobQueue()
        self.settings = settings or WorkerSettings()
        self.clock = clock
        self.claim_kinds = frozenset(claim_kinds) if claim_kinds is not None else None
        self.claim_pools = frozenset(claim_pools) if claim_pools is not None else None

    async def run_once(self) -> WorkerRunResult:
        lease = self._claim()
        if lease is None:
            return WorkerRunResult.IDLE

        handler = self.handlers.get(lease.kind)
        if handler is None:
            return self._fail(
                lease,
                code="handler_missing",
                message=f"no handler registered for {lease.kind.value}",
            )

        lease_lost = asyncio.Event()
        context = JobContext(lease=lease, lease_lost=lease_lost)
        heartbeat = asyncio.create_task(self._heartbeat_loop(lease, lease_lost))
        try:
            await handler(context)
        except DeferredJobError as exc:
            now = self.clock()
            with self.session_factory() as session, session.begin():
                deferred = self.queue.defer(
                    session,
                    job_id=lease.id,
                    owner=lease.owner,
                    available_at=now + exc.retry_after,
                    code=exc.code,
                    message=exc.message,
                    now=now,
                )
            return WorkerRunResult.RETRY_SCHEDULED if deferred else WorkerRunResult.LEASE_LOST
        except RetryableJobError as exc:
            return self._retry(
                lease,
                code=exc.code,
                message=exc.message,
                retry_after=exc.retry_after,
            )
        except ReroutedJobError:
            return WorkerRunResult.RETRY_SCHEDULED
        except PermanentJobError as exc:
            return self._fail(lease, code=exc.code, message=exc.message)
        except LeaseLostError:
            return WorkerRunResult.LEASE_LOST
        except asyncio.CancelledError:
            self._release(lease, reason="worker shutting down")
            raise
        except Exception as exc:
            logger.exception("unexpected job failure job_id=%s kind=%s", lease.id, lease.kind)
            return self._retry(
                lease,
                code="unexpected_error",
                message=exception_message(exc),
                retry_after=None,
            )
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

        if lease_lost.is_set():
            return WorkerRunResult.LEASE_LOST

        with self.session_factory() as session, session.begin():
            succeeded = self.queue.succeed(
                session,
                job_id=lease.id,
                owner=lease.owner,
                now=self.clock(),
            )
        return WorkerRunResult.SUCCEEDED if succeeded else WorkerRunResult.LEASE_LOST

    async def run_forever(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            result = await self.run_once()
            if result is not WorkerRunResult.IDLE:
                continue
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=self.settings.poll_interval.total_seconds()
                )
            except TimeoutError:
                continue

    def _claim(self) -> JobLease | None:
        with self.session_factory() as session, session.begin():
            return self.queue.claim(
                session,
                owner=self.owner,
                lease_for=self.settings.lease_for,
                now=self.clock(),
                kinds=self.claim_kinds,
                pool_limits=dict(self.settings.pool_limits or {}),
                pools=self.claim_pools,
            )

    async def _heartbeat_loop(self, lease: JobLease, lease_lost: asyncio.Event) -> None:
        interval = self.settings.heartbeat_interval.total_seconds()
        while True:
            await asyncio.sleep(interval)
            with self.session_factory() as session, session.begin():
                extended = self.queue.heartbeat(
                    session,
                    job_id=lease.id,
                    owner=lease.owner,
                    lease_for=self.settings.lease_for,
                    now=self.clock(),
                )
            if not extended:
                logger.warning("job lease lost during heartbeat job_id=%s", lease.id)
                lease_lost.set()
                return

    def _retry(
        self,
        lease: JobLease,
        *,
        code: str,
        message: str,
        retry_after: timedelta | None,
    ) -> WorkerRunResult:
        now = self.clock()
        delay = retry_after or self._retry_delay(lease.attempt)
        with self.session_factory() as session, session.begin():
            state = self.queue.retry(
                session,
                job_id=lease.id,
                owner=lease.owner,
                available_at=now + delay,
                error_code=code,
                error_message=message,
                now=now,
            )
        if state is None:
            return WorkerRunResult.LEASE_LOST
        if state is JobState.FAILED:
            return WorkerRunResult.FAILED
        return WorkerRunResult.RETRY_SCHEDULED

    def _fail(self, lease: JobLease, *, code: str, message: str) -> WorkerRunResult:
        with self.session_factory() as session, session.begin():
            failed = self.queue.fail(
                session,
                job_id=lease.id,
                owner=lease.owner,
                error_code=code,
                error_message=message,
                now=self.clock(),
            )
        return WorkerRunResult.FAILED if failed else WorkerRunResult.LEASE_LOST

    def _release(self, lease: JobLease, *, reason: str) -> bool:
        with self.session_factory() as session, session.begin():
            return self.queue.release(
                session,
                job_id=lease.id,
                owner=lease.owner,
                reason=reason,
                now=self.clock(),
            )

    def _retry_delay(self, attempt: int) -> timedelta:
        base = self.settings.retry_base.total_seconds() * (2 ** max(attempt - 1, 0))
        capped = min(base, self.settings.retry_cap.total_seconds())
        return timedelta(seconds=capped * random.uniform(0.8, 1.2))
