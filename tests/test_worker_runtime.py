from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from manga_manager.application.job_handlers import PermanentJobError, RetryableJobError
from manga_manager.domain.jobs import ChapterDownloadPayload, JobKind, JobState
from manga_manager.infrastructure.db_models import JobBase, WorkJob
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.settings import V2Settings
from manga_manager.worker.runtime import JobWorker, WorkerRunResult, WorkerSettings
from manga_manager.worker.service import WorkerService


NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)


class TrackingSessionFactory:
    def __init__(self, factory: sessionmaker[Session]) -> None:
        self.factory = factory
        self.active = 0

    @contextmanager
    def __call__(self) -> Iterator[Session]:
        self.active += 1
        try:
            with self.factory() as session:
                yield session
        finally:
            self.active -= 1


@pytest.fixture
def sessions() -> TrackingSessionFactory:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    JobBase.metadata.create_all(engine)
    return TrackingSessionFactory(sessionmaker(engine, expire_on_commit=False))


def enqueue(sessions: TrackingSessionFactory, *, max_attempts: int = 3) -> WorkJob:
    with sessions() as session, session.begin():
        job, _ = JobQueue().enqueue(
            session,
            kind=JobKind.CHAPTER_DOWNLOAD,
            dedupe_key="release:42",
            payload=ChapterDownloadPayload(chapter_release_id=42),
            max_attempts=max_attempts,
            available_at=NOW,
        )
        return job


def load_job(sessions: TrackingSessionFactory, job_id: int) -> WorkJob:
    with sessions() as session:
        job = session.get(WorkJob, job_id)
        assert job is not None
        session.expunge(job)
        return job


def worker(
    sessions: TrackingSessionFactory,
    handler,
) -> JobWorker:
    return JobWorker(
        owner="worker-a",
        session_factory=sessions,
        handlers={JobKind.CHAPTER_DOWNLOAD: handler},
        settings=WorkerSettings(
            lease_for=timedelta(minutes=5),
            heartbeat_interval=timedelta(minutes=1),
            poll_interval=timedelta(milliseconds=10),
        ),
        clock=lambda: NOW,
    )


@pytest.mark.asyncio
async def test_worker_does_not_hold_session_while_handler_runs(
    sessions: TrackingSessionFactory,
) -> None:
    job = enqueue(sessions)

    async def handler(context) -> None:
        assert sessions.active == 0
        assert context.lease.payload == ChapterDownloadPayload(chapter_release_id=42)

    result = await worker(sessions, handler).run_once()
    assert result is WorkerRunResult.SUCCEEDED
    assert load_job(sessions, job.id).status == JobState.SUCCEEDED.value


@pytest.mark.asyncio
async def test_retryable_failure_schedules_retry(sessions: TrackingSessionFactory) -> None:
    job = enqueue(sessions)

    async def handler(_lease) -> None:
        raise RetryableJobError(
            "rate_limited", "source asked us to wait", retry_after=timedelta(minutes=5)
        )

    result = await worker(sessions, handler).run_once()
    stored = load_job(sessions, job.id)
    assert result is WorkerRunResult.RETRY_SCHEDULED
    assert stored.status == JobState.RETRY_WAIT.value
    assert stored.available_at.replace(tzinfo=timezone.utc) == NOW + timedelta(minutes=5)
    assert stored.error_code == "rate_limited"


@pytest.mark.asyncio
async def test_retryable_failure_at_limit_becomes_failed(
    sessions: TrackingSessionFactory,
) -> None:
    job = enqueue(sessions, max_attempts=1)

    async def handler(_lease) -> None:
        raise RetryableJobError("invalid_content", "archive did not validate")

    result = await worker(sessions, handler).run_once()
    assert result is WorkerRunResult.FAILED
    assert load_job(sessions, job.id).status == JobState.FAILED.value


@pytest.mark.asyncio
async def test_permanent_failure_is_not_retried(sessions: TrackingSessionFactory) -> None:
    job = enqueue(sessions)

    async def handler(_lease) -> None:
        raise PermanentJobError("release_missing", "release was deleted")

    result = await worker(sessions, handler).run_once()
    stored = load_job(sessions, job.id)
    assert result is WorkerRunResult.FAILED
    assert stored.status == JobState.FAILED.value
    assert stored.attempts == 1
    assert stored.error_code == "release_missing"


@pytest.mark.asyncio
async def test_missing_handler_fails_claimed_job(sessions: TrackingSessionFactory) -> None:
    job = enqueue(sessions)
    runtime = JobWorker(
        owner="worker-a",
        session_factory=sessions,
        handlers={},
        settings=WorkerSettings(
            lease_for=timedelta(minutes=5),
            heartbeat_interval=timedelta(minutes=1),
        ),
        clock=lambda: NOW,
    )

    assert await runtime.run_once() is WorkerRunResult.FAILED
    assert load_job(sessions, job.id).error_code == "handler_missing"


@pytest.mark.asyncio
async def test_idle_worker_returns_without_handler_call(sessions: TrackingSessionFactory) -> None:
    called = False

    async def handler(_lease) -> None:
        nonlocal called
        called = True

    assert await worker(sessions, handler).run_once() is WorkerRunResult.IDLE
    assert called is False


def test_source_pull_workers_are_partitioned_by_provider(
    sessions: TrackingSessionFactory,
) -> None:
    async def handler(_context) -> None:
        return None

    service = WorkerService(
        session_factory=sessions,
        handlers={JobKind.SOURCE_PULL: handler},
        settings=V2Settings(),
    )
    pools = [pool for _slot, pool, _kinds in service._pool_specs()]
    assert pools == [
        "pull:asura",
        "pull:mangadex",
        "pull:mangafire",
        "pull:kingofshojo",
    ]


def test_maintenance_worker_claims_repairs_and_catalog_rescores(
    sessions: TrackingSessionFactory,
) -> None:
    async def handler(_context) -> None:
        return None

    service = WorkerService(
        session_factory=sessions,
        handlers={JobKind.LIBRARY_REPAIR: handler, JobKind.MAINTENANCE: handler},
        settings=V2Settings(),
        pools={"maintenance"},
    )
    specs = service._pool_specs()
    assert len(specs) == 1
    assert specs[0][1:] == (
        "maintenance",
        {JobKind.LIBRARY_REPAIR, JobKind.MAINTENANCE},
    )
