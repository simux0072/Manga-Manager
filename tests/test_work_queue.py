from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from manga_manager.domain.jobs import (
    ChapterDownloadPayload,
    JobKind,
    JobState,
    KavitaSyncPayload,
    MaintenancePayload,
    NotificationPayload,
    SourcePullPayload,
)
from manga_manager.infrastructure.db_models import JobBase, JobEvent, JobPermit, WorkJob
from manga_manager.infrastructure.job_queue import JobQueue


NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine, expire_on_commit=False) as db_session:
        yield db_session


def test_enqueue_deduplicates_only_active_jobs(session: Session) -> None:
    queue = JobQueue()
    first, created = queue.enqueue(
        session,
        kind=JobKind.CHAPTER_DOWNLOAD,
        dedupe_key="release:42",
        payload=ChapterDownloadPayload(chapter_release_id=42),
        available_at=NOW,
    )
    duplicate, duplicate_created = queue.enqueue(
        session,
        kind=JobKind.CHAPTER_DOWNLOAD,
        dedupe_key="release:42",
        payload=ChapterDownloadPayload(chapter_release_id=42),
        available_at=NOW,
    )
    assert created is True
    assert duplicate_created is False
    assert duplicate.id == first.id

    lease = queue.claim(session, owner="worker-a", lease_for=timedelta(minutes=5), now=NOW)
    assert lease is not None
    assert queue.succeed(session, job_id=lease.id, owner=lease.owner, now=NOW)

    replacement, replacement_created = queue.enqueue(
        session,
        kind=JobKind.CHAPTER_DOWNLOAD,
        dedupe_key="release:42",
        payload=ChapterDownloadPayload(chapter_release_id=42),
        available_at=NOW,
    )
    assert replacement_created is True
    assert replacement.id != first.id


def test_claim_uses_priority_and_skips_future_jobs(session: Session) -> None:
    queue = JobQueue()
    queue.enqueue(
        session,
        kind=JobKind.SOURCE_PULL,
        dedupe_key="source:later",
        payload=SourcePullPayload(source="later"),
        priority=1,
        available_at=NOW + timedelta(hours=1),
    )
    queue.enqueue(
        session,
        kind=JobKind.SOURCE_PULL,
        dedupe_key="source:normal",
        payload=SourcePullPayload(source="normal"),
        priority=100,
        available_at=NOW,
    )
    urgent, _ = queue.enqueue(
        session,
        kind=JobKind.SOURCE_PULL,
        dedupe_key="source:urgent",
        payload=SourcePullPayload(source="urgent"),
        priority=10,
        available_at=NOW,
    )

    lease = queue.claim(session, owner="worker-a", lease_for=timedelta(minutes=5), now=NOW)
    assert lease is not None
    assert lease.id == urgent.id
    assert lease.attempt == 1
    assert lease.payload == SourcePullPayload(source="urgent")


def test_expired_lease_is_recovered_by_another_worker(session: Session) -> None:
    queue = JobQueue()
    queue.enqueue(
        session,
        kind=JobKind.KAVITA_SYNC,
        dedupe_key="series:12",
        payload=KavitaSyncPayload(series_id=12),
        available_at=NOW,
    )
    first = queue.claim(session, owner="worker-a", lease_for=timedelta(seconds=30), now=NOW)
    assert first is not None

    assert (
        queue.claim(
            session,
            owner="worker-b",
            lease_for=timedelta(seconds=30),
            now=NOW + timedelta(seconds=10),
        )
        is None
    )
    recovered = queue.claim(
        session,
        owner="worker-b",
        lease_for=timedelta(seconds=30),
        now=NOW + timedelta(seconds=31),
    )
    assert recovered is not None
    assert recovered.id == first.id
    assert recovered.attempt == 2
    assert recovered.owner == "worker-b"


def test_heartbeat_and_completion_require_the_current_owner(session: Session) -> None:
    queue = JobQueue()
    queue.enqueue(
        session,
        kind=JobKind.MAINTENANCE,
        dedupe_key="reconcile-storage",
        payload=MaintenancePayload(action="reconcile-storage"),
        available_at=NOW,
    )
    lease = queue.claim(session, owner="worker-a", lease_for=timedelta(minutes=1), now=NOW)
    assert lease is not None
    assert not queue.heartbeat(
        session,
        job_id=lease.id,
        owner="worker-b",
        lease_for=timedelta(minutes=1),
        now=NOW + timedelta(seconds=10),
    )
    assert queue.heartbeat(
        session,
        job_id=lease.id,
        owner="worker-a",
        lease_for=timedelta(minutes=2),
        now=NOW + timedelta(seconds=10),
    )
    assert not queue.succeed(session, job_id=lease.id, owner="worker-b", now=NOW)
    assert queue.succeed(session, job_id=lease.id, owner="worker-a", now=NOW)

    job = session.get(WorkJob, lease.id)
    assert job is not None
    assert job.status == JobState.SUCCEEDED.value
    assert job.lease_owner == ""
    assert job.lease_expires_at is None


def test_retry_becomes_terminal_at_max_attempts(session: Session) -> None:
    queue = JobQueue()
    queue.enqueue(
        session,
        kind=JobKind.NOTIFICATION,
        dedupe_key="activity:99",
        payload=NotificationPayload(activity_event_id=99),
        max_attempts=2,
        available_at=NOW,
    )
    first = queue.claim(session, owner="worker-a", lease_for=timedelta(minutes=1), now=NOW)
    assert first is not None
    retry_at = NOW + timedelta(minutes=5)
    assert (
        queue.retry(
            session,
            job_id=first.id,
            owner=first.owner,
            available_at=retry_at,
            error_code="timeout",
            error_message="notification timed out",
            now=NOW,
        )
        is JobState.RETRY_WAIT
    )
    assert (
        queue.claim(
            session,
            owner="worker-a",
            lease_for=timedelta(minutes=1),
            now=retry_at - timedelta(seconds=1),
        )
        is None
    )

    second = queue.claim(
        session,
        owner="worker-a",
        lease_for=timedelta(minutes=1),
        now=retry_at,
    )
    assert second is not None
    assert second.attempt == 2
    assert (
        queue.retry(
            session,
            job_id=second.id,
            owner=second.owner,
            available_at=retry_at + timedelta(minutes=5),
            error_code="timeout",
            error_message="notification timed out again",
            now=retry_at,
        )
        is JobState.FAILED
    )


def test_final_expired_lease_is_failed_instead_of_reclaimed(session: Session) -> None:
    queue = JobQueue()
    job, _ = queue.enqueue(
        session,
        kind=JobKind.SOURCE_PULL,
        dedupe_key="source:asura",
        payload=SourcePullPayload(source="asura"),
        max_attempts=1,
        available_at=NOW,
    )
    assert queue.claim(session, owner="worker-a", lease_for=timedelta(seconds=10), now=NOW)

    assert (
        queue.claim(
            session,
            owner="worker-b",
            lease_for=timedelta(seconds=10),
            now=NOW + timedelta(seconds=11),
        )
        is None
    )
    session.refresh(job)
    assert job.status == JobState.FAILED.value
    assert job.error_code == "lease_expired"


def test_postgresql_claim_query_uses_skip_locked() -> None:
    statement = JobQueue().claim_query(now=NOW)
    sql = str(statement.compile(dialect=postgresql.dialect())).upper()
    assert "FOR UPDATE SKIP LOCKED" in sql


def test_database_constraint_rejects_unknown_status(session: Session) -> None:
    session.add(
        WorkJob(
            kind=JobKind.MAINTENANCE.value,
            dedupe_key="invalid-state",
            status="mystery",
            available_at=NOW,
        )
    )
    with pytest.raises(Exception):
        session.flush()
    session.rollback()
    assert session.scalars(select(WorkJob)).all() == []


def test_stale_owner_cannot_finalize_expired_lease(session: Session) -> None:
    queue = JobQueue()
    job, _ = queue.enqueue(
        session,
        kind=JobKind.SOURCE_PULL,
        dedupe_key="source:mangafire",
        payload=SourcePullPayload(source="mangafire"),
        available_at=NOW,
    )
    lease = queue.claim(session, owner="worker-a", lease_for=timedelta(seconds=10), now=NOW)
    assert lease is not None
    assert not queue.succeed(
        session,
        job_id=job.id,
        owner="worker-a",
        now=NOW + timedelta(seconds=11),
    )
    session.refresh(job)
    assert job.status == JobState.LEASED.value


def test_cancel_and_release_record_events(session: Session) -> None:
    queue = JobQueue()
    first, _ = queue.enqueue(
        session,
        kind=JobKind.MAINTENANCE,
        dedupe_key="maintenance:reconcile",
        payload=MaintenancePayload(action="reconcile-storage"),
        available_at=NOW,
    )
    lease = queue.claim(session, owner="worker-a", lease_for=timedelta(minutes=1), now=NOW)
    assert lease is not None
    assert queue.release(
        session,
        job_id=first.id,
        owner="worker-a",
        reason="shutdown",
        now=NOW,
    )
    session.refresh(first)
    assert first.status == JobState.QUEUED.value
    assert first.attempts == 0
    assert queue.cancel(session, job_id=first.id, reason="operator request", now=NOW)

    events = session.scalars(
        select(JobEvent).where(JobEvent.job_id == first.id).order_by(JobEvent.id)
    ).all()
    assert [event.event_type for event in events] == [
        "enqueued",
        "leased",
        "released",
        "cancelled",
    ]


def test_claim_can_be_limited_to_registered_kinds(session: Session) -> None:
    queue = JobQueue()
    queue.enqueue(
        session,
        kind=JobKind.SOURCE_PULL,
        dedupe_key="source:asura",
        payload=SourcePullPayload(source="asura"),
        available_at=NOW,
    )
    download, _ = queue.enqueue(
        session,
        kind=JobKind.CHAPTER_DOWNLOAD,
        dedupe_key="release:91",
        payload=ChapterDownloadPayload(chapter_release_id=91),
        priority=200,
        available_at=NOW,
    )
    lease = queue.claim(
        session,
        owner="download-worker",
        lease_for=timedelta(minutes=1),
        now=NOW,
        kinds={JobKind.CHAPTER_DOWNLOAD},
    )
    assert lease is not None
    assert lease.id == download.id


def test_provider_and_global_permits_limit_chapter_claims(session: Session) -> None:
    queue = JobQueue()
    for release_id in (1, 2):
        queue.enqueue(
            session,
            kind=JobKind.CHAPTER_DOWNLOAD,
            dedupe_key=f"release:{release_id}",
            payload=ChapterDownloadPayload(chapter_release_id=release_id),
            source="asura",
            series_key=f"series-{release_id}",
            available_at=NOW,
        )
    limits = {"download:asura": 1, "chapter_global": 4}
    first = queue.claim(
        session,
        owner="worker-a",
        lease_for=timedelta(minutes=1),
        now=NOW,
        pool_limits=limits,
    )
    assert first is not None
    assert first.pool == "download:asura"
    assert (
        queue.claim(
            session,
            owner="worker-b",
            lease_for=timedelta(minutes=1),
            now=NOW,
            pool_limits=limits,
        )
        is None
    )
    assert session.scalars(select(JobPermit).where(JobPermit.job_id == first.id)).all()
    assert queue.succeed(session, job_id=first.id, owner="worker-a", now=NOW)
    assert (
        queue.claim(
            session,
            owner="worker-b",
            lease_for=timedelta(minutes=1),
            now=NOW,
            pool_limits=limits,
        )
        is not None
    )


def test_per_series_exclusion_skips_to_other_series(session: Session) -> None:
    queue = JobQueue()
    for release_id, series_key in ((1, "same"), (2, "same"), (3, "other")):
        queue.enqueue(
            session,
            kind=JobKind.CHAPTER_DOWNLOAD,
            dedupe_key=f"release:{release_id}",
            payload=ChapterDownloadPayload(chapter_release_id=release_id),
            source="mangafire",
            series_key=series_key,
            available_at=NOW,
        )
    limits = {"download:mangafire": 2, "chapter_global": 4}
    first = queue.claim(
        session, owner="a", lease_for=timedelta(minutes=1), now=NOW, pool_limits=limits
    )
    second = queue.claim(
        session, owner="b", lease_for=timedelta(minutes=1), now=NOW, pool_limits=limits
    )
    assert first is not None and first.series_key == "same"
    assert second is not None and second.series_key == "other"


def test_expired_permits_are_recovered_with_job_lease(session: Session) -> None:
    queue = JobQueue()
    queue.enqueue(
        session,
        kind=JobKind.CHAPTER_DOWNLOAD,
        dedupe_key="release:1",
        payload=ChapterDownloadPayload(chapter_release_id=1),
        source="kingofshojo",
        series_key="series-1",
        available_at=NOW,
    )
    limits = {"download:kingofshojo": 1, "chapter_global": 4}
    first = queue.claim(
        session, owner="dead", lease_for=timedelta(seconds=10), now=NOW, pool_limits=limits
    )
    assert first is not None
    recovered = queue.claim(
        session,
        owner="replacement",
        lease_for=timedelta(minutes=1),
        now=NOW + timedelta(seconds=11),
        pool_limits=limits,
    )
    assert recovered is not None
    assert recovered.id == first.id
    permits = session.scalars(select(JobPermit).where(JobPermit.job_id == first.id)).all()
    assert {permit.owner for permit in permits} == {"replacement"}
