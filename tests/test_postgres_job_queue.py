from __future__ import annotations

import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from manga_manager.domain.jobs import ChapterDownloadPayload, JobKind, MaintenancePayload
from manga_manager.infrastructure.database import DEFAULT_ALEMBIC_CONFIG, run_migrations
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.scheduler_leadership import (
    try_acquire_scheduler_leadership,
)
from manga_manager.infrastructure.storage_capacity import StorageCapacityCoordinator


DATABASE_URL = os.environ.get("V2_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="V2_TEST_DATABASE_URL is not configured")
NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def sessions():
    run_migrations(DATABASE_URL)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.begin() as connection:
        connection.execute(
            text(
                "TRUNCATE job_event, worker_heartbeat, storage_state, job "
                "RESTART IDENTITY CASCADE"
            )
        )
    yield engine, sessionmaker(engine, expire_on_commit=False)
    engine.dispose()


def enqueue_jobs(factory, count: int) -> None:
    with factory() as session, session.begin():
        for index in range(count):
            JobQueue().enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key=f"maintenance:{index}",
                payload=MaintenancePayload(action=f"task-{index}"),
                available_at=NOW,
            )


def test_concurrent_workers_claim_distinct_jobs(sessions) -> None:
    _engine, factory = sessions
    enqueue_jobs(factory, 2)
    barrier = Barrier(2)

    def claim(owner: str) -> int:
        with factory() as session, session.begin():
            barrier.wait()
            lease = JobQueue().claim(
                session,
                owner=owner,
                lease_for=timedelta(minutes=1),
                now=NOW,
            )
            assert lease is not None
            return lease.id

    with ThreadPoolExecutor(max_workers=2) as executor:
        job_ids = list(executor.map(claim, ["worker-a", "worker-b"]))
    assert len(set(job_ids)) == 2


def test_partial_unique_index_deduplicates_concurrent_enqueue(sessions) -> None:
    _engine, factory = sessions
    barrier = Barrier(2)

    def enqueue(_index: int) -> tuple[int, bool]:
        with factory() as session, session.begin():
            barrier.wait()
            job, created = JobQueue().enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key="maintenance:shared",
                payload=MaintenancePayload(action="shared-task"),
                available_at=NOW,
            )
            return job.id, created

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(enqueue, range(2)))
    assert len({job_id for job_id, _created in results}) == 1
    assert sorted(created for _job_id, created in results) == [False, True]


def test_provider_permit_cap_is_atomic_across_workers(sessions) -> None:
    _engine, factory = sessions
    with factory() as session, session.begin():
        for release_id in (1, 2):
            JobQueue().enqueue(
                session,
                kind=JobKind.CHAPTER_DOWNLOAD,
                dedupe_key=f"release:{release_id}",
                payload=ChapterDownloadPayload(chapter_release_id=release_id),
                source="asura",
                series_key=f"series-{release_id}",
                available_at=NOW,
            )
    barrier = Barrier(2)

    def claim(owner: str) -> int | None:
        with factory() as session, session.begin():
            barrier.wait()
            lease = JobQueue().claim(
                session,
                owner=owner,
                lease_for=timedelta(minutes=1),
                now=NOW,
                pool_limits={"download:asura": 1, "chapter_global": 4},
            )
            return lease.id if lease else None

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, ["worker-a", "worker-b"]))
    assert sum(job_id is not None for job_id in claims) == 1


def test_per_series_cap_is_atomic_across_workers(sessions) -> None:
    _engine, factory = sessions
    with factory() as session, session.begin():
        for release_id in (1, 2):
            JobQueue().enqueue(
                session,
                kind=JobKind.CHAPTER_DOWNLOAD,
                dedupe_key=f"release:{release_id}",
                payload=ChapterDownloadPayload(chapter_release_id=release_id),
                source="mangafire",
                series_key="same-series",
                available_at=NOW,
            )
    barrier = Barrier(2)

    def claim(owner: str) -> int | None:
        with factory() as session, session.begin():
            barrier.wait()
            lease = JobQueue().claim(
                session,
                owner=owner,
                lease_for=timedelta(minutes=1),
                now=NOW,
                pool_limits={"download:mangafire": 2, "chapter_global": 4},
            )
            return lease.id if lease else None

    with ThreadPoolExecutor(max_workers=2) as executor:
        claims = list(executor.map(claim, ["worker-a", "worker-b"]))
    assert sum(job_id is not None for job_id in claims) == 1


def test_storage_reservation_cap_is_atomic_across_workers(sessions, tmp_path: Path) -> None:
    _engine, factory = sessions
    requested = 1024 * 1024 * 1024
    minimum_free = shutil.disk_usage(tmp_path).free - requested - requested // 2
    lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=1)
    with factory() as session, session.begin():
        jobs = [
            JobQueue().enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key=f"storage:{index}",
                payload=MaintenancePayload(action="stage_probe"),
                available_at=NOW,
            )[0].id
            for index in range(2)
        ]
    barrier = Barrier(2)

    def reserve(item: tuple[int, str]) -> bool:
        job_id, owner = item
        with factory() as session, session.begin():
            barrier.wait()
            return StorageCapacityCoordinator(tmp_path, minimum_free).reserve(
                session,
                job_id=job_id,
                owner=owner,
                requested_bytes=requested,
                lease_expires_at=lease_expires_at,
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        reservations = list(executor.map(reserve, zip(jobs, ("worker-a", "worker-b"))))
    assert sorted(reservations) == [False, True]


def test_scheduler_advisory_lock_has_one_leader(sessions) -> None:
    engine, _factory = sessions
    first = try_acquire_scheduler_leadership(engine)
    assert first is not None
    try:
        assert try_acquire_scheduler_leadership(engine) is None
    finally:
        first.release()
    replacement = try_acquire_scheduler_leadership(engine)
    assert replacement is not None
    replacement.release()


def test_v2_migrations_round_trip_on_postgresql(sessions) -> None:
    engine, _factory = sessions
    engine.dispose()
    config = Config(str(DEFAULT_ALEMBIC_CONFIG))
    config.set_main_option("sqlalchemy.url", DATABASE_URL)
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    with Session(create_engine(DATABASE_URL)) as session:
        assert session.scalar(text("SELECT version_num FROM alembic_version")) == (
            "0016_refresh_storage_hardening"
        )
        indexes = set(
            session.scalars(
                text("SELECT indexname FROM pg_indexes WHERE tablename='series_v2'")
            ).all()
        )
        assert {
            "ix_series_v2_title_trgm",
            "ix_series_v2_normalized_title_trgm",
            "ix_series_v2_latest_cursor",
        } <= indexes
