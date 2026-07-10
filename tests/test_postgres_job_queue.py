from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from manga_manager.domain.jobs import JobKind, MaintenancePayload
from manga_manager.infrastructure.database import DEFAULT_ALEMBIC_CONFIG, run_migrations
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.scheduler_leadership import (
    try_acquire_scheduler_leadership,
)


DATABASE_URL = os.environ.get("V2_TEST_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="V2_TEST_DATABASE_URL is not configured")
NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def sessions():
    run_migrations(DATABASE_URL)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE job_event, worker_heartbeat, job RESTART IDENTITY CASCADE"))
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
            "0005_kavita_mapping"
        )
