from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from manga_manager.infrastructure.db_models import CatalogSourceState, JobBase
from manga_manager.infrastructure.provider_scheduler import ProviderRequestScheduler
from manga_manager.infrastructure.database import create_database_engine, run_migrations


def test_provider_scheduler_reserves_shared_request_times() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    scheduler = ProviderRequestScheduler(sessions)
    now = datetime(2026, 7, 11, tzinfo=timezone.utc)

    assert scheduler.reserve("asura", 2.0, now=now) == 0
    assert scheduler.reserve("asura", 2.0, now=now) == 2.0
    assert scheduler.reserve("mangafire", 2.0, now=now) == 0
    with sessions() as session:
        assert session.get(CatalogSourceState, "asura").next_request_at is not None


def test_provider_scheduler_is_atomic_on_postgresql() -> None:
    database_url = os.getenv("V2_TEST_DATABASE_URL", "")
    if not database_url:
        pytest.skip("V2_TEST_DATABASE_URL is not configured")
    run_migrations(database_url)
    engine = create_database_engine(database_url)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        state = session.get(CatalogSourceState, "scheduler-test")
        if state is None:
            session.add(CatalogSourceState(source="scheduler-test"))
        else:
            state.next_request_at = None
            state.cooldown_until = None
    scheduler = ProviderRequestScheduler(sessions)
    now = datetime(2026, 7, 11, tzinfo=timezone.utc)
    with ThreadPoolExecutor(max_workers=2) as executor:
        delays = sorted(
            executor.map(lambda _: scheduler.reserve("scheduler-test", 1.0, now=now), range(2))
        )
    assert delays == [0.0, 1.0]
