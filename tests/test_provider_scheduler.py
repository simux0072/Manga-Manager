from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import os

import pytest
from sqlalchemy import create_engine, delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    JobBase,
    ProviderEndpointState,
    ProviderPolicy,
)
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
    assert scheduler.reserve("asura", 2.0, traffic_class="cdn", now=now) == 0
    assert scheduler.reserve("asura", 2.0, traffic_class="cdn", now=now) == 2.0
    assert scheduler.reserve("mangafire", 2.0, now=now) == 0
    with sessions() as session:
        assert session.get(CatalogSourceState, "asura").next_request_at is not None


def test_recovery_probe_bypasses_cooldown_but_preserves_pacing() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    scheduler = ProviderRequestScheduler(sessions)
    now = datetime(2026, 8, 2, 12, tzinfo=timezone.utc)
    cooling_until = now + timedelta(hours=1)
    with sessions() as session, session.begin():
        session.add_all(
            [
                CatalogSourceState(source="mangadex", cooldown_until=cooling_until),
                ProviderPolicy(source="mangadex", request_interval_seconds=0.25),
                ProviderEndpointState(
                    source="mangadex",
                    traffic_class="origin",
                    cooldown_until=cooling_until,
                    next_request_at=cooling_until + timedelta(seconds=0.25),
                ),
            ]
        )

    assert scheduler.reserve("mangadex", 0.25, now=now) == 3600
    bypass_delay = scheduler.reserve(
        "mangadex", 0.25, now=now, bypass_cooldown=True
    )
    assert 0 <= bypass_delay <= 0.25
    with sessions() as session:
        endpoint = session.query(ProviderEndpointState).one()
        next_request_at = endpoint.next_request_at.replace(tzinfo=timezone.utc)
        assert next_request_at <= now + timedelta(seconds=0.5)


async def test_cooldown_wait_rechecks_after_early_recovery() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    scheduler = ProviderRequestScheduler(sessions, cooldown_recheck_seconds=0.05)
    with sessions() as session, session.begin():
        session.add(
            CatalogSourceState(
                source="mangadex",
                cooldown_until=datetime.now(timezone.utc) + timedelta(hours=1),
            )
        )

    waiting = asyncio.create_task(scheduler.wait("mangadex", "origin", 0))
    await asyncio.sleep(0.1)
    assert not waiting.done()
    with sessions() as session, session.begin():
        state = session.get(CatalogSourceState, "mangadex")
        state.cooldown_until = None
    await asyncio.wait_for(waiting, timeout=1)
    scheduler.close()


def test_provider_scheduler_is_atomic_on_postgresql() -> None:
    database_url = os.getenv("V2_TEST_DATABASE_URL", "")
    if not database_url:
        pytest.skip("V2_TEST_DATABASE_URL is not configured")
    run_migrations(database_url)
    engine = create_database_engine(database_url)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        session.execute(
            delete(ProviderEndpointState).where(ProviderEndpointState.source == "asura")
        )
        session.execute(delete(ProviderPolicy).where(ProviderPolicy.source == "asura"))
        state = session.get(CatalogSourceState, "asura")
        if state is None:
            session.add(CatalogSourceState(source="asura"))
        else:
            state.next_request_at = None
            state.cooldown_until = None
    scheduler = ProviderRequestScheduler(sessions)
    now = datetime(2026, 7, 11, tzinfo=timezone.utc)
    with ThreadPoolExecutor(max_workers=2) as executor:
        delays = sorted(executor.map(lambda _: scheduler.reserve("asura", 1.0, now=now), range(2)))
    assert delays == [0.0, 1.0]
