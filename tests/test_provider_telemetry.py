from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from manga_manager.infrastructure.db_models import JobBase, ProviderPolicy
from manga_manager.infrastructure.catalog_repository import update_poll_cadence
from manga_manager.infrastructure.provider_telemetry import (
    ProviderTelemetry,
    effective_poll_interval,
)


NOW = datetime(2026, 7, 12, tzinfo=timezone.utc)


def telemetry() -> tuple[ProviderTelemetry, sessionmaker[Session]]:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    return ProviderTelemetry(sessions), sessions


def test_asura_requires_two_clean_explorations_before_promoting() -> None:
    service, sessions = telemetry()
    service.ensure_policy("asura", job_limit=1, page_limit=1, cooldown_seconds=900)
    with sessions() as session, session.begin():
        policy = session.get(ProviderPolicy, "asura")
        assert policy is not None
        policy.next_exploration_at = NOW - timedelta(seconds=1)
    first = service.start_due_exploration("asura", ceiling=2, now=NOW)
    assert first is not None
    for _ in range(100):
        service.observer(first)({"source": "asura", "status_code": 200})
    service.finish(first, rate_limited=False, report={})
    with sessions() as session, session.begin():
        policy = session.get(ProviderPolicy, "asura")
        assert policy is not None and policy.learned_job_limit == 1
        policy.next_exploration_at = NOW - timedelta(seconds=1)
    second = service.start_due_exploration("asura", ceiling=2, now=NOW)
    assert second is not None
    for _ in range(100):
        service.observer(second)({"source": "asura", "status_code": 200})
    service.finish(second, rate_limited=False, report={})
    with sessions() as session:
        policy = session.get(ProviderPolicy, "asura")
        assert policy is not None
        assert (policy.learned_job_limit, policy.learned_page_limit) == (2, 2)


def test_rate_limit_abandons_exploration_and_learns_retry_after() -> None:
    service, sessions = telemetry()
    service.ensure_policy("mangafire", job_limit=2, page_limit=4, cooldown_seconds=300)
    run_id = service.begin("mangafire", 3)
    service.observer(run_id)(
        {
            "source": "mangafire",
            "host": "cdn.example",
            "status_code": 429,
            "retry_after_seconds": 1200,
        }
    )
    service.finish(run_id, rate_limited=True, report={})
    with sessions() as session:
        policy = session.get(ProviderPolicy, "mangafire")
        assert policy is not None
        assert policy.learned_job_limit == 2
        assert policy.cooldown_seconds == 1200
        assert policy.next_exploration_at is not None


def test_existing_zero_interval_policy_receives_conservative_pacing() -> None:
    service, sessions = telemetry()
    with sessions() as session, session.begin():
        session.add(ProviderPolicy(source="asura", request_interval_seconds=0))

    service.ensure_policy(
        "asura",
        job_limit=1,
        page_limit=1,
        cooldown_seconds=900,
        request_interval_seconds=2.0,
    )

    with sessions() as session:
        policy = session.get(ProviderPolicy, "asura")
        assert policy is not None and policy.request_interval_seconds == 2.0


def test_poll_cadence_adapts_to_changes_idle_polls_and_errors() -> None:
    policy = ProviderPolicy(
        source="mangafire",
        metadata_json={
            "base_poll_seconds": 3600,
            "adaptive_poll_seconds": 3600,
            "unchanged_poll_streak": 0,
        },
    )

    update_poll_cadence(policy, successful=True, changed=True)
    assert effective_poll_interval(policy, timedelta(hours=1)) == timedelta(minutes=45)

    update_poll_cadence(policy, successful=True, changed=False)
    assert effective_poll_interval(policy, timedelta(hours=1)) > timedelta(minutes=45)

    update_poll_cadence(policy, successful=False, changed=False)
    assert effective_poll_interval(policy, timedelta(hours=1)) >= timedelta(hours=1)
