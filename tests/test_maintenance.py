from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from manga_manager.application.maintenance import MaintenanceHandler
from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    JobBase,
    ProviderEndpointState,
    ProviderPolicy,
    WorkJob,
)
from manga_manager.infrastructure.provider_scheduler import recovery_probe_active


class HealthyAdapter:
    def __init__(self) -> None:
        self.probes = 0
        self.closed = False

    async def probe(self) -> None:
        assert recovery_probe_active()
        self.probes += 1

    async def aclose(self) -> None:
        self.closed = True


class LiveContext:
    def ensure_lease(self) -> None:
        return None


async def test_successful_recovery_clears_stale_provider_state() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    with sessions() as session, session.begin():
        session.add_all(
            [
                CatalogSourceState(
                    source="mangadex",
                    health_status="cooldown",
                    consecutive_failures=7,
                    last_error="ConnectError",
                    cooldown_until=now + timedelta(hours=1),
                    next_request_at=now + timedelta(hours=1),
                ),
                ProviderPolicy(
                    source="mangadex",
                    metadata_json={"recovery_probe_successes": 1},
                ),
                ProviderEndpointState(
                    source="mangadex",
                    traffic_class="origin",
                    consecutive_failures=7,
                    last_error="ConnectError",
                    cooldown_until=now + timedelta(hours=1),
                    next_request_at=now + timedelta(hours=1),
                ),
                WorkJob(
                    kind="source_pull",
                    dedupe_key="source:mangadex",
                    payload={"version": 1, "source": "mangadex", "workflow_key": ""},
                    source="mangadex",
                    pool="pull:mangadex",
                    status="retry_wait",
                    available_at=now + timedelta(hours=1),
                    error_code="source_network_error",
                    error_message="ConnectError",
                ),
            ]
        )
    adapter = HealthyAdapter()
    handler = MaintenanceHandler(
        session_factory=sessions,
        adapter_factory=lambda _source: adapter,
    )

    await handler._provider_probe("mangadex", LiveContext())

    assert adapter.probes == 1
    assert adapter.closed is True
    with sessions() as session:
        state = session.get(CatalogSourceState, "mangadex")
        endpoint = session.query(ProviderEndpointState).one()
        policy = session.get(ProviderPolicy, "mangadex")
        deferred = session.query(WorkJob).one()
        assert state.health_status == "healthy"
        assert state.consecutive_failures == 0
        assert state.last_error == ""
        assert state.cooldown_until is None
        assert state.next_request_at is None
        assert endpoint.consecutive_failures == 0
        assert endpoint.last_error == ""
        assert endpoint.cooldown_until is None
        assert endpoint.next_request_at is None
        assert "recovery_probe_successes" not in policy.metadata_json
        assert deferred.status == "retry_wait"
        available_at = deferred.available_at.replace(tzinfo=timezone.utc)
        assert available_at <= datetime.now(timezone.utc)
        assert deferred.error_code == ""
        assert deferred.error_message == ""
