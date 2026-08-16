from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from manga_manager.infrastructure.db_models import JobBase
from manga_manager.infrastructure.worker_registry import WorkerRegistry


NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)


def test_worker_registry_tracks_live_and_stopped_workers() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    registry = WorkerRegistry()
    with Session(engine, expire_on_commit=False) as session:
        registry.register(session, worker_id="worker-a", metadata={"version": "v2"}, now=NOW)
        registry.register(
            session,
            worker_id="worker-b",
            now=NOW - timedelta(minutes=10),
        )
        session.commit()

        live = registry.live_workers(session, seen_within=timedelta(minutes=2), now=NOW)
        assert [worker.worker_id for worker in live] == ["worker-a"]
        assert live[0].metadata_json == {"version": "v2"}

        assert registry.heartbeat(
            session,
            worker_id="worker-a",
            status="stopped",
            now=NOW + timedelta(seconds=5),
        )
        session.commit()
        assert (
            registry.live_workers(
                session,
                seen_within=timedelta(minutes=2),
                now=NOW + timedelta(seconds=5),
            )
            == []
        )
