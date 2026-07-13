from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from manga_manager.application.refresh_queue_reconcile import RefreshQueueReconciler
from manga_manager.infrastructure.db_models import JobBase, WorkJob


def test_refresh_queue_reconcile_preserves_compatible_and_rebuilds_incompatible() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    base = {
        "source": "asura", "source_id": "comics/painter", "title": "Painter",
        "url": "https://asurascans.com/comics/painter", "observation_version": "1",
    }
    with Session(engine) as session, session.begin():
        session.add_all([
            WorkJob(
                kind="source_refresh", dedupe_key="refresh:asura:compatible",
                source="asura", status="queued", payload={**base, "version": 1},
            ),
            WorkJob(
                kind="source_refresh", dedupe_key="refresh:asura:future",
                source="asura", status="queued",
                payload={**base, "source_id": "comics/future", "version": 99},
            ),
        ])
    service = RefreshQueueReconciler()
    with Session(engine) as session:
        records = service.audit(session)
        assert [row.action for row in records] == ["regroup", "rebuild"]
    with Session(engine) as session, session.begin():
        service.apply(session, records)
    with Session(engine) as session:
        active = session.scalars(
            select(WorkJob).where(WorkJob.status.in_(("queued", "leased", "retry_wait")))
        ).all()
        assert len(active) == 2
        assert all(row.payload["version"] == 2 for row in active)
        assert all(row.workflow_key == "pull:asura:legacy" for row in active)


def test_refresh_queue_reconcile_defers_leased_incompatible_payload() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        session.add(WorkJob(
            kind="source_refresh", dedupe_key="refresh:asura:leased", source="asura",
            status="leased", lease_owner="worker",
            lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
            payload={
                "version": 3, "source": "asura", "source_id": "comics/leased",
                "title": "Leased", "url": "https://asurascans.com/comics/leased",
            },
        ))
    with Session(engine) as session, session.begin():
        records = RefreshQueueReconciler().audit(session)
        assert records[0].action == "defer_rebuild"
        RefreshQueueReconciler().apply(session, records)
    with Session(engine) as session:
        row = session.scalar(select(WorkJob))
        assert row is not None and row.pending_payload["version"] == 2


def test_refresh_queue_reconcile_reports_malformed_work_without_deleting_it() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        session.add(WorkJob(
            kind="source_refresh", dedupe_key="refresh:asura:malformed", source="asura",
            status="queued", payload={"version": "future", "source": "asura"},
        ))
    with Session(engine) as session, session.begin():
        service = RefreshQueueReconciler()
        records = service.audit(session)
        assert records[0].action == "blocked"
        service.apply(session, records)
    with Session(engine) as session:
        row = session.scalar(select(WorkJob))
        assert row is not None and row.status == "queued"
