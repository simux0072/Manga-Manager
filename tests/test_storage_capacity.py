from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from manga_manager.domain.jobs import JobKind, MaintenancePayload
from manga_manager.infrastructure.db_models import JobBase, StorageReservation, StorageState
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.storage_capacity import StorageCapacityCoordinator


def test_storage_reservations_are_atomic_and_released_with_job(tmp_path) -> None:
    engine = create_engine("sqlite://")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(timezone.utc)
    with sessions() as session, session.begin():
        job, _ = JobQueue().enqueue(
            session,
            kind=JobKind.MAINTENANCE,
            dedupe_key="capacity-test",
            payload=MaintenancePayload(action="stage_probe"),
        )
        coordinator = StorageCapacityCoordinator(tmp_path, 0)
        assert coordinator.reserve(
            session,
            job_id=job.id,
            owner="worker",
            requested_bytes=1024,
            lease_expires_at=now + timedelta(minutes=5),
        )
        assert coordinator.reserve(
            session,
            job_id=job.id,
            owner="worker",
            requested_bytes=1024,
            lease_expires_at=now + timedelta(minutes=5),
        )
    with sessions() as session:
        assert len(session.scalars(select(StorageReservation)).all()) == 1
        state = session.get(StorageState, 1)
        assert state is not None and not state.paused


def test_storage_reservation_pauses_below_watermark(tmp_path) -> None:
    engine = create_engine("sqlite://")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        job, _ = JobQueue().enqueue(
            session,
            kind=JobKind.MAINTENANCE,
            dedupe_key="capacity-blocked",
            payload=MaintenancePayload(action="stage_probe"),
        )
        assert not StorageCapacityCoordinator(tmp_path, 10**18).reserve(
            session,
            job_id=job.id,
            owner="worker",
            requested_bytes=1,
            lease_expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
    with sessions() as session:
        assert session.get(StorageState, 1).paused
