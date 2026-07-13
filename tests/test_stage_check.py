from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from manga_manager.cli import stage_active_mutations, stage_check_details
from manga_manager.domain.jobs import JobKind, KavitaSyncPayload, LibraryRepairPayload
from manga_manager.infrastructure.db_models import JobBase
from manga_manager.infrastructure.job_queue import JobQueue


def test_stage_check_detects_jobs_that_can_change_validation_results() -> None:
    engine = create_engine("sqlite://")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        JobQueue().enqueue(
            session,
            kind=JobKind.LIBRARY_REPAIR,
            dedupe_key="repair:1",
            payload=LibraryRepairPayload(series_id=1),
        )
        synced, _ = JobQueue().enqueue(
            session,
            kind=JobKind.KAVITA_SYNC,
            dedupe_key="kavita:1",
            payload=KavitaSyncPayload(series_id=1),
        )
        synced.status = "succeeded"

    with engine.connect() as connection:
        assert stage_active_mutations(connection) == [
            {"kind": "library_repair", "status": "queued", "count": 1}
        ]


def test_stage_check_bounds_failure_details_unless_explicitly_requested() -> None:
    values = [f"archive-{index}" for index in range(100)]

    assert stage_check_details(values, limit=3, full=False) == values[:3]
    assert stage_check_details(values, limit=3, full=True) == values
