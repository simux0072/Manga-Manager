from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from manga_manager.application.job_retention import JobRetention
from manga_manager.domain.jobs import JobKind
from manga_manager.infrastructure.db_models import JobBase, JobDailyAggregate, WorkJob


def test_retention_rolls_up_old_terminal_jobs_and_preserves_recent_and_active() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    now = datetime(2026, 7, 13, tzinfo=timezone.utc)
    with Session(engine) as session, session.begin():
        session.add_all([
            WorkJob(
                kind=JobKind.MAINTENANCE.value,
                dedupe_key="old-success",
                status="succeeded",
                attempts=2,
                created_at=now - timedelta(days=20, minutes=5),
                updated_at=now - timedelta(days=20),
                completed_at=now - timedelta(days=20),
            ),
            WorkJob(
                kind=JobKind.MAINTENANCE.value,
                dedupe_key="recent-success",
                status="succeeded",
                completed_at=now - timedelta(days=2),
            ),
            WorkJob(
                kind=JobKind.MAINTENANCE.value,
                dedupe_key="active",
                status="queued",
            ),
        ])
    with Session(engine) as session, session.begin():
        assert JobRetention().prune(session, now=now) == 1
    with Session(engine) as session:
        assert {row.dedupe_key for row in session.scalars(select(WorkJob))} == {
            "recent-success", "active"
        }
        aggregate = session.scalar(select(JobDailyAggregate))
        assert aggregate is not None
        assert aggregate.job_count == 1
        assert aggregate.attempt_count == 2
        assert aggregate.duration_seconds == 300
