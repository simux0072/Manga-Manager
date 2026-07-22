from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from manga_manager.application.match_rescore import MatchRescorePlanner
from manga_manager.application.matching_score import SCORER_VERSION
from manga_manager.domain.jobs import JobKind, MaintenancePayload
from manga_manager.infrastructure.db_models import (
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSourceSeries,
    JobBase,
    WorkJob,
)


def test_old_pending_match_evidence_queues_one_bounded_rescore_job() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        left = CatalogSeries(title="Left", normalized_title="left")
        right = CatalogSeries(title="Right", normalized_title="right")
        session.add_all([left, right])
        session.flush()
        left_id = left.id
        left_source = CatalogSourceSeries(
            series_id=left.id,
            source="asura",
            source_id="left",
            title="Left",
            normalized_title="left",
            url="https://asura.test/left",
        )
        right_source = CatalogSourceSeries(
            series_id=right.id,
            source="mangafire",
            source_id="right",
            title="Right",
            normalized_title="right",
            url="https://mangafire.test/right",
        )
        session.add_all([left_source, right_source])
        session.flush()
        session.add(
            CatalogMatchDecision(
                left_source_series_id=left_source.id,
                right_source_series_id=right_source.id,
                confidence=0.5,
                evidence_json={"scorer_version": "cover-primary-v2"},
            )
        )

    with Session(engine) as session, session.begin():
        assert MatchRescorePlanner().enqueue_pending(session) == 1
        assert MatchRescorePlanner().enqueue_pending(session) == 0

    with Session(engine) as session:
        job = session.scalar(select(WorkJob))
        assert job is not None
        assert job.dedupe_key == f"match-rescore:{left_id}:{SCORER_VERSION}"
        assert job.payload["action"] == "rescore_matches"
        assert job.payload["series_id"] == left_id
        assert job.series_key == ""


def test_rescore_backlog_is_bounded_and_does_not_lock_series() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        queue = MatchRescorePlanner().queue
        for number in range(15):
            queue.enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key=f"rescore:{number}",
                payload=MaintenancePayload(action="rescore_matches", series_id=number + 1),
                pool="maintenance",
                series_key=str(number + 1),
            )

    with Session(engine) as session, session.begin():
        assert MatchRescorePlanner().reconcile_active(session, limit=10) == 5

    with Session(engine) as session:
        active = session.scalars(
            select(WorkJob).where(WorkJob.status.in_(("queued", "leased", "retry_wait")))
        ).all()
        assert len(active) == 10
        assert all(job.series_key == "" for job in active)
