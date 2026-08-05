from __future__ import annotations

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from manga_manager.application.match_operations import (
    MatchOperationHandler,
    enqueue_match_operation,
    reconcile_stranded_match_operations,
)
from manga_manager.infrastructure.db_models import (
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSourceSeries,
    JobBase,
    MatchOperation,
    MatchOperationSeries,
    WorkJob,
)


@pytest.fixture
def sessions():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    JobBase.metadata.create_all(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    with factory() as session, session.begin():
        left = CatalogSeries(title="Left", normalized_title="left", status="interested")
        right = CatalogSeries(title="Right", normalized_title="right", status="interested")
        third = CatalogSeries(title="Third", normalized_title="third", status="interested")
        session.add_all([left, right, third])
        session.flush()
        identities = [
            CatalogSourceSeries(
                series_id=series.id,
                source=source,
                source_id=source,
                title=series.title,
                normalized_title=series.normalized_title,
                url=f"https://{source}.test/title",
            )
            for series, source in (
                (left, "asura"),
                (right, "mangafire"),
                (third, "kingofshojo"),
            )
        ]
        session.add_all(identities)
        session.flush()
        decision = CatalogMatchDecision(
            left_source_series_id=identities[0].id,
            right_source_series_id=identities[1].id,
            confidence=0.9,
        )
        session.add(decision)
        session.flush()
        factory.series_ids = [left.id, right.id, third.id]
        factory.decision_id = decision.id
    return factory


def test_enqueue_reserves_series_and_is_idempotent(sessions) -> None:
    with sessions() as session, session.begin():
        operation, created = enqueue_match_operation(
            session,
            action="rejected",
            representative_id=sessions.decision_id,
            decision_ids=[sessions.decision_id],
            proposal_ids=[sessions.decision_id],
            series_ids=sessions.series_ids[:2],
        )
        repeated, repeated_created = enqueue_match_operation(
            session,
            action="rejected",
            representative_id=sessions.decision_id,
            decision_ids=[sessions.decision_id],
            proposal_ids=[sessions.decision_id],
            series_ids=list(reversed(sessions.series_ids[:2])),
        )
        assert created is True and repeated_created is False
        assert repeated.id == operation.id

    with sessions() as session:
        assert session.query(MatchOperationSeries).count() == 2
        job = session.query(WorkJob).filter_by(kind="match_operation").one()
        assert job.pool == "catalog_mutation"
        assert job.max_attempts == 3


def test_overlapping_operation_is_rejected_before_queueing(sessions) -> None:
    with sessions() as session, session.begin():
        enqueue_match_operation(
            session,
            action="rejected",
            representative_id=sessions.decision_id,
            decision_ids=[sessions.decision_id],
            proposal_ids=[sessions.decision_id],
            series_ids=sessions.series_ids[:2],
        )
    with sessions() as session, session.begin(), pytest.raises(HTTPException) as raised:
        enqueue_match_operation(
            session,
            action="accepted",
            representative_id=999,
            decision_ids=[],
            proposal_ids=[],
            series_ids=sessions.series_ids[1:],
        )
    assert raised.value.status_code == 409


def test_handler_completes_split_and_releases_reservations(sessions) -> None:
    with sessions() as session, session.begin():
        operation, _ = enqueue_match_operation(
            session,
            action="rejected",
            representative_id=sessions.decision_id,
            decision_ids=[sessions.decision_id],
            proposal_ids=[sessions.decision_id],
            series_ids=sessions.series_ids[:2],
        )
        operation_id = operation.id

    MatchOperationHandler(session_factory=sessions)._execute(operation_id)

    with sessions() as session:
        operation = session.get(MatchOperation, operation_id)
        decision = session.get(CatalogMatchDecision, sessions.decision_id)
        assert operation is not None and operation.status == "succeeded"
        assert decision is not None and decision.decision == "rejected"
        assert session.scalar(select(MatchOperationSeries.series_id)) is None


def test_handler_failure_is_visible_and_releases_reservations(sessions) -> None:
    with sessions() as session, session.begin():
        operation, _ = enqueue_match_operation(
            session,
            action="rejected",
            representative_id=sessions.decision_id,
            decision_ids=[sessions.decision_id],
            proposal_ids=[sessions.decision_id],
            series_ids=sessions.series_ids[:2],
        )
        operation_id = operation.id
        session.get(CatalogMatchDecision, sessions.decision_id).decision = "accepted"

    with pytest.raises(Exception):
        MatchOperationHandler(session_factory=sessions)._execute(operation_id)

    with sessions() as session:
        operation = session.get(MatchOperation, operation_id)
        assert operation is not None and operation.status == "failed"
        assert "already reviewed" in operation.error_message
        assert session.scalar(select(MatchOperationSeries.series_id)) is None


def test_reconciler_releases_operation_after_terminal_worker_job(sessions) -> None:
    with sessions() as session, session.begin():
        operation, _ = enqueue_match_operation(
            session,
            action="rejected",
            representative_id=sessions.decision_id,
            decision_ids=[sessions.decision_id],
            proposal_ids=[sessions.decision_id],
            series_ids=sessions.series_ids[:2],
        )
        session.get(WorkJob, operation.job_id).status = "failed"
        assert reconcile_stranded_match_operations(session) == 1
        operation_id = operation.id

    with sessions() as session:
        operation = session.get(MatchOperation, operation_id)
        assert operation is not None and operation.status == "failed"
        assert operation.error_code == "match_worker_interrupted"
        assert session.scalar(select(MatchOperationSeries.series_id)) is None
