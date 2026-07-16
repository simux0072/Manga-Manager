from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from manga_manager.domain.jobs import JobKind, SourceRefreshPayload
from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    JobBase,
    ProviderPolicy,
    WorkJob,
)
from manga_manager.settings import V2Settings
from manga_manager.worker.scheduler import SourcePollScheduler


NOW = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)


def test_scheduler_enqueues_only_due_enabled_sources() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with Session(engine) as session:
        session.add_all(
            [
                CatalogSourceState(
                    source="asura",
                    last_poll_at=NOW - timedelta(minutes=31),
                ),
                CatalogSourceState(
                    source="mangafire",
                    last_poll_at=NOW - timedelta(minutes=5),
                ),
                CatalogSourceState(
                    source="kingofshojo",
                    manual_enabled=False,
                ),
            ]
        )
        session.commit()

    scheduler = SourcePollScheduler(
        engine=engine,
        session_factory=sessions,
        settings=V2Settings(database_url="postgresql+psycopg://unused"),
    )
    assert scheduler.enqueue_due(now=NOW) == 1
    assert scheduler.enqueue_due(now=NOW) == 0
    with Session(engine) as session:
        jobs = session.scalars(select(WorkJob)).all()
    assert [(job.kind, job.dedupe_key) for job in jobs] == [("source_pull", "source:asura")]


def test_scheduler_respects_source_circuit_breaker() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with Session(engine) as session:
        session.add(
            CatalogSourceState(
                source="asura",
                last_poll_at=NOW - timedelta(days=1),
                health_status="cooldown",
                cooldown_until=NOW + timedelta(minutes=10),
            )
        )
        session.commit()
    scheduler = SourcePollScheduler(
        engine=engine,
        session_factory=sessions,
        settings=V2Settings(
            database_url="postgresql+psycopg://unused",
            enable_mangafire=False,
            enable_kingofshojo=False,
        ),
    )
    assert scheduler.enqueue_due(now=NOW) == 0


def test_provider_recovery_probe_uses_its_provider_pull_pool() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with Session(engine) as session:
        session.add(
            ProviderPolicy(
                source="asura",
                metadata_json={"next_recovery_probe": (NOW - timedelta(seconds=1)).isoformat()},
            )
        )
        session.commit()
    scheduler = SourcePollScheduler(
        engine=engine,
        session_factory=sessions,
        settings=V2Settings(
            database_url="postgresql+psycopg://unused",
            enable_asura=False,
            enable_mangafire=False,
            enable_kingofshojo=False,
        ),
    )

    assert scheduler.enqueue_due(now=NOW) == 1
    with Session(engine) as session:
        job = session.scalar(select(WorkJob))
    assert job is not None
    assert (job.kind, job.source, job.pool) == ("maintenance", "asura", "pull:asura")


def test_scheduler_requeues_refreshes_misclassified_during_cloudflare_outage() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with Session(engine) as session:
        from manga_manager.infrastructure.job_queue import JobQueue

        failed, _created = JobQueue().enqueue(
            session,
            kind=JobKind.SOURCE_REFRESH,
            dedupe_key="refresh:mangafire:example",
            payload=SourceRefreshPayload(
                source="mangafire",
                source_id="example",
                title="Example",
                url="https://mangafire.to/title/example",
            ),
        )
        failed.status = "failed"
        failed.error_code = "source_item_invalid"
        failed.error_message = (
            "Server error '521 <none>' for url 'https://mangafire.to/title/example'"
        )
        session.commit()

    scheduler = SourcePollScheduler(
        engine=engine,
        session_factory=sessions,
        settings=V2Settings(
            database_url="postgresql+psycopg://unused",
            enable_asura=False,
            enable_mangafire=False,
            enable_kingofshojo=False,
        ),
    )

    assert scheduler.enqueue_due(now=NOW) == 1
    assert scheduler.enqueue_due(now=NOW) == 0
    with Session(engine) as session:
        jobs = session.scalars(select(WorkJob).order_by(WorkJob.id)).all()
        assert len(jobs) == 2
        assert jobs[0].error_code == "provider_outage_requeued"
        assert jobs[1].status == "queued"
        assert jobs[1].dedupe_key == jobs[0].dedupe_key
