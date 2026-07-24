from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from manga_manager.domain.jobs import JobKind, SourceRefreshPayload
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogSeries,
    CatalogSourceState,
    CatalogSourceSeries,
    ChapterDownloadIntent,
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
                    source="mangadex",
                    last_poll_at=NOW - timedelta(minutes=5),
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
            enable_mangadex=False,
            enable_mangafire=False,
            enable_kingofshojo=False,
        ),
    )
    assert scheduler.enqueue_due(now=NOW) == 0


def test_scheduler_respects_learned_poll_interval() -> None:
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
                ProviderPolicy(
                    source="asura",
                    metadata_json={
                        "base_poll_seconds": 1800,
                        "adaptive_poll_seconds": 3600,
                    },
                ),
            ]
        )
        session.commit()
    scheduler = SourcePollScheduler(
        engine=engine,
        session_factory=sessions,
        settings=V2Settings(
            database_url="postgresql+psycopg://unused",
            enable_mangadex=False,
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
            enable_mangadex=False,
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
            enable_mangadex=False,
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


def test_scheduler_requeues_terminal_mangafire_token_403() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with Session(engine) as session:
        series = CatalogSeries(
            title="Example",
            normalized_title="example",
            status="interested",
        )
        session.add(series)
        session.flush()
        identity = CatalogSourceSeries(
            series_id=series.id,
            source="mangafire",
            source_id="example",
            title="Example",
            normalized_title="example",
            url="https://mangafire.to/title/example",
        )
        chapter = CatalogChapter(
            series_id=series.id,
            canonical_number="1",
            display_number="1",
        )
        session.add_all([identity, chapter])
        session.flush()
        release = CatalogChapterRelease(
            chapter_id=chapter.id,
            source_series_id=identity.id,
            source="mangafire",
            source_release_id="release",
            url="https://mangafire.to/title/example/chapter/release",
            downloadable_after=NOW + timedelta(hours=1),
        )
        session.add(release)
        session.flush()
        failed = WorkJob(
            kind="chapter_download",
            dedupe_key=f"chapter:{chapter.id}",
            payload={"version": 1, "chapter_release_id": release.id},
            status="failed",
            source="mangafire",
            series_key=str(series.id),
            pool="download:mangafire",
            error_code="http_403",
            error_message="403 Forbidden",
            completed_at=NOW,
        )
        session.add(failed)
        session.flush()
        session.add(
            ChapterDownloadIntent(
                series_id=series.id,
                chapter_id=chapter.id,
                tier="priority",
                state="attention",
                job_id=failed.id,
            )
        )
        session.commit()

    scheduler = SourcePollScheduler(
        engine=engine,
        session_factory=sessions,
        settings=V2Settings(
            database_url="postgresql+psycopg://unused",
            enable_asura=False,
            enable_mangadex=False,
            enable_mangafire=False,
            enable_kingofshojo=False,
        ),
    )

    assert scheduler.enqueue_due(now=NOW) == 1
    with Session(engine) as session:
        jobs = session.scalars(select(WorkJob).order_by(WorkJob.id)).all()
        intent = session.scalar(select(ChapterDownloadIntent))
        release = session.scalar(select(CatalogChapterRelease))
    assert len(jobs) == 2
    assert jobs[0].error_code == "mangafire_token_requeued"
    assert jobs[1].status == "queued"
    assert jobs[1].pool == "download:mangafire"
    assert intent is not None and intent.job_id == jobs[1].id and intent.state == "queued"
    assert release is not None and release.downloadable_after is None
