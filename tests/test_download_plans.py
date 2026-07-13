from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from manga_manager.application.download_plans import DownloadPlanCoordinator
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogSeries,
    CatalogSourceSeries,
    ChapterDownloadIntent,
    JobBase,
    WorkJob,
    ArtifactBlob,
    ChapterArtifact,
    ChapterReleaseAttempt,
)


def populated_session(chapter_count: int = 8) -> tuple[Session, int]:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    session = Session(engine, expire_on_commit=False)
    series = CatalogSeries(title="Example", normalized_title="example", status="interested")
    session.add(series)
    session.flush()
    source = CatalogSourceSeries(
        series_id=series.id,
        source="mangafire",
        source_id="example",
        title="Example",
        normalized_title="example",
        url="https://example.test/example",
    )
    session.add(source)
    session.flush()
    for number in range(1, chapter_count + 1):
        chapter = CatalogChapter(
            series_id=series.id,
            canonical_number=str(number),
            display_number=str(number),
            sort_number=Decimal(number),
        )
        session.add(chapter)
        session.flush()
        session.add(
            CatalogChapterRelease(
                chapter_id=chapter.id,
                source_series_id=source.id,
                source="mangafire",
                source_release_id=str(number),
                url=f"https://example.test/example/{number}",
            )
        )
    session.commit()
    return session, series.id


def test_tracking_queues_first_two_and_latest_two_before_backfill() -> None:
    session, series_id = populated_session()
    with session.begin():
        plan = DownloadPlanCoordinator().track(session, series_id)

    intents = session.scalars(
        select(ChapterDownloadIntent).order_by(ChapterDownloadIntent.chapter_id)
    ).all()
    chapters = {row.id: row.canonical_number for row in session.scalars(select(CatalogChapter))}
    queued_numbers = {chapters[row.chapter_id] for row in intents if row.state == "queued"}
    assert plan.phase == "priority"
    assert queued_numbers == {"1", "2", "7", "8"}
    assert all(
        row.state == "blocked"
        for row in intents
        if chapters[row.chapter_id] in {"3", "4", "5", "6"}
    )


def test_priority_terminal_jobs_release_backfill_and_untrack_only_cancels_queued() -> None:
    session, series_id = populated_session()
    coordinator = DownloadPlanCoordinator(rolling_window=2)
    with session.begin():
        coordinator.track(session, series_id)
    with session.begin():
        for job in session.scalars(select(WorkJob)).all():
            job.status = "failed"
        plan = coordinator.reconcile(session, series_id)
    assert plan is not None and plan.phase == "backfill"
    backfill = session.scalars(
        select(ChapterDownloadIntent).where(ChapterDownloadIntent.tier == "backfill")
    ).all()
    assert sum(row.state == "queued" for row in backfill) == 2

    leased = session.scalar(select(WorkJob).where(WorkJob.status == "queued"))
    assert leased is not None
    leased.status = "leased"
    leased.lease_owner = "worker"
    leased.lease_expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    with session.begin_nested():
        coordinator.untrack(session, series_id)
    session.commit()
    assert leased.status == "leased"
    assert not session.scalars(select(WorkJob).where(WorkJob.status == "queued")).all()


def test_fallback_artifact_is_queued_for_preferred_source_upgrade() -> None:
    session, series_id = populated_session(chapter_count=1)
    chapter = session.scalar(select(CatalogChapter))
    assert chapter is not None
    session.commit()
    with session.begin():
        asura = CatalogSourceSeries(
            series_id=series_id,
            source="asura",
            source_id="asura-example",
            title="Example",
            normalized_title="example",
            url="https://asura.test/example",
        )
        session.add(asura)
        session.flush()
        session.add(
            CatalogChapterRelease(
                chapter_id=chapter.id,
                source_series_id=asura.id,
                source="asura",
                source_release_id="asura-1",
                url="https://asura.test/example/1",
            )
        )
        session.add(ArtifactBlob(checksum="a" * 64, relative_path="blobs/a.cbz", byte_count=1))
        session.add(
            ChapterArtifact(
                chapter_id=chapter.id,
                blob_checksum="a" * 64,
                state="active",
                provenance="fallback",
                source="mangafire",
                image_count=10,
            )
        )
    with session.begin():
        created = DownloadPlanCoordinator().enqueue_preferred_upgrades(session, [series_id])
    assert created == 1
    upgrade = session.scalar(select(WorkJob).where(WorkJob.dedupe_key.like("upgrade:%")))
    assert upgrade is not None and upgrade.source == "asura" and upgrade.priority == 250


def test_cooling_provider_reroutes_all_waiting_chapters_to_alternates() -> None:
    session, series_id = populated_session(chapter_count=2)
    chapters = session.scalars(select(CatalogChapter).order_by(CatalogChapter.id)).all()
    session.commit()
    with session.begin():
        asura = CatalogSourceSeries(
            series_id=series_id,
            source="asura",
            source_id="asura-example",
            title="Example",
            normalized_title="example",
            url="https://asura.test/example",
        )
        session.add(asura)
        session.flush()
        for chapter in chapters:
            session.add(
                CatalogChapterRelease(
                    chapter_id=chapter.id,
                    source_series_id=asura.id,
                    source="asura",
                    source_release_id=f"asura-{chapter.canonical_number}",
                    url=f"https://asura.test/example/{chapter.canonical_number}",
                )
            )
        DownloadPlanCoordinator().track(session, series_id)

    assert {job.source for job in session.scalars(select(WorkJob)).all()} == {"asura"}
    session.commit()
    retry_after = datetime.now(timezone.utc) + timedelta(minutes=15)
    with session.begin():
        rerouted = DownloadPlanCoordinator().bypass_cooling_source(
            session, source="asura", retry_after=retry_after
        )

    assert rerouted == 2
    active = session.scalars(
        select(WorkJob).where(WorkJob.status.in_(["queued", "retry_wait"]))
    ).all()
    assert len(active) == 2 and {job.source for job in active} == {"mangafire"}
    attempts = session.scalars(select(ChapterReleaseAttempt)).all()
    assert len(attempts) == 2
    assert all(row.outcome == "fallback_queued" for row in attempts)
