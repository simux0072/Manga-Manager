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
    SeriesDownloadPlan,
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


def test_bounded_recovery_advances_terminal_priority_plan() -> None:
    session, series_id = populated_session()
    coordinator = DownloadPlanCoordinator(rolling_window=2)
    with session.begin():
        coordinator.track(session, series_id)
    with session.begin():
        for job in session.scalars(select(WorkJob)).all():
            job.status = "failed"
        assert coordinator.reconcile_active(session, limit=1) == 1

    plan = session.get(SeriesDownloadPlan, series_id)
    backfill = session.scalars(
        select(ChapterDownloadIntent).where(ChapterDownloadIntent.tier == "backfill")
    ).all()
    assert plan is not None and plan.phase == "backfill"
    assert sum(row.state == "queued" for row in backfill) == 2


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


def test_mangafire_official_release_upgrades_an_unranked_existing_artifact() -> None:
    session, series_id = populated_session(chapter_count=1)
    chapter = session.scalar(select(CatalogChapter))
    release = session.scalar(select(CatalogChapterRelease))
    assert chapter is not None and release is not None
    release.quality_rank = 100
    session.add(ArtifactBlob(checksum="b" * 64, relative_path="blobs/b.cbz", byte_count=1))
    session.add(
        ChapterArtifact(
            chapter_id=chapter.id,
            chapter_release_id=release.id,
            blob_checksum="b" * 64,
            state="active",
            provenance="download",
            source="mangafire",
            quality_rank=0,
            image_count=10,
        )
    )
    session.commit()

    with session.begin():
        created = DownloadPlanCoordinator().enqueue_preferred_upgrades(session, [series_id])

    assert created == 1
    upgrade = session.scalar(select(WorkJob).where(WorkJob.dedupe_key.like("upgrade:%")))
    assert upgrade is not None and upgrade.source == "mangafire"


def test_mangadex_release_upgrades_an_existing_kingofshojo_artifact() -> None:
    session, series_id = populated_session(chapter_count=1)
    chapter = session.scalar(select(CatalogChapter))
    assert chapter is not None
    session.commit()
    with session.begin():
        mangadex = CatalogSourceSeries(
            series_id=series_id,
            source="mangadex",
            source_id="mangadex-example",
            title="Example",
            normalized_title="example",
            url="https://mangadex.org/title/example",
        )
        session.add(mangadex)
        session.flush()
        session.add(
            CatalogChapterRelease(
                chapter_id=chapter.id,
                source_series_id=mangadex.id,
                source="mangadex",
                source_release_id="mangadex-1",
                url="https://mangadex.org/chapter/one",
                quality_rank=100,
            )
        )
        session.add(
            ArtifactBlob(checksum="c" * 64, relative_path="blobs/c.cbz", byte_count=1)
        )
        session.add(
            ChapterArtifact(
                chapter_id=chapter.id,
                blob_checksum="c" * 64,
                state="active",
                provenance="download",
                source="kingofshojo",
                quality_rank=0,
                image_count=10,
            )
        )

    with session.begin():
        created = DownloadPlanCoordinator().enqueue_preferred_upgrades(session, [series_id])

    assert created == 1
    upgrade = session.scalar(select(WorkJob).where(WorkJob.dedupe_key.like("upgrade:%")))
    assert upgrade is not None and upgrade.source == "mangadex"


def test_fallback_prefers_mangafire_over_kingofshojo() -> None:
    session, series_id = populated_session(chapter_count=1)
    chapter = session.scalar(select(CatalogChapter))
    mangafire = session.scalar(select(CatalogChapterRelease))
    assert chapter is not None and mangafire is not None
    session.commit()
    with session.begin():
        king = CatalogSourceSeries(
            series_id=series_id,
            source="kingofshojo",
            source_id="king-example",
            title="Example",
            normalized_title="example",
            url="https://king.test/example",
        )
        asura = CatalogSourceSeries(
            series_id=series_id,
            source="asura",
            source_id="asura-example",
            title="Example",
            normalized_title="example",
            url="https://asura.test/example",
        )
        session.add_all([king, asura])
        session.flush()
        king_release = CatalogChapterRelease(
            chapter_id=chapter.id,
            source_series_id=king.id,
            source="kingofshojo",
            source_release_id="king-1",
            url="https://king.test/example/1",
        )
        asura_release = CatalogChapterRelease(
            chapter_id=chapter.id,
            source_series_id=asura.id,
            source="asura",
            source_release_id="asura-1",
            url="https://asura.test/example/1",
        )
        session.add_all([king_release, asura_release])
        session.flush()

        selected = DownloadPlanCoordinator().fallback_release(
            session,
            chapter.id,
            asura_release.id,
        )

    assert selected is not None
    assert selected.id == mangafire.id
    assert selected.source == "mangafire"


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
    original_ids = {job.id for job in session.scalars(select(WorkJob)).all()}
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
    assert {job.id for job in active} == original_ids
    attempts = session.scalars(select(ChapterReleaseAttempt)).all()
    assert len(attempts) == 2
    assert all(row.outcome == "fallback_queued" for row in attempts)
