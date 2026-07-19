from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from manga_manager.application.provider_identity_repair import ProviderIdentityRepair
from manga_manager.infrastructure.db_models import (
    CatalogObservation,
    ArtifactBlob,
    CatalogAlternateSourceListing,
    CatalogChapter,
    CatalogChapterRelease,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSourceSeries,
    CatalogSourceState,
    ChapterArtifact,
    ChapterReleaseAttempt,
    JobBase,
    WorkJob,
)


def test_mangafire_duplicate_repair_creates_one_primary_with_alternate_listing() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        left_series = CatalogSeries(title="Shared", normalized_title="shared")
        right_series = CatalogSeries(title="Shared", normalized_title="shared")
        comparison_series = CatalogSeries(title="Other", normalized_title="other")
        session.add_all([left_series, right_series, comparison_series])
        session.flush()
        left = CatalogSourceSeries(
            series_id=left_series.id,
            source="mangafire",
            source_id="first-id",
            title="Shared",
            normalized_title="shared",
            url="https://mangafire.test/first-id",
        )
        right = CatalogSourceSeries(
            series_id=right_series.id,
            source="mangafire",
            source_id="second-id",
            title="Shared",
            normalized_title="shared",
            url="https://mangafire.test/second-id",
        )
        comparison = CatalogSourceSeries(
            series_id=comparison_series.id,
            source="kingofshojo",
            source_id="comparison",
            title="Other",
            normalized_title="other",
            url="https://kingofshojo.test/comparison",
        )
        session.add_all([left, right, comparison])
        session.flush()
        duplicate_identity_ids = {left.id, right.id}
        for number in range(1, 11):
            for series, identity in ((left_series, left), (right_series, right)):
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
                        source_series_id=identity.id,
                        source="mangafire",
                        source_release_id=str(number),
                        url=f"{identity.url}/{number}",
                    )
                )
        session.add_all(
            [
                CatalogMatchDecision(
                    left_source_series_id=min(left.id, comparison.id),
                    right_source_series_id=max(left.id, comparison.id),
                    confidence=0.8,
                ),
                CatalogMatchDecision(
                    left_source_series_id=min(right.id, comparison.id),
                    right_source_series_id=max(right.id, comparison.id),
                    confidence=0.8,
                ),
            ]
        )

    service = ProviderIdentityRepair()
    with Session(engine) as session:
        records = service.audit(session)
        duplicate = next(row for row in records if row.source == "mangafire")
        assert duplicate.action == "consolidate_provider_duplicates"
        assert set(duplicate.identity_ids) == duplicate_identity_ids
    with Session(engine) as session, session.begin():
        service.apply(session, service.audit(session))
    with Session(engine) as session:
        assert session.query(CatalogSourceSeries).filter_by(source="mangafire").count() == 1
        alternate = session.query(CatalogAlternateSourceListing).one()
        assert alternate.source == "mangafire"
        assert alternate.source_id in {"first-id", "second-id"}
        assert session.query(CatalogMatchDecision).count() == 1
        assert not [row for row in service.audit(session) if row.source == "mangafire"]


def test_asura_revision_pair_repair_is_dry_run_safe_and_idempotent() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        series = CatalogSeries(title="Painter", normalized_title="painter")
        duplicate_series = CatalogSeries(title="Painter", normalized_title="painter")
        session.add_all([series, duplicate_series])
        session.flush()
        session.add_all([
            CatalogSourceSeries(
                series_id=duplicate_series.id, source="asura",
                source_id="comics/painter-a80d257e", title="Painter",
                normalized_title="painter", url="https://asurascans.com/comics/painter-a80d257e",
                cover_url="https://images.example/painter.webp",
            ),
            CatalogSourceSeries(
                series_id=series.id, source="asura",
                source_id="comics/painter-1d35e5bd", title="Painter",
                normalized_title="painter", url="https://asurascans.com/comics/painter-1d35e5bd",
                cover_url="https://images.example/painter.webp",
            ),
            CatalogSourceState(source="asura", cursor_json={"global_revision": "1d35e5bd"}),
        ])
    service = ProviderIdentityRepair()
    with Session(engine) as session:
        records = service.audit(session)
        assert len(records) == 1 and records[0].action == "consolidate"
        assert len(session.scalars(select(CatalogSourceSeries)).all()) == 2
    with Session(engine) as session, session.begin():
        service.apply(session, records)
    with Session(engine) as session:
        rows = session.scalars(select(CatalogSourceSeries)).all()
        assert len(rows) == 1
    assert rows[0].source_id == "comics/painter"
    assert service.audit(session) == []


def test_ambiguous_provider_pair_is_quarantined_only_once() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        one = CatalogSeries(title="One", normalized_title="one")
        two = CatalogSeries(title="Different", normalized_title="different")
        session.add_all([one, two])
        session.flush()
        session.add_all([
            CatalogSourceSeries(
                series_id=one.id, source="asura", source_id="comics/ambiguous-a80d257e",
                title="One", normalized_title="one", url="https://example/old",
            ),
            CatalogSourceSeries(
                series_id=two.id, source="asura", source_id="comics/ambiguous-1d35e5bd",
                title="Different", normalized_title="different", url="https://example/new",
            ),
        ])
    service = ProviderIdentityRepair()
    for _ in range(2):
        with Session(engine) as session, session.begin():
            records = service.audit(session)
            assert records[0].action == "quarantine"
            service.apply(session, records)
    with Session(engine) as session:
        assert session.query(CatalogObservation).count() == 1


def test_provider_repair_preserves_duplicate_release_references() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        old_series = CatalogSeries(title="Painter", normalized_title="painter")
        new_series = CatalogSeries(title="Painter", normalized_title="painter")
        session.add_all([old_series, new_series])
        session.flush()
        old_identity = CatalogSourceSeries(
            series_id=old_series.id, source="asura", source_id="comics/painter-a80d257e",
            title="Painter", normalized_title="painter", url="https://example/old",
            cover_url="https://images.example/painter.webp",
        )
        new_identity = CatalogSourceSeries(
            series_id=new_series.id, source="asura", source_id="comics/painter-1d35e5bd",
            title="Painter", normalized_title="painter", url="https://example/new",
            cover_url="https://images.example/painter.webp",
        )
        session.add_all([old_identity, new_identity])
        session.flush()
        old_chapter = CatalogChapter(
            series_id=old_series.id, canonical_number="1", display_number="1",
            sort_number=Decimal("1"), title="Chapter 1",
        )
        new_chapter = CatalogChapter(
            series_id=new_series.id, canonical_number="1", display_number="1",
            sort_number=Decimal("1"), title="Chapter 1",
        )
        session.add_all([old_chapter, new_chapter])
        session.flush()
        old_release = CatalogChapterRelease(
            chapter_id=old_chapter.id, source_series_id=old_identity.id, source="asura",
            source_release_id="chapter-1", title="Chapter 1", url="https://example/old/1",
        )
        new_release = CatalogChapterRelease(
            chapter_id=new_chapter.id, source_series_id=new_identity.id, source="asura",
            source_release_id="chapter-1", title="Chapter 1", url="https://example/new/1",
        )
        session.add_all([old_release, new_release])
        session.flush()
        session.add(ArtifactBlob(checksum="a" * 64, relative_path="aa/test.cbz", byte_count=1))
        session.add(ChapterArtifact(
            chapter_id=old_chapter.id, chapter_release_id=old_release.id,
            blob_checksum="a" * 64, state="active", source="asura", image_count=1,
        ))
        session.add(ChapterReleaseAttempt(
            chapter_id=old_chapter.id, chapter_release_id=old_release.id,
            source="asura", outcome="failed",
        ))
        session.add(WorkJob(
            kind="chapter_download", dedupe_key="historical:chapter-1",
            payload={"chapter_release_id": old_release.id, "attempted_sources": []},
            pending_payload={"chapter_release_id": old_release.id, "attempted_sources": []},
            status="succeeded", source="asura", pool="download:asura",
        ))
        session.add(CatalogSourceState(source="asura", cursor_json={"global_revision": "1d35e5bd"}))
        duplicate_release_id = old_release.id

    service = ProviderIdentityRepair()
    with Session(engine) as session, session.begin():
        service.apply(session, service.audit(session))

    with Session(engine) as session:
        release = session.scalar(select(CatalogChapterRelease))
        artifact = session.scalar(select(ChapterArtifact))
        attempt = session.scalar(select(ChapterReleaseAttempt))
        job = session.scalar(select(WorkJob).where(WorkJob.dedupe_key == "historical:chapter-1"))
        assert release is not None and release.id != duplicate_release_id
        assert artifact is not None and artifact.chapter_release_id == release.id
        assert attempt is not None and attempt.chapter_release_id == release.id
        assert job is not None and job.payload["chapter_release_id"] == release.id
        assert job.pending_payload["chapter_release_id"] == release.id
