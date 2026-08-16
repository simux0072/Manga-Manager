from __future__ import annotations

import json
from io import StringIO
from pathlib import Path

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from manga_manager.application.portable_state import (
    PortableState,
    apply_portable_import,
    export_portable_state,
    load_portable_state,
    plan_portable_import,
    write_portable_state,
)
from manga_manager.cli import portable_import_progress
from manga_manager.infrastructure.db_models import (
    ArtifactBlob,
    CatalogAlternateSourceListing,
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogExternalIdentifier,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    CatalogSourceState,
    ChapterArtifact,
    JobBase,
    SeriesDownloadPlan,
    WorkJob,
)


def database_factory(*, autoflush: bool = True):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    JobBase.metadata.create_all(engine)
    return sessionmaker(engine, autoflush=autoflush, expire_on_commit=False)


def seed_portable_source(sessions) -> None:
    with sessions() as session, session.begin():
        merged = CatalogSeries(
            title="Merged Story",
            normalized_title="merged story",
            description="Useful metadata",
            status="reading",
            cover_url="https://signed-cover.test/secret-image.jpg",
        )
        separate_left = CatalogSeries(
            title="Similar Left", normalized_title="similar left", status="untracked"
        )
        separate_right = CatalogSeries(
            title="Similar Right", normalized_title="similar right", status="untracked"
        )
        downloaded = CatalogSeries(
            title="Downloaded Cache", normalized_title="downloaded cache", status="untracked"
        )
        session.add_all([merged, separate_left, separate_right, downloaded])
        session.flush()
        asura = CatalogSourceSeries(
            series_id=merged.id,
            source="asura",
            source_id="comics/merged-story-deadbeef",
            normalized_source_id="comics/merged-story",
            revision_override="deadbeef",
            title="Merged Story",
            normalized_title="merged story",
            url="https://asurascans.com/comics/merged-story-deadbeef",
            description="Provider description",
            cover_url="https://signed-cover.test/provider-secret.jpg",
            metadata_json={"token": "must-not-export", "latest_chapter": "10"},
        )
        mangafire = CatalogSourceSeries(
            series_id=merged.id,
            source="mangafire",
            source_id="merged-story.x1",
            normalized_source_id="merged-story.x1",
            title="Merged Story Other Name",
            normalized_title="merged story other name",
            url="https://mangafire.to/manga/merged-story.x1",
        )
        left = CatalogSourceSeries(
            series_id=separate_left.id,
            source="mangadex",
            source_id="left-uuid",
            normalized_source_id="left-uuid",
            title="Similar Left",
            normalized_title="similar left",
            url="https://mangadex.org/title/left-uuid",
        )
        right = CatalogSourceSeries(
            series_id=separate_right.id,
            source="kingofshojo",
            source_id="similar-right",
            normalized_source_id="similar-right",
            title="Similar Right",
            normalized_title="similar right",
            url="https://kingofshojo.com/manga/similar-right",
        )
        cache = CatalogSourceSeries(
            series_id=downloaded.id,
            source="mangafire",
            source_id="downloaded-cache.x2",
            normalized_source_id="downloaded-cache.x2",
            title="Downloaded Cache",
            normalized_title="downloaded cache",
            url="https://mangafire.to/manga/downloaded-cache.x2",
        )
        session.add_all([asura, mangafire, left, right, cache])
        session.flush()
        session.add(
            CatalogSeriesAlias(
                series_id=merged.id,
                source_series_id=asura.id,
                display_value="Merged Alias",
                normalized_value="merged alias",
            )
        )
        session.add(
            CatalogExternalIdentifier(
                series_id=merged.id,
                source_series_id=asura.id,
                provider="anilist",
                value="1234",
            )
        )
        session.add(
            CatalogAlternateSourceListing(
                primary_source_series_id=asura.id,
                source="asura",
                source_id="comics/old-merged-story",
                title="Old Merged Story",
                url="https://asurascans.com/comics/old-merged-story",
            )
        )
        chapter = CatalogChapter(
            series_id=merged.id,
            canonical_number="8",
            display_number="8",
            sort_number=8,
        )
        cached_chapter = CatalogChapter(
            series_id=downloaded.id,
            canonical_number="1",
            display_number="1",
            sort_number=1,
        )
        session.add_all([chapter, cached_chapter])
        session.flush()
        session.add(CatalogChapterReadingState(chapter_id=chapter.id, status="read"))
        blob = ArtifactBlob(checksum="a" * 64, relative_path="objects/a.cbz", byte_count=12)
        session.add(blob)
        session.flush()
        session.add(
            ChapterArtifact(
                chapter_id=cached_chapter.id,
                blob_checksum=blob.checksum,
                state="active",
            )
        )
        session.add(
            CatalogMatchDecision(
                left_source_series_id=left.id,
                right_source_series_id=right.id,
                decision="rejected",
                decided_by="operator",
            )
        )
        session.add(CatalogSourceState(source="mangafire", manual_enabled=False))


def test_portable_export_omits_media_runtime_and_secrets(tmp_path: Path) -> None:
    sessions = database_factory()
    seed_portable_source(sessions)
    with sessions() as session:
        state = export_portable_state(session)

    assert len(state.series) == 4
    merged = next(row for row in state.series if row.title == "Merged Story")
    assert {source.source for source in merged.sources} == {"asura", "mangafire"}
    assert merged.aliases == []
    asura = next(source for source in merged.sources if source.source == "asura")
    assert asura.aliases == ["Merged Alias"]
    assert merged.reading[0].chapter == "8"
    assert merged.reading[0].status == "read"
    downloaded = next(row for row in state.series if row.title == "Downloaded Cache")
    assert downloaded.downloaded_on_export is True
    assert downloaded.status == "interested"
    assert len(state.separations) == 1
    assert state.providers[0].enabled is False

    output = tmp_path / "state.json"
    write_portable_state(state, output)
    rendered = output.read_text()
    assert "signed-cover" not in rendered
    assert "must-not-export" not in rendered
    assert "artifact" not in rendered.lower()
    assert "kavita" not in rendered.lower()
    assert "Merged Alias" in rendered
    assert output.stat().st_mode & 0o777 == 0o600
    assert load_portable_state(output).series == state.series


def test_portable_import_is_idempotent_and_rebuilds_download_intent() -> None:
    source = database_factory()
    seed_portable_source(source)
    with source() as session:
        state = export_portable_state(session)

    target = database_factory()
    with target() as session:
        preview = plan_portable_import(session, state)
        assert preview.applied is False
        assert preview.create_series == 4
        assert preview.create_sources == 5
        assert preview.conflicts == []

    with target() as session, session.begin():
        report = apply_portable_import(session, state)
    assert report.applied is True
    assert report.refresh_jobs == 5
    assert report.download_plans == 2

    with target() as session:
        assert session.query(CatalogSeries).count() == 4
        assert session.query(CatalogSourceSeries).count() == 5
        assert session.query(ChapterArtifact).count() == 0
        asura = session.scalar(
            select(CatalogSourceSeries).where(CatalogSourceSeries.source == "asura")
        )
        mangafire = session.scalar(
            select(CatalogSourceSeries).where(
                CatalogSourceSeries.source_id == "merged-story.x1"
            )
        )
        assert asura is not None and mangafire is not None
        assert asura.series_id == mangafire.series_id
        assert asura.metadata_json == {}
        reading = session.scalar(select(CatalogChapterReadingState))
        assert reading is not None and reading.status == "read"
        separation = session.scalar(
            select(CatalogMatchDecision).where(CatalogMatchDecision.decision == "rejected")
        )
        assert separation is not None and separation.decided_by == "portable_import"
        assert session.query(SeriesDownloadPlan).count() == 2
        assert session.query(WorkJob).filter_by(kind="source_refresh").count() == 5
        assert session.get(CatalogSourceState, "mangafire").manual_enabled is False

    with target() as session, session.begin():
        repeated = apply_portable_import(session, state)
    assert repeated.create_series == 0
    assert repeated.create_sources == 0
    assert repeated.refresh_jobs == 0
    with target() as session:
        assert session.query(CatalogSeries).count() == 4
        assert session.query(CatalogSourceSeries).count() == 5
        assert session.query(CatalogMatchDecision).count() == 1
        assert session.query(WorkJob).filter_by(kind="source_refresh").count() == 5


def test_portable_import_allows_separated_series_with_the_same_title() -> None:
    source = database_factory()
    seed_portable_source(source)
    with source() as session:
        state = export_portable_state(session)

    left = next(
        series
        for series in state.series
        if any(identity.source_id == "left-uuid" for identity in series.sources)
    )
    right = next(
        series
        for series in state.series
        if any(identity.source_id == "similar-right" for identity in series.sources)
    )
    right.title = left.title

    target = database_factory()
    with target() as session:
        preview = plan_portable_import(session, state)

    assert preview.conflicts == []


def test_portable_import_deduplicates_pending_aliases_without_autoflush() -> None:
    source = database_factory()
    seed_portable_source(source)
    with source() as session:
        state = export_portable_state(session)

    merged = next(row for row in state.series if row.title == "Merged Story")
    merged.aliases.append("Merged Alias")

    target = database_factory(autoflush=False)
    with target() as session, session.begin():
        apply_portable_import(session, state)

    with target() as session:
        aliases = session.scalars(
            select(CatalogSeriesAlias).where(
                CatalogSeriesAlias.normalized_value == "merged alias"
            )
        ).all()
    assert len(aliases) == 1


def test_portable_import_reports_phases_and_noninteractive_progress() -> None:
    source = database_factory()
    seed_portable_source(source)
    with source() as session:
        state = export_portable_state(session)

    output = StringIO()
    progress = portable_import_progress(output)
    target = database_factory(autoflush=False)
    with target() as session, session.begin():
        apply_portable_import(session, state, progress=progress)

    rendered = output.getvalue()
    assert "phase=planning progress=0/1 percent=0" in rendered
    assert f"phase=series progress={len(state.series)}/{len(state.series)} percent=100" in rendered
    assert "phase=separations" in rendered
    assert "phase=providers" in rendered
    assert "phase=finalizing progress=1/1 percent=100" in rendered


def test_portable_import_merges_existing_provider_records() -> None:
    source = database_factory()
    seed_portable_source(source)
    with source() as session:
        state = export_portable_state(session)

    target = database_factory()
    with target() as session, session.begin():
        left = CatalogSeries(title="Local Left", normalized_title="local left")
        right = CatalogSeries(title="Local Right", normalized_title="local right")
        session.add_all([left, right])
        session.flush()
        session.add_all(
            [
                CatalogSourceSeries(
                    series_id=left.id,
                    source="asura",
                    source_id="comics/merged-story-deadbeef",
                    normalized_source_id="comics/merged-story",
                    title="Local Left",
                    normalized_title="local left",
                    url="https://asurascans.com/comics/merged-story-deadbeef",
                ),
                CatalogSourceSeries(
                    series_id=right.id,
                    source="mangafire",
                    source_id="merged-story.x1",
                    normalized_source_id="merged-story.x1",
                    title="Local Right",
                    normalized_title="local right",
                    url="https://mangafire.to/manga/merged-story.x1",
                ),
            ]
        )

    with target() as session:
        preview = plan_portable_import(session, state)
        assert preview.merge_series == 1
        assert preview.conflicts == []
    with target() as session, session.begin():
        apply_portable_import(session, state)
    with target() as session:
        identities = session.scalars(
            select(CatalogSourceSeries).where(
                CatalogSourceSeries.source_id.in_(
                    ["comics/merged-story-deadbeef", "merged-story.x1"]
                )
            )
        ).all()
        assert len({identity.series_id for identity in identities}) == 1


def test_portable_schema_rejects_unknown_or_inconsistent_data(tmp_path: Path) -> None:
    payload = {
        "format": "manga-manager-portable-state",
        "version": 1,
        "exported_at": "2026-08-05T00:00:00Z",
        "series": [],
        "separations": [],
        "providers": [],
        "api_key": "must-not-be-accepted",
    }
    source = tmp_path / "bad.json"
    source.write_text(json.dumps(payload))
    with pytest.raises(ValidationError):
        load_portable_state(source)

    with pytest.raises(ValidationError):
        PortableState.model_validate(
            {
                **{key: value for key, value in payload.items() if key != "api_key"},
                "series": [
                    {
                        "title": "Broken",
                        "status": "reading",
                        "sources": [
                            {
                                "source": "asura",
                                "source_id": "one",
                                "title": "One",
                                "url": "https://example.test/one",
                            },
                            {
                                "source": "asura",
                                "source_id": "two",
                                "title": "Two",
                                "url": "https://example.test/two",
                            },
                        ],
                    }
                ],
            }
        )
