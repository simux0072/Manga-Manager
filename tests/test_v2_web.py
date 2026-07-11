from __future__ import annotations

import httpx
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogChapterReadingState,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSourceSeries,
    JobBase,
    JobEvent,
    WorkJob,
)
from manga_manager.web.app import create_app


def app_with_catalog():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=__import__("sqlalchemy").pool.StaticPool,
    )
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        one = CatalogSeries(
            title="A Very Long Example Manga Title",
            normalized_title="a very long example manga title",
            status="untracked",
        )
        two = CatalogSeries(title="Tracked", normalized_title="tracked", status="reading")
        session.add_all([one, two])
        session.flush()
        first_source = CatalogSourceSeries(
            series_id=one.id,
            source="asura",
            source_id="example",
            title=one.title,
            normalized_title=one.normalized_title,
            url="https://example.test",
        )
        second_source = CatalogSourceSeries(
            series_id=two.id,
            source="mangafire",
            source_id="tracked",
            title=two.title,
            normalized_title=two.normalized_title,
            url="https://example.test/tracked",
        )
        session.add_all([first_source, second_source])
        session.flush()
        chapter = CatalogChapter(
            series_id=two.id, canonical_number="1", display_number="1", title="Start"
        )
        session.add(chapter)
        session.flush()
        session.add(
            CatalogChapterRelease(
                chapter_id=chapter.id,
                source_series_id=second_source.id,
                source="mangafire",
                source_release_id="1",
                title="Chapter 1",
                url="https://example.test/tracked/1",
                published_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
            )
        )
        session.add(
            CatalogMatchDecision(
                left_source_series_id=first_source.id,
                right_source_series_id=second_source.id,
                confidence=0.92,
                evidence_json={"title": "similar"},
            )
        )
        job = WorkJob(kind="maintenance", dedupe_key="web-test", payload={})
        session.add(job)
        session.flush()
        session.add(JobEvent(job_id=job.id, event_type="enqueued", status="queued"))
    return create_app(sessions), sessions


async def test_discovery_search_source_and_fragment() -> None:
    app, _ = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/discovery", params={"q": "long", "source": "asura"})
        fragment = await client.get("/discovery", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "A Very Long Example" in response.text
    assert "<!doctype html>" not in fragment.text


async def test_library_state_progressive_post_and_redirects() -> None:
    app, sessions = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        library = await client.get("/library")
        with sessions() as session:
            series_id = session.query(CatalogSeries).filter_by(title="Tracked").one().id
        response = await client.post(
            f"/series/{series_id}/state", data={"state": "caught_up"}, follow_redirects=False
        )
        redirect = await client.get("/info", follow_redirects=False)
    assert "Tracked" in library.text
    assert response.status_code == 303
    with sessions() as session:
        assert session.get(CatalogSeries, series_id).status == "caught_up"
    assert redirect.headers["location"] == "/operations"


async def test_operations_and_health() -> None:
    app, _ = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/healthz")).json()["architecture"] == "postgresql-v2"
        operations = await client.get("/operations")
        probe = await client.post("/operations/probe", headers={"HX-Request": "true"})
        assert operations.status_code == 200
        assert "missing_projections" in operations.text
        assert "Queued probe" in probe.text


async def test_updates_matches_activity_and_library_modes() -> None:
    app, sessions = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert "Chapter 1" in (await client.get("/updates")).text
        matches = await client.get("/matches")
        assert "92%" in matches.text
        assert "enqueued" in (await client.get("/activity")).text
        assert (await client.get("/library", params={"view": "list"})).status_code == 200
        with sessions() as session:
            decision_id = session.query(CatalogMatchDecision).one().id
        rejected = await client.post(
            f"/matches/{decision_id}/decision",
            data={"decision": "rejected"},
            follow_redirects=False,
        )
        assert rejected.status_code == 303


async def test_merge_decision_requires_explicit_confirmation() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        decision_id = session.query(CatalogMatchDecision).one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            f"/matches/{decision_id}/decision", data={"decision": "accepted"}
        )
        assert response.status_code == 422


async def test_confirmed_match_merges_complete_groups() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        decision_id = session.query(CatalogMatchDecision).one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            f"/matches/{decision_id}/decision",
            data={"decision": "accepted", "confirmation": "MERGE"},
            follow_redirects=False,
        )
    assert response.status_code == 303
    with sessions() as session:
        assert session.query(CatalogSeries).count() == 1
        assert {row.series_id for row in session.query(CatalogSourceSeries).all()} == {
            session.query(CatalogSeries).one().id
        }
        assert session.query(CatalogMatchDecision).one().decision == "accepted"


async def test_chapter_read_state_and_series_state_return_fragments() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        chapter_id = session.query(CatalogChapter).one().id
        series_id = session.query(CatalogSeries).filter_by(title="Tracked").one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        chapter = await client.post(
            f"/chapters/{chapter_id}/state",
            data={"state": "read"},
            headers={"HX-Request": "true"},
        )
        series = await client.post(
            f"/series/{series_id}/state",
            data={"state": "caught_up"},
            headers={"HX-Request": "true"},
        )
    assert 'id="chapter-state-' in chapter.text
    assert ">read<" in chapter.text
    assert 'id="series-state-' in series.text
    with sessions() as session:
        assert session.get(CatalogChapterReadingState, chapter_id).status == "read"


async def test_library_cursor_fragment_preserves_filter_parameters() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        for index in range(55):
            session.add(
                CatalogSeries(
                    title=f"Extra {index}",
                    normalized_title=f"extra {index}",
                    status="reading",
                )
            )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            "/library",
            params={"state": "reading", "q": ""},
            headers={"HX-Request": "true"},
        )
    assert "<!doctype html>" not in response.text
    assert "state=reading" in response.text


async def test_batch_match_rejection_returns_empty_fragment() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        decision_id = session.query(CatalogMatchDecision).one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/matches/batch",
            data={"decision_ids": str(decision_id), "decision": "rejected"},
            headers={"HX-Request": "true"},
        )
    assert response.status_code == 200
    assert response.text == ""
