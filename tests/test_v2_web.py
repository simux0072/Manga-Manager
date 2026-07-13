from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from manga_manager.infrastructure.db_models import (
    CatalogAlternateSourceListing,
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogChapterRelease,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSourceSeries,
    JobBase,
    JobEvent,
    WorkerHeartbeat,
    WorkJob,
)
from manga_manager.web.api import operational_error_message
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
            description="A painter explores impossible dungeons",
            cover_url="https://images.test/one.jpg",
            status="untracked",
        )
        two = CatalogSeries(
            title="Tracked",
            normalized_title="tracked",
            cover_url="https://images.test/two.jpg",
            status="reading",
        )
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
                evidence_json={"title_or_alias": True, "cover_match": False},
            )
        )
        job = WorkJob(kind="maintenance", dedupe_key="web-test", payload={})
        session.add(job)
        session.flush()
        session.add(JobEvent(job_id=job.id, event_type="enqueued", status="queued"))
    return create_app(sessions), sessions


async def test_health_and_legacy_bookmarks() -> None:
    app, _ = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        health = await client.get("/healthz")
        redirect = await client.get("/info", follow_redirects=False)
    assert health.json() == {"ok": True, "architecture": "postgresql-v2"}
    assert redirect.headers["location"] == "/operations"


async def test_discovery_searches_description_and_uses_multi_source_or() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        row = session.query(CatalogSeries).filter_by(status="untracked").one()
        session.add(
            CatalogSourceSeries(
                series_id=row.id,
                source="kingofshojo",
                source_id="second-identity",
                title=row.title,
                normalized_title=row.normalized_title,
                url="https://example.test/king",
            )
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get(
            "/api/v2/discovery",
            params=[("q", "dungeons"), ("source", "asura"), ("source", "mangafire")],
        )
    assert [item["title"] for item in response.json()["items"]] == [
        "A Very Long Example Manga Title"
    ]
    assert {source["name"] for source in response.json()["items"][0]["sources"]} == {
        "asura",
        "kingofshojo",
    }


async def test_tracking_moves_series_between_discovery_and_library() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        series_id = session.query(CatalogSeries).filter_by(status="untracked").one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        tracked = await client.patch(f"/api/v2/series/{series_id}", json={"status": "interested"})
        discovery = await client.get("/api/v2/discovery")
        library = await client.get("/api/v2/library")
    assert tracked.json()["previous"] == "untracked"
    assert series_id not in {item["id"] for item in discovery.json()["items"]}
    assert series_id in {item["id"] for item in library.json()["items"]}


async def test_updates_group_unread_tracked_chapters_by_series() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        series = session.query(CatalogSeries).filter_by(title="Tracked").one()
        source = session.query(CatalogSourceSeries).filter_by(series_id=series.id).one()
        second = CatalogChapter(
            series_id=series.id, canonical_number="2", display_number="2", title="Next"
        )
        session.add(second)
        session.flush()
        session.add(
            CatalogChapterRelease(
                chapter_id=second.id,
                source_series_id=source.id,
                source="mangafire",
                source_release_id="2",
                title="Chapter 2",
                url="https://example.test/tracked/2",
                published_at=datetime(2026, 7, 11, tzinfo=timezone.utc),
            )
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/v2/updates")
    assert len(response.json()["items"]) == 1
    assert [chapter["number"] for chapter in response.json()["items"][0]["unread_chapters"]] == [
        "2",
        "1",
    ]


async def test_chapter_and_bulk_read_state() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        chapter = session.query(CatalogChapter).one()
        chapter_id, series_id = chapter.id, chapter.series_id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (
            await client.patch(f"/api/v2/chapters/{chapter_id}", json={"status": "reading"})
        ).status_code == 200
        assert (await client.post(f"/api/v2/series/{series_id}/chapters/read")).status_code == 200
    with sessions() as session:
        assert session.get(CatalogChapterReadingState, chapter_id).status == "read"


async def test_matches_return_human_evidence_and_require_confirmation() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        decision_id = session.query(CatalogMatchDecision).one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        matches = await client.get("/api/v2/matches")
        rejected = await client.post(
            f"/api/v2/matches/{decision_id}", json={"decision": "accepted"}
        )
    labels = {row["label"] for row in matches.json()["items"][0]["evidence"]}
    assert "Strong title or alias match" in labels
    assert "Cover mismatch" not in labels
    assert rejected.status_code == 422
    assert "evidence_json" not in matches.text


async def test_confirmed_match_merges_complete_groups() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        decision_id = session.query(CatalogMatchDecision).one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            f"/api/v2/matches/{decision_id}",
            json={"decision": "accepted", "confirmation": "MERGE"},
        )
    assert response.status_code == 200
    with sessions() as session:
        assert session.query(CatalogSeries).count() == 1
        assert len({row.series_id for row in session.query(CatalogSourceSeries).all()}) == 1


async def test_merge_consolidates_strong_same_provider_duplicate_before_group_merge() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        left = session.query(CatalogSeries).filter_by(title="A Very Long Example Manga Title").one()
        right = session.query(CatalogSeries).filter_by(title="Tracked").one()
        duplicate = CatalogSourceSeries(
            series_id=left.id,
            source="mangafire",
            source_id="alternate-slug",
            title="Tracked alternate",
            normalized_title="tracked alternate",
            url="https://example.test/alternate",
        )
        session.add(duplicate)
        session.flush()
        keeper = (
            session.query(CatalogSourceSeries)
            .filter_by(series_id=right.id, source="mangafire")
            .one()
        )
        for number in ("2", "3"):
            for series, identity, suffix in (
                (left, duplicate, "alternate"),
                (right, keeper, "tracked"),
            ):
                chapter = CatalogChapter(
                    series_id=series.id, canonical_number=number, display_number=number
                )
                session.add(chapter)
                session.flush()
                session.add(
                    CatalogChapterRelease(
                        chapter_id=chapter.id,
                        source_series_id=identity.id,
                        source="mangafire",
                        source_release_id=f"{suffix}-{number}",
                        url=f"https://example.test/{suffix}/{number}",
                    )
                )
        decision_id = session.query(CatalogMatchDecision).one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            f"/api/v2/matches/{decision_id}",
            json={"decision": "accepted", "confirmation": "MERGE"},
        )
    assert response.status_code == 200, response.text
    with sessions() as session:
        assert session.query(CatalogSeries).count() == 1
        assert session.query(CatalogSourceSeries).filter_by(source="mangafire").count() == 1
        alternate = session.query(CatalogAlternateSourceListing).one()
        assert alternate.source_id == "alternate-slug"


async def test_jobs_activity_and_operations_have_human_context() -> None:
    app, _ = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        jobs = await client.get("/api/v2/jobs")
        activity = await client.get("/api/v2/activity")
        operations = await client.get("/api/v2/operations")
        probe = await client.post("/api/v2/probe")
    description = "Run storage and database health probe"
    assert jobs.json()["items"][0]["description"] == description
    assert activity.json()["items"][0]["job"]["description"] == description
    assert operations.json()["health"]["series"] == 2
    assert probe.status_code == 200


async def test_operations_hides_stale_worker_processes() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        session.add_all(
            [
                WorkerHeartbeat(
                    worker_id="current-worker",
                    status="running",
                    heartbeat_at=datetime.now(timezone.utc),
                ),
                WorkerHeartbeat(
                    worker_id="old-process",
                    status="stopped",
                    heartbeat_at=datetime.now(timezone.utc) - timedelta(hours=1),
                ),
            ]
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/v2/operations")
    assert [row["id"] for row in response.json()["workers"]] == ["current-worker"]


def test_blank_legacy_network_error_has_operational_fallback() -> None:
    assert operational_error_message("source_network_error", "") == (
        "Provider network request failed; retry is scheduled."
    )
