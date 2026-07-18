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
    MatchTrainingLabel,
    WorkerHeartbeat,
    WorkJob,
    WorkloadCycle,
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


async def test_request_metrics_report_route_latency_and_sql_count() -> None:
    app, _ = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        health = await client.get("/healthz")
        metrics = await client.get("/metrics")

    assert health.headers["X-SQL-Query-Count"] == "1"
    assert "app;dur=" in health.headers["Server-Timing"]
    assert "db;dur=" in health.headers["Server-Timing"]
    assert (
        'manga_manager_http_requests_total{method="GET",route="/healthz",status="200"} 1'
        in metrics.text
    )
    assert (
        'manga_manager_http_sql_queries_total{method="GET",route="/healthz",status="200"} 1'
        in metrics.text
    )


async def test_primary_read_routes_stay_inside_scale_query_budget() -> None:
    app, _ = app_with_catalog()
    paths = (
        "/api/v2/discovery?limit=25",
        "/api/v2/library?limit=30",
        "/api/v2/updates?limit=20",
        "/api/v2/matches?limit=24",
        "/api/v2/job-groups?state=queued",
        "/api/v2/activity?limit=100",
        "/api/v2/operations",
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        responses = {path: await client.get(path) for path in paths}

    failures = {
        path: response.text for path, response in responses.items() if response.status_code != 200
    }
    assert failures == {}
    query_counts = {
        path: int(response.headers["X-SQL-Query-Count"])
        for path, response in responses.items()
    }
    assert max(query_counts.values()) <= 25, query_counts


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


async def test_manual_merge_candidates_are_ranked_and_merge_uses_best_provider() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        asura = session.query(CatalogSeries).filter_by(status="untracked").one()
        tracked = session.query(CatalogSeries).filter_by(status="reading").one()
        asura.status = "interested"
        asura.title = "Tracked"
        asura.normalized_title = "tracked"
        asura_id, tracked_id = asura.id, tracked.id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        candidates = await client.get(
            "/api/v2/merge-candidates", params={"anchor_id": tracked_id}
        )
        preview = await client.post(
            "/api/v2/series/merge-preview", json={"series_ids": [tracked_id, asura_id]}
        )
        merged = await client.post(
            "/api/v2/series/merge",
            json={"series_ids": [tracked_id, asura_id], "confirmation": "MERGE"},
        )
    assert candidates.status_code == 200
    assert candidates.json()["items"][0]["id"] == asura_id
    assert 0.35 <= candidates.json()["items"][0]["similarity"] < 1
    assert candidates.json()["items"][0]["score_breakdown"]["title"] == 1
    assert preview.json()["target_id"] == asura_id
    assert preview.json()["can_merge"] is True
    assert merged.status_code == 200, merged.text
    assert merged.json()["target_id"] == asura_id
    with sessions() as session:
        assert session.query(CatalogSeries).count() == 1
        assert session.get(CatalogSeries, asura_id).status == "reading"
        assert session.query(MatchTrainingLabel).filter_by(origin="manual_merge").count() == 1


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
        newest_number = CatalogChapter(
            series_id=series.id,
            canonical_number="20",
            display_number="20",
            title="Newest numeric chapter",
            sort_number=20,
        )
        session.add(newest_number)
        session.flush()
        session.add(
            CatalogChapterRelease(
                chapter_id=newest_number.id,
                source_series_id=source.id,
                source="mangafire",
                source_release_id="20",
                title="Chapter 20",
                url="https://example.test/tracked/20",
                published_at=datetime(2026, 7, 9, tzinfo=timezone.utc),
            )
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/v2/updates")
    assert len(response.json()["items"]) == 1
    assert [chapter["number"] for chapter in response.json()["items"][0]["unread_chapters"]] == [
        "20",
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


async def test_caught_up_marks_every_chapter_read_and_queues_kavita_write() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session:
        series = session.query(CatalogSeries).filter_by(title="Tracked").one()
        series_id = series.id
        chapter_id = session.query(CatalogChapter).filter_by(series_id=series_id).one().id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.patch(
            f"/api/v2/series/{series_id}", json={"status": "caught_up"}
        )
    assert response.status_code == 200
    assert response.json()["item"]["status"] == "caught_up"
    assert response.json()["item"]["read_count"] == 1
    with sessions() as session:
        reading = session.get(CatalogChapterReadingState, chapter_id)
        assert reading is not None and reading.status == "read"
        job = session.query(WorkJob).filter_by(kind="kavita_sync").one()
        assert job.payload["reading_status"] == "read"
        assert job.priority == 20


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


async def test_matches_omit_obsolete_decisions_without_mutating_during_get() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        identities = session.query(CatalogSourceSeries).order_by(CatalogSourceSeries.id).all()
        identities[1].series_id = identities[0].series_id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/v2/matches")
    assert response.status_code == 200 and response.json()["total"] == 0
    with sessions() as session:
        decision = session.query(CatalogMatchDecision).one()
        assert decision.decision == "pending"
        assert decision.decided_by == "canonicalized"


async def test_matches_collapse_multiple_identity_decisions_per_canonical_pair() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        left, right = session.query(CatalogSeries).order_by(CatalogSeries.id).all()
        left_extra = CatalogSourceSeries(
            series_id=left.id, source="kingofshojo", source_id="left-extra",
            title=left.title, normalized_title=left.normalized_title,
            url="https://example.test/left-extra",
        )
        session.add(left_extra)
        session.flush()
        right_identity = session.query(CatalogSourceSeries).filter_by(series_id=right.id).one()
        session.add(CatalogMatchDecision(
            left_source_series_id=min(left_extra.id, right_identity.id),
            right_source_series_id=max(left_extra.id, right_identity.id),
            confidence=0.81,
        ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/v2/matches")
        rejected = await client.post(
            f"/api/v2/matches/{response.json()['items'][0]['id']}",
            json={"decision": "rejected"},
        )
    assert response.json()["total"] == 1
    assert len(response.json()["items"][0]["decision_ids"]) == 2
    assert rejected.status_code == 200
    with sessions() as session:
        assert {row.decision for row in session.query(CatalogMatchDecision)} == {"rejected"}


async def test_match_cursor_survives_reviewing_the_preceding_proposal() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        first_identity = session.query(CatalogSourceSeries).filter_by(source="asura").one()
        third = CatalogSeries(title="Third", normalized_title="third", status="untracked")
        session.add(third)
        session.flush()
        third_identity = CatalogSourceSeries(
            series_id=third.id,
            source="kingofshojo",
            source_id="third",
            title="Third",
            normalized_title="third",
            url="https://example.test/third",
        )
        session.add(third_identity)
        session.flush()
        session.add(
            CatalogMatchDecision(
                left_source_series_id=min(first_identity.id, third_identity.id),
                right_source_series_id=max(first_identity.id, third_identity.id),
                confidence=0.5,
            )
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        first_page = await client.get("/api/v2/matches?limit=1")
        cursor = first_page.json()["next_cursor"]
        reviewed = await client.post(
            f"/api/v2/matches/{cursor}", json={"decision": "rejected"}
        )
        second_page = await client.get(f"/api/v2/matches?limit=1&cursor={cursor}")
    assert reviewed.status_code == 200
    assert len(second_page.json()["items"]) == 1
    assert second_page.json()["items"][0]["confidence"] == 0.5


async def test_entire_match_queue_preview_keeps_active_job_blockers_visible() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        blocked_series = session.query(CatalogSeries).order_by(CatalogSeries.id).first()
        session.add(WorkJob(
            kind="library_repair", dedupe_key="blocked-match", payload={
                "version": 1, "series_id": blocked_series.id, "reason": "merge",
                "obsolete_storage_keys": [],
            },
            series_key=str(blocked_series.id), status="queued",
        ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        preview = await client.post(
            "/api/v2/match-batch/preview",
            json={"ids": [], "entire_queue": True, "decision": "accepted"},
        )
        result = await client.post(
            "/api/v2/match-batch",
            json={
                "ids": [], "entire_queue": True, "decision": "accepted",
                "confirmation": "MERGE",
            },
        )
    assert preview.json()["selected"] == 1
    assert preview.json()["blocked"] == 1
    assert "active jobs" in preview.json()["items"][0]["blocked_reasons"]
    assert result.json()["ids"] == [] and result.json()["blocked"]


async def test_connected_batch_matches_merge_once() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        second = session.query(CatalogSeries).filter_by(title="Tracked").one()
        third = CatalogSeries(
            title="Third", normalized_title="third", status="interested",
        )
        session.add(third)
        session.flush()
        third_identity = CatalogSourceSeries(
            series_id=third.id, source="kingofshojo", source_id="third",
            title="Third", normalized_title="third", url="https://example.test/third",
        )
        session.add(third_identity)
        session.flush()
        second_identity = session.query(CatalogSourceSeries).filter_by(series_id=second.id).one()
        session.add(CatalogMatchDecision(
            left_source_series_id=min(second_identity.id, third_identity.id),
            right_source_series_id=max(second_identity.id, third_identity.id),
            confidence=0.8,
        ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/v2/match-batch",
            json={
                "ids": [], "entire_queue": True, "decision": "accepted",
                "confirmation": "MERGE",
            },
        )
    assert response.status_code == 200, response.text
    assert len(response.json()["ids"]) == 2
    with sessions() as session:
        assert session.query(CatalogSeries).count() == 1
        assert session.query(WorkJob).filter_by(kind="library_repair").count() == 1


async def test_oversized_connected_match_batch_is_previewed_and_left_pending() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        first_identity = session.query(CatalogSourceSeries).filter_by(source="asura").one()
        second_identity = session.query(CatalogSourceSeries).filter_by(
            source="mangafire"
        ).one()
        third = CatalogSeries(title="Third", normalized_title="third", status="interested")
        fourth = CatalogSeries(title="Fourth", normalized_title="fourth", status="interested")
        session.add_all([third, fourth])
        session.flush()
        third_identity = CatalogSourceSeries(
            series_id=third.id,
            source="kingofshojo",
            source_id="third",
            title=third.title,
            normalized_title=third.normalized_title,
            url="https://kingofshojo.example/third",
        )
        # This is an equivalent historical Asura identity: the rotating revision suffix is
        # intentionally ignored. It therefore has no provider conflict but still makes the
        # connected component larger than the configured three-provider merge limit.
        fourth_identity = CatalogSourceSeries(
            series_id=fourth.id,
            source="asura",
            source_id="example-deadbeef",
            title=fourth.title,
            normalized_title=fourth.normalized_title,
            url="https://asura.example/example-deadbeef",
        )
        session.add_all([third_identity, fourth_identity])
        session.flush()
        for left, right in (
            (second_identity, third_identity),
            (third_identity, fourth_identity),
        ):
            session.add(
                CatalogMatchDecision(
                    left_source_series_id=min(left.id, right.id),
                    right_source_series_id=max(left.id, right.id),
                    confidence=0.8,
                )
            )
        assert first_identity.series_id != fourth_identity.series_id

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        preview = await client.post(
            "/api/v2/match-batch/preview",
            json={"ids": [], "entire_queue": True, "decision": "accepted"},
        )
        result = await client.post(
            "/api/v2/match-batch",
            json={
                "ids": [],
                "entire_queue": True,
                "decision": "accepted",
                "confirmation": "MERGE",
            },
        )

    assert preview.status_code == 200
    assert preview.json()["selected"] == 3
    assert preview.json()["eligible"] == 0
    assert preview.json()["blocked"] == 3
    reasons = {reason for item in preview.json()["items"] for reason in item["blocked_reasons"]}
    assert "connected component contains 4 manga; maximum is 3" in reasons
    assert result.status_code == 200
    assert result.json()["ids"] == []
    assert len(result.json()["blocked"]) == 3
    with sessions() as session:
        assert session.query(CatalogSeries).count() == 4
        assert {row.decision for row in session.query(CatalogMatchDecision)} == {"pending"}


async def test_provider_registry_expands_manual_merge_limit_dynamically(monkeypatch) -> None:
    from manga_manager.domain import providers

    monkeypatch.setitem(providers.PROVIDER_ORIGINS, "fourth", "https://fourth.example")
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        existing = session.query(CatalogSeries).all()
        existing[0].status = "interested"
        ids = [row.id for row in existing]
        for source in ("kingofshojo", "fourth"):
            series = CatalogSeries(
                title=f"Series {source}", normalized_title=f"series {source}",
                status="interested",
            )
            session.add(series)
            session.flush()
            ids.append(series.id)
            session.add(CatalogSourceSeries(
                series_id=series.id, source=source, source_id=source, title=series.title,
                normalized_title=series.normalized_title, url=f"https://{source}.example/title",
            ))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        registry = await client.get("/api/v2/providers")
        preview = await client.post(
            "/api/v2/series/merge-preview", json={"series_ids": ids}
        )
    assert registry.json()["items"][-1] == "fourth"
    assert preview.status_code == 200 and preview.json()["can_merge"] is True


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
        label = session.query(MatchTrainingLabel).one()
        assert label.label == 1
        assert label.origin == "suggested_review"
        assert label.left_identity_json["title"]


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


async def test_failed_views_exclude_failures_resolved_by_a_later_success() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        session.add_all(
            [
                WorkJob(
                    kind="kavita_sync",
                    dedupe_key="series:resolved",
                    payload={"version": 1, "series_id": 2, "folder_path": ""},
                    status="failed",
                ),
                WorkJob(
                    kind="kavita_sync",
                    dedupe_key="series:resolved",
                    payload={"version": 1, "series_id": 2, "folder_path": ""},
                    status="succeeded",
                ),
                WorkJob(
                    kind="cover_backfill",
                    dedupe_key="cover:unresolved",
                    payload={"version": 1, "source_series_id": 1},
                    status="failed",
                ),
            ]
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        jobs = await client.get("/api/v2/jobs", params={"state": "failed"})
        groups = await client.get("/api/v2/job-groups", params={"state": "failed"})
        operations = await client.get("/api/v2/operations")

    assert [row["kind"] for row in jobs.json()["items"]] == ["cover_backfill"]
    assert [row["kind"] for row in groups.json()["items"]] == ["cover_backfill"]
    assert operations.json()["job_counts"]["failed"] == 1


async def test_workload_cycle_uses_live_active_units_when_counters_lag() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        cycle = WorkloadCycle(
            status="active",
            total_units=3,
            successful_units=3,
            added_units=3,
        )
        session.add(cycle)
        session.flush()
        session.add(
            WorkJob(
                kind="maintenance",
                dedupe_key="late-active-job",
                payload={"version": 1, "action": "stage_probe"},
                status="queued",
                cycle_id=cycle.id,
                logical_units=1,
            )
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/api/v2/workload-cycle")
    assert response.status_code == 200
    assert response.json()["remaining"] == 1
    assert response.json()["total"] == 4


async def test_job_group_and_child_keyset_cursors_do_not_repeat_rows() -> None:
    app, sessions = app_with_catalog()
    with sessions() as session, session.begin():
        for group in range(3):
            for child in range(2):
                session.add(WorkJob(
                    kind="maintenance",
                    dedupe_key=f"cursor:{group}:{child}",
                    payload={"version": 1, "action": "stage_probe"},
                    group_key=f"health:{group}",
                    priority=group + 1,
                    status="queued",
                ))
        session.add(
            WorkJob(
                kind="maintenance",
                dedupe_key="cursor:0:complete",
                payload={"version": 1, "action": "stage_probe"},
                group_key="health:0",
                priority=1,
                status="succeeded",
            )
        )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        first = await client.get("/api/v2/job-groups", params={"state": "queued", "limit": 2})
        second = await client.get(
            "/api/v2/job-groups",
            params={"state": "queued", "limit": 2, "cursor": first.json()["next_cursor"]},
        )
        child_first = await client.get(
            "/api/v2/job-groups/health:0/children", params={"state": "queued", "limit": 1}
        )
        child_second = await client.get(
            "/api/v2/job-groups/health:0/children",
            params={
                "state": "queued", "limit": 1,
                "cursor": child_first.json()["next_cursor"],
            },
        )
    first_keys = {row["key"] for row in first.json()["items"]}
    second_keys = {row["key"] for row in second.json()["items"]}
    assert first.status_code == second.status_code == 200
    assert first_keys.isdisjoint(second_keys)
    assert child_first.json()["items"][0]["id"] != child_second.json()["items"][0]["id"]
    health = next(row for row in first.json()["items"] if row["key"] == "health:0")
    assert health["progress"]["total"] == 3
    assert health["progress"]["current"] == 1


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
