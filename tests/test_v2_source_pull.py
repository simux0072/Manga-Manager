from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
import httpx
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.adapters.base import FrontierSentinel, SourceAdapter
from app.domain import ChapterItem, SeriesItem
from manga_manager.application.job_handlers import DeferredJobError, JobContext, RetryableJobError
from manga_manager.application.source_pull import (
    SourcePullHandler,
    SourceRefreshHandler,
    build_listing_frontier,
    changed_before_stable_frontier,
)
from manga_manager.domain.jobs import JobKind, SourcePullPayload, SourceRefreshPayload
from manga_manager.infrastructure.db_models import (
    CatalogAlternateSourceListing,
    CatalogChapter,
    CatalogChapterRelease,
    CatalogObservation,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSourceSeries,
    CatalogSourceState,
    JobBase,
    ProviderEndpointState,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.catalog_repository import CatalogRepository


class TrackingSessionFactory:
    def __init__(self, factory: sessionmaker[Session]) -> None:
        self.factory = factory
        self.active = 0

    @contextmanager
    def __call__(self) -> Iterator[Session]:
        self.active += 1
        try:
            with self.factory() as session:
                yield session
        finally:
            self.active -= 1


class FakeAdapter(SourceAdapter):
    source = "fake"
    base_url = "https://example.test"

    def __init__(self, sessions: TrackingSessionFactory) -> None:
        self.sessions = sessions
        self.frontier: list[FrontierSentinel] = []
        self.closed = False

    async def list_recent(self) -> list[SeriesItem]:
        return await self.list_recent_frontier([])

    async def list_recent_frontier(self, sentinels: list[FrontierSentinel]) -> list[SeriesItem]:
        assert self.sessions.active == 0
        self.frontier = sentinels
        return [
            SeriesItem(
                source="fake",
                source_id="series-1",
                title="The Example Hero",
                url="https://example.test/series-1",
                aliases=("Example Hero",),
                description="A test series",
                cover_url="https://example.test/cover.jpg",
                external_ids={"mal": "123"},
                metadata={"rating": 9.1, "recent_chapters": [{"number": "10.0"}]},
            )
        ]

    async def get_series_detail(self, item: SeriesItem) -> SeriesItem:
        assert self.sessions.active == 0
        return item

    async def get_chapters(self, source_series: SeriesItem) -> list[ChapterItem]:
        assert self.sessions.active == 0
        return [
            ChapterItem(
                source="fake",
                source_series_id=source_series.source_id,
                number="10.0",
                title="Chapter 10",
                url="https://example.test/series-1/10",
            )
        ]

    async def download_chapter_pages(self, chapter: ChapterItem) -> list[bytes]:
        raise AssertionError("source pull must not download pages")

    async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
        raise AssertionError("source pull must not download pages")
        yield b""

    async def aclose(self) -> None:
        self.closed = True


class DisconnectedAdapter(FakeAdapter):
    async def list_recent_frontier(self, sentinels: list[FrontierSentinel]) -> list[SeriesItem]:
        raise httpx.RemoteProtocolError("")


class OriginUnavailableAdapter(FakeAdapter):
    async def get_series_detail(self, item: SeriesItem) -> SeriesItem:
        request = httpx.Request("GET", item.url)
        response = httpx.Response(521, request=request)
        raise httpx.HTTPStatusError("origin unavailable", request=request, response=response)


@pytest.fixture
def sessions() -> TrackingSessionFactory:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    JobBase.metadata.create_all(engine)
    return TrackingSessionFactory(sessionmaker(engine, autoflush=False, expire_on_commit=False))


def claimed_context(sessions: TrackingSessionFactory) -> JobContext:
    now = datetime.now(timezone.utc)
    with sessions() as session, session.begin():
        JobQueue().enqueue(
            session,
            kind=JobKind.SOURCE_PULL,
            dedupe_key="source:fake",
            payload=SourcePullPayload(source="fake"),
            available_at=now,
        )
        lease = JobQueue().claim(
            session,
            owner="worker-a",
            lease_for=timedelta(minutes=5),
            now=now,
        )
        assert lease is not None
    return JobContext(lease=lease, lease_lost=asyncio.Event())


def claimed_refresh_context(sessions: TrackingSessionFactory) -> JobContext:
    now = datetime.now(timezone.utc)
    with sessions() as session, session.begin():
        JobQueue().enqueue(
            session,
            kind=JobKind.SOURCE_REFRESH,
            dedupe_key="refresh:fake:series-1",
            payload=SourceRefreshPayload(
                source="fake",
                source_id="series-1",
                title="The Example Hero",
                url="https://example.test/series-1",
            ),
            available_at=now,
        )
        lease = JobQueue().claim(
            session,
            owner="worker-refresh",
            lease_for=timedelta(minutes=5),
            now=now,
            kinds={JobKind.SOURCE_REFRESH},
        )
        assert lease is not None
    return JobContext(lease=lease, lease_lost=asyncio.Event())


@pytest.mark.asyncio
async def test_source_pull_keeps_database_closed_during_http_and_ingests_idempotently(
    sessions: TrackingSessionFactory,
) -> None:
    adapter = FakeAdapter(sessions)
    handler = SourcePullHandler(
        session_factory=sessions,
        adapter_factory=lambda _source: adapter,
    )
    context = claimed_context(sessions)
    await handler(context)
    await handler(context)

    with sessions() as session, session.begin():
        assert (
            session.scalar(
                select(func.count()).select_from(WorkJob).where(WorkJob.kind == "source_refresh")
            )
            == 1
        )
        assert JobQueue().succeed(session, job_id=context.lease.id, owner=context.lease.owner)
        refresh_lease = JobQueue().claim(
            session,
            owner="worker-refresh",
            lease_for=timedelta(minutes=5),
            kinds={JobKind.SOURCE_REFRESH},
        )
        assert refresh_lease is not None
    await SourceRefreshHandler(session_factory=sessions, adapter_factory=lambda _source: adapter)(
        JobContext(lease=refresh_lease, lease_lost=asyncio.Event())
    )

    assert adapter.closed is True
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(CatalogSeries)) == 1
        assert session.scalar(select(func.count()).select_from(CatalogSourceSeries)) == 1
        assert session.scalar(select(func.count()).select_from(CatalogChapter)) == 1
        assert session.scalar(select(func.count()).select_from(CatalogChapterRelease)) == 1
        state = session.get(CatalogSourceState, "fake")
        assert state is not None
        assert state.health_status == "healthy"
        assert state.frontier_json == [{"source_id": "series-1", "latest_chapter": "10.0"}]
        assert (
            session.scalar(
                select(func.count()).select_from(WorkJob).where(WorkJob.kind == "source_refresh")
            )
            == 1
        )


@pytest.mark.asyncio
async def test_source_pull_reads_persisted_frontier(sessions: TrackingSessionFactory) -> None:
    with sessions() as session, session.begin():
        session.add(
            CatalogSourceState(
                source="fake",
                frontier_json=[{"source_id": "known", "latest_chapter": "9"}],
            )
        )
    adapter = FakeAdapter(sessions)
    handler = SourcePullHandler(
        session_factory=sessions,
        adapter_factory=lambda _source: adapter,
    )
    await handler(claimed_context(sessions))
    assert adapter.frontier == [FrontierSentinel(source_id="known", latest_chapter="9")]


@pytest.mark.asyncio
async def test_source_pull_classifies_empty_protocol_error_as_network_failure(
    sessions: TrackingSessionFactory,
) -> None:
    adapter = DisconnectedAdapter(sessions)
    handler = SourcePullHandler(
        session_factory=sessions,
        adapter_factory=lambda _source: adapter,
    )

    with pytest.raises(RetryableJobError) as raised:
        await handler(claimed_context(sessions))

    assert raised.value.code == "source_network_error"
    assert raised.value.message == "RemoteProtocolError"
    assert adapter.closed is True


@pytest.mark.asyncio
async def test_source_refresh_defers_cloudflare_origin_outage_without_quarantine(
    sessions: TrackingSessionFactory,
) -> None:
    adapter = OriginUnavailableAdapter(sessions)
    handler = SourceRefreshHandler(
        session_factory=sessions,
        adapter_factory=lambda _source: adapter,
    )

    with pytest.raises(DeferredJobError) as raised:
        await handler(claimed_refresh_context(sessions))

    assert raised.value.code == "provider_origin_unavailable"
    assert raised.value.retry_after >= timedelta(minutes=4)
    assert adapter.closed is True
    with sessions() as session:
        state = session.get(CatalogSourceState, "fake")
        assert state is not None
        assert state.health_status == "cooldown"
        assert state.cooldown_until is not None
        assert session.scalar(select(func.count()).select_from(CatalogObservation)) == 0


def test_exact_title_without_external_id_creates_pending_match(
    sessions: TrackingSessionFactory,
) -> None:
    repository = CatalogRepository()
    with sessions() as session, session.begin():
        first = repository.ingest(
            session,
            SeriesItem("asura", "one", "Shared Title", "https://asura.test/one"),
            [],
        )
        second = repository.ingest(
            session,
            SeriesItem("mangafire", "two", "Shared Title", "https://mf.test/two"),
            [],
        )
        assert first.series_id != second.series_id
        decision = session.scalar(select(CatalogMatchDecision))
        assert decision is not None
        assert decision.decision == "pending"
        assert decision.evidence_json["cover_match"] is False


def test_shared_external_id_never_adds_second_identity_from_same_provider(
    sessions: TrackingSessionFactory,
) -> None:
    repository = CatalogRepository()
    with sessions() as session, session.begin():
        first = repository.ingest(
            session,
            SeriesItem(
                "asura",
                "one",
                "First",
                "https://asura.test/one",
                external_ids={"mal": "42"},
            ),
            [],
        )
        second = repository.ingest(
            session,
            SeriesItem(
                "asura",
                "two",
                "Second",
                "https://asura.test/two",
                external_ids={"mal": "42"},
            ),
            [],
        )
        assert first.series_id != second.series_id


def test_ingest_reuses_historical_alternate_provider_identity(
    sessions: TrackingSessionFactory,
) -> None:
    repository = CatalogRepository()
    with sessions() as session, session.begin():
        primary = repository.ingest(
            session,
            SeriesItem("mangafire", "primary-id", "Same Manga", "https://mf.test/primary"),
            [],
        )
        primary_id = primary.id
        session.add(
            CatalogAlternateSourceListing(
                primary_source_series_id=primary.id,
                source="mangafire",
                source_id="historical-id",
                title="Old title",
                url="https://mf.test/historical",
                evidence_json={},
            )
        )

    with sessions() as session, session.begin():
        alternate_item = SeriesItem(
            "mangafire",
            "historical-id",
            "Same Manga Updated",
            "https://mf.test/historical-new",
        )
        assert (
            SourcePullHandler._changed_catalog_items(session, "mangafire", [alternate_item]) == []
        )
        resolved = repository.ingest(
            session,
            alternate_item,
            [],
        )
        assert resolved.id == primary_id
        assert resolved.source_id == "primary-id"
        assert session.query(CatalogSourceSeries).count() == 1
        assert session.query(CatalogSeries).count() == 1
        alternate = session.query(CatalogAlternateSourceListing).one()
        assert alternate.title == "Same Manga Updated"
        assert alternate.url == "https://mf.test/historical-new"


def test_ingest_registers_strong_same_provider_duplicate_as_alternate(
    sessions: TrackingSessionFactory,
) -> None:
    repository = CatalogRepository()

    def chapters(source_id: str) -> list[ChapterItem]:
        return [
            ChapterItem(
                "mangafire",
                source_id,
                str(number),
                f"Chapter {number}",
                f"https://mf.test/{source_id}/{number}",
            )
            for number in range(1, 11)
        ]

    with sessions() as session, session.begin():
        primary = repository.ingest(
            session,
            SeriesItem(
                "mangafire", "first-id", "Shared Manga", "https://mf.test/first-id"
            ),
            chapters("first-id"),
        )
        resolved = repository.ingest(
            session,
            SeriesItem(
                "mangafire", "second-id", "Shared Manga", "https://mf.test/second-id"
            ),
            chapters("second-id"),
        )
        assert resolved.id == primary.id
        assert resolved.source_id == "first-id"
        assert session.query(CatalogSeries).count() == 1
        assert session.query(CatalogSourceSeries).count() == 1
        alternate = session.query(CatalogAlternateSourceListing).one()
        assert alternate.source_id == "second-id"
        assert alternate.primary_source_series_id == primary.id
        assert alternate.evidence_json["chapter_overlap_ratio"] == 1.0


def test_ingest_does_not_collapse_same_title_with_conflicting_external_id(
    sessions: TrackingSessionFactory,
) -> None:
    repository = CatalogRepository()
    chapters = [
        ChapterItem(
            "mangafire", "id", str(number), f"Chapter {number}", f"https://mf.test/{number}"
        )
        for number in range(1, 8)
    ]
    with sessions() as session, session.begin():
        repository.ingest(
            session,
            SeriesItem(
                "mangafire",
                "first-id",
                "Generic Shared Title",
                "https://mf.test/first-id",
                external_ids={"anilist": "100"},
            ),
            chapters,
        )
        repository.ingest(
            session,
            SeriesItem(
                "mangafire",
                "second-id",
                "Generic Shared Title",
                "https://mf.test/second-id",
                external_ids={"anilist": "200"},
            ),
            chapters,
        )
        assert session.query(CatalogSeries).count() == 2
        assert session.query(CatalogSourceSeries).count() == 2
        assert session.query(CatalogAlternateSourceListing).count() == 0


def test_asura_ingest_uses_global_revision_without_persisting_an_override(
    sessions: TrackingSessionFactory,
) -> None:
    repository = CatalogRepository()
    with sessions() as session, session.begin():
        row = repository.ingest(
            session,
            SeriesItem(
                "asura",
                "comics/painter",
                "Painter",
                "https://asurascans.com/comics/painter-1d35e5bd",
                metadata={"asura_revision": "1d35e5bd"},
            ),
            [],
        )
        assert row.source_id == "comics/painter"
        assert row.normalized_source_id == "comics/painter"
        assert row.revision_override == ""


def test_asura_ingest_retains_only_explicit_per_series_revision_override(
    sessions: TrackingSessionFactory,
) -> None:
    with sessions() as session, session.begin():
        row = CatalogRepository().ingest(
            session,
            SeriesItem(
                "asura",
                "comics/exception",
                "Exception",
                "https://asurascans.com/comics/exception-deadbeef",
                metadata={
                    "asura_revision": "deadbeef",
                    "asura_revision_override": "deadbeef",
                },
            ),
            [],
        )
        assert row.revision_override == "deadbeef"


def test_asura_revision_requires_three_distinct_series_for_global_consensus(
    sessions: TrackingSessionFactory,
) -> None:
    items = [
        SeriesItem(
            "asura",
            f"comics/title-{index}",
            f"Title {index}",
            f"https://asurascans.com/comics/title-{index}-1d35e5bd",
            metadata={"asura_revision": "1d35e5bd"},
        )
        for index in range(3)
    ]
    with sessions() as session, session.begin():
        normalized = SourcePullHandler._apply_asura_revision_consensus(session, items)
        state = session.get(CatalogSourceState, "asura")
        assert state is not None
        assert state.cursor_json["global_revision"] == "1d35e5bd"
        assert all("-1d35e5bd" in row.url for row in normalized)
        assert all("asura_revision_override" not in row.metadata for row in normalized)


def test_asura_mixed_revision_without_consensus_uses_observed_per_series_urls(
    sessions: TrackingSessionFactory,
) -> None:
    items = [
        SeriesItem(
            "asura",
            f"comics/title-{index}",
            f"Title {index}",
            "https://example/old",
            metadata={"asura_revision": revision},
        )
        for index, revision in enumerate(("1d35e5bd", "1d35e5bd", "deadbeef"))
    ]
    with sessions() as session, session.begin():
        normalized = SourcePullHandler._apply_asura_revision_consensus(session, items)
        state = session.get(CatalogSourceState, "asura")
        assert state is not None and "global_revision" not in state.cursor_json
        assert normalized[0].url.endswith("-1d35e5bd")
        assert normalized[2].url.endswith("-deadbeef")


def test_undated_descending_chapters_recompute_numeric_latest(
    sessions: TrackingSessionFactory,
) -> None:
    with sessions() as session, session.begin():
        identity = CatalogRepository().ingest(
            session,
            SeriesItem("mangafire", "numeric", "Numeric", "https://example/numeric"),
            [
                ChapterItem(
                    "mangafire",
                    "numeric",
                    str(number),
                    f"Chapter {number}",
                    f"https://example/numeric/{number}",
                )
                for number in range(75, 0, -1)
            ],
        )
        series = session.get(CatalogSeries, identity.series_id)
        assert series is not None and series.latest_release_number == "75"


def test_duplicate_normalized_aliases_are_inserted_once(
    sessions: TrackingSessionFactory,
) -> None:
    with sessions() as session, session.begin():
        CatalogRepository().ingest(
            session,
            SeriesItem(
                "asura",
                "alias-test",
                "Eden",
                "https://asura.test/eden",
                aliases=("Eden", " EDEN ", "Eden!"),
            ),
            [],
        )
    with sessions() as session:
        from manga_manager.infrastructure.db_models import CatalogSeriesAlias

        assert session.scalar(select(func.count()).select_from(CatalogSeriesAlias)) == 1


def test_source_failure_repairs_null_counter_and_opens_circuit_after_three(
    sessions: TrackingSessionFactory,
) -> None:
    repository = CatalogRepository()
    with sessions() as session, session.begin():
        state = CatalogSourceState(source="broken", consecutive_failures=0)
        session.add(state)
        session.flush()
        state.consecutive_failures = None
        repository.record_poll_failure(session, source="broken", error="first")
    with sessions() as session, session.begin():
        repository.record_poll_failure(session, source="broken", error="second")
        repository.record_poll_failure(session, source="broken", error="third")
    with sessions() as session:
        state = session.get(CatalogSourceState, "broken")
        assert state is not None
        assert state.consecutive_failures == 3
        assert state.health_status == "cooldown"
        assert state.cooldown_until is not None


def test_source_pull_failure_records_endpoint_cooldown(
    sessions: TrackingSessionFactory,
) -> None:
    retry_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    SourcePullHandler(
        session_factory=sessions, adapter_factory=lambda _source: None
    )._record_failure("asura", "limited", retry_at, traffic_class="origin")

    with sessions() as session:
        endpoint = session.scalar(select(ProviderEndpointState))
        assert endpoint is not None
        assert endpoint.source == "asura"
        assert endpoint.traffic_class == "origin"
        assert endpoint.cooldown_until is not None


def test_frontier_only_enriches_new_or_advanced_rows_before_stable_boundary() -> None:
    def item(source_id: str, chapter: str) -> SeriesItem:
        return SeriesItem(
            "asura",
            source_id,
            source_id,
            f"https://asura.test/{source_id}",
            metadata={"recent_chapters": [{"number": chapter}]},
        )

    listed = [
        item("new", "1"),
        item("advanced", "11"),
        item("stable-a", "10"),
        item("stable-b", "8"),
        item("stable-c", "5"),
        item("older", "1"),
    ]
    sentinels = [
        FrontierSentinel("advanced", "10"),
        FrontierSentinel("stable-a", "10"),
        FrontierSentinel("stable-b", "8"),
        FrontierSentinel("stable-c", "5"),
    ]
    changed = changed_before_stable_frontier(listed, sentinels, required_hits=3)
    assert [row.source_id for row in changed] == ["new", "advanced"]
    assert build_listing_frontier(listed, 5) == [
        {"source_id": row.source_id, "latest_chapter": row.metadata["recent_chapters"][0]["number"]}
        for row in listed[:5]
    ]


def test_catalog_comparison_skips_unchanged_identity_and_refreshes_advanced_one(
    sessions: TrackingSessionFactory,
) -> None:
    repository = CatalogRepository()
    existing = SeriesItem(
        "mangafire",
        "known",
        "Known",
        "https://example/known",
        metadata={"recent_chapters": [{"number": "10"}]},
    )
    with sessions() as session, session.begin():
        repository.ingest(session, existing, [])
    with sessions() as session:
        assert SourcePullHandler._changed_catalog_items(session, "mangafire", [existing]) == []
        advanced = replace(existing, metadata={"recent_chapters": [{"number": "11"}]})
        assert SourcePullHandler._changed_catalog_items(session, "mangafire", [advanced]) == [
            advanced
        ]
