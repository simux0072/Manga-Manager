from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.adapters.base import FrontierSentinel, SourceAdapter
from app.domain import ChapterItem, SeriesItem
from manga_manager.application.job_handlers import JobContext
from manga_manager.application.source_pull import SourcePullHandler
from manga_manager.domain.jobs import JobKind, SourcePullPayload
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogSeries,
    CatalogSourceSeries,
    CatalogSourceState,
    JobBase,
    JobEvent,
)
from manga_manager.infrastructure.job_queue import JobQueue


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

    async def list_recent_frontier(
        self, sentinels: list[FrontierSentinel]
    ) -> list[SeriesItem]:
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
                metadata={"rating": 9.1},
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


@pytest.fixture
def sessions() -> TrackingSessionFactory:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    JobBase.metadata.create_all(engine)
    return TrackingSessionFactory(sessionmaker(engine, expire_on_commit=False))


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
        progress = session.scalars(
            select(JobEvent).where(JobEvent.event_type == "progress")
        ).all()
        assert len(progress) == 2


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
