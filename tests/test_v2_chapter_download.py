from __future__ import annotations

import asyncio
import io
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import httpx
from PIL import Image
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.adapters.base import SourceAdapter, SourceRateLimited
from app.domain import ChapterItem, SeriesItem
from manga_manager.application.chapter_download import ChapterDownloadHandler
from manga_manager.application.job_handlers import (
    DeferredJobError,
    JobContext,
    ReroutedJobError,
    RetryableJobError,
)
from manga_manager.domain.jobs import ChapterDownloadPayload, JobKind
from manga_manager.infrastructure.catalog_repository import CatalogRepository
from manga_manager.infrastructure.db_models import (
    CatalogChapterRelease,
    CatalogChapter,
    CatalogSeries,
    CatalogSourceSeries,
    CatalogSourceState,
    ChapterArtifact,
    ChapterDownloadIntent,
    ChapterReleaseAttempt,
    JobBase,
    LibraryProjection,
    SeriesDownloadPlan,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.storage import ContentAddressedStorage


def image_bytes(color: str) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (12, 12), color=color).save(output, format="PNG")
    return output.getvalue()


class TrackingSessions:
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


class PageAdapter(SourceAdapter):
    source = "fake"
    base_url = "https://example.test"

    def __init__(self, sessions: TrackingSessions, pages: list[bytes]) -> None:
        self.sessions = sessions
        self.pages = pages
        self.closed = False

    async def list_recent(self) -> list[SeriesItem]:
        return []

    async def get_chapters(self, source_series: SeriesItem) -> list[ChapterItem]:
        return []

    async def download_chapter_pages(self, chapter: ChapterItem) -> list[bytes]:
        return self.pages

    async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
        for page in self.pages:
            assert self.sessions.active == 0
            yield page

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def sessions() -> TrackingSessions:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    JobBase.metadata.create_all(engine)
    return TrackingSessions(sessionmaker(engine, expire_on_commit=False))


def setup_release_and_context(sessions: TrackingSessions) -> tuple[int, JobContext]:
    now = datetime.now(timezone.utc)
    with sessions() as session, session.begin():
        CatalogRepository().ingest(
            session,
            SeriesItem(
                source="fake",
                source_id="series-1",
                title="Example Series",
                url="https://example.test/series-1",
            ),
            [
                ChapterItem(
                    source="fake",
                    source_series_id="series-1",
                    number="1",
                    title="First",
                    url="https://example.test/chapter-1",
                )
            ],
        )
        release = session.scalar(select(CatalogChapterRelease))
        assert release is not None
        JobQueue().enqueue(
            session,
            kind=JobKind.CHAPTER_DOWNLOAD,
            dedupe_key=f"release:{release.id}",
            payload=ChapterDownloadPayload(chapter_release_id=release.id),
            available_at=now,
        )
        lease = JobQueue().claim(
            session,
            owner="worker-a",
            lease_for=timedelta(minutes=5),
            now=now,
        )
        assert lease is not None
        return release.id, JobContext(lease=lease, lease_lost=asyncio.Event())


def storage(tmp_path: Path) -> ContentAddressedStorage:
    return ContentAddressedStorage(
        tmp_path / "storage-v2",
        max_page_bytes=1024 * 1024,
        max_chapter_bytes=10 * 1024 * 1024,
        max_pages=100,
        min_free_bytes=0,
    )


@pytest.mark.asyncio
async def test_download_handler_keeps_database_closed_and_materializes_cbz(
    sessions: TrackingSessions,
    tmp_path: Path,
) -> None:
    _release_id, context = setup_release_and_context(sessions)
    adapter = PageAdapter(sessions, [image_bytes("red"), image_bytes("blue")])
    store = storage(tmp_path)
    handler = ChapterDownloadHandler(
        session_factory=sessions,
        storage=store,
        adapter_factory=lambda _source: adapter,
    )
    await asyncio.wait_for(handler(context), timeout=5)
    assert adapter.closed is True

    with sessions() as session:
        artifact = session.scalar(select(ChapterArtifact))
        projection = session.scalar(select(LibraryProjection))
        assert artifact is not None and artifact.state == "active"
        assert artifact.image_count == 2
        assert projection is not None
        assert (store.library_root / projection.relative_path).is_file()


@pytest.mark.asyncio
async def test_download_success_reconciles_plan_without_periodic_recovery(
    sessions: TrackingSessions,
    tmp_path: Path,
) -> None:
    _release_id, context = setup_release_and_context(sessions)
    with sessions() as session, session.begin():
        series = session.scalar(select(CatalogSeries))
        chapter = session.scalar(select(CatalogChapter))
        assert series is not None and chapter is not None
        series.status = "interested"
        session.add(
            SeriesDownloadPlan(
                series_id=series.id,
                status="active",
                phase="priority",
                total_chapters=1,
            )
        )
        session.add(
            ChapterDownloadIntent(
                series_id=series.id,
                chapter_id=chapter.id,
                tier="priority",
                state="queued",
                job_id=context.lease.id,
            )
        )
        series_id = series.id

    handler = ChapterDownloadHandler(
        session_factory=sessions,
        storage=storage(tmp_path),
        adapter_factory=lambda _source: PageAdapter(sessions, [image_bytes("green")]),
    )

    await asyncio.wait_for(handler(context), timeout=5)

    with sessions() as session:
        plan = session.get(SeriesDownloadPlan, series_id)
        intent = session.scalar(select(ChapterDownloadIntent))
        assert plan is not None
        assert plan.status == "complete"
        assert plan.phase == "complete"
        assert plan.total_chapters == 1
        assert plan.satisfied_chapters == 1
        assert intent is not None and intent.state == "satisfied"


@pytest.mark.asyncio
async def test_download_handler_classifies_invalid_pages_as_retryable(
    sessions: TrackingSessions,
    tmp_path: Path,
) -> None:
    _release_id, context = setup_release_and_context(sessions)
    adapter = PageAdapter(sessions, [b"not an image"])
    handler = ChapterDownloadHandler(
        session_factory=sessions,
        storage=storage(tmp_path),
        adapter_factory=lambda _source: adapter,
    )
    with pytest.raises(RetryableJobError) as error:
        await handler(context)
    assert error.value.code == "invalid_content"


@pytest.mark.asyncio
async def test_rate_limit_sets_shared_source_cooldown(
    sessions: TrackingSessions,
    tmp_path: Path,
) -> None:
    _release_id, context = setup_release_and_context(sessions)

    class RateLimitedAdapter(PageAdapter):
        async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
            raise SourceRateLimited("slow down")
            yield b""  # pragma: no cover

    adapter = RateLimitedAdapter(sessions, [])
    handler = ChapterDownloadHandler(
        session_factory=sessions,
        storage=storage(tmp_path),
        adapter_factory=lambda _source: adapter,
        cooldowns={"default": timedelta(minutes=7)},
    )
    with pytest.raises(DeferredJobError) as error:
        await handler(context)
    assert error.value.code == "rate_limited"
    with sessions() as session:
        state = session.get(CatalogSourceState, "fake")
        assert state is not None
        assert state.health_status == "cooldown"
        assert state.cooldown_until is not None


@pytest.mark.asyncio
async def test_cloudflare_origin_outage_defers_download_without_spending_attempts(
    sessions: TrackingSessions,
    tmp_path: Path,
) -> None:
    _release_id, context = setup_release_and_context(sessions)

    class OriginUnavailableAdapter(PageAdapter):
        async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
            request = httpx.Request("GET", chapter.url)
            response = httpx.Response(521, request=request)
            raise httpx.HTTPStatusError(
                "origin unavailable", request=request, response=response
            )
            yield b""  # pragma: no cover

    handler = ChapterDownloadHandler(
        session_factory=sessions,
        storage=storage(tmp_path),
        adapter_factory=lambda _source: OriginUnavailableAdapter(sessions, []),
    )

    with pytest.raises(DeferredJobError) as error:
        await handler(context)

    assert error.value.code == "provider_origin_unavailable"
    assert error.value.retry_after >= timedelta(minutes=4)
    with sessions() as session:
        state = session.get(CatalogSourceState, "fake")
        assert state is not None
        assert state.health_status == "cooldown"


@pytest.mark.asyncio
async def test_rate_limit_reroutes_same_chapter_to_next_provider(
    sessions: TrackingSessions,
    tmp_path: Path,
) -> None:
    now = datetime.now(timezone.utc)
    with sessions() as session, session.begin():
        series = CatalogSeries(title="Example", normalized_title="example", status="interested")
        session.add(series)
        session.flush()
        chapter = CatalogChapter(
            series_id=series.id,
            canonical_number="1",
            display_number="1",
        )
        session.add(chapter)
        session.flush()
        releases = []
        for source in ("asura", "mangafire"):
            identity = CatalogSourceSeries(
                series_id=series.id,
                source=source,
                source_id=f"{source}-example",
                title="Example",
                normalized_title="example",
                url=f"https://{source}.test/example",
            )
            session.add(identity)
            session.flush()
            release = CatalogChapterRelease(
                chapter_id=chapter.id,
                source_series_id=identity.id,
                source=source,
                source_release_id=f"{source}-1",
                url=f"https://{source}.test/example/1",
            )
            session.add(release)
            session.flush()
            releases.append(release)
        job, _ = JobQueue().enqueue(
            session,
            kind=JobKind.CHAPTER_DOWNLOAD,
            dedupe_key=f"chapter:{chapter.id}",
            payload=ChapterDownloadPayload(chapter_release_id=releases[0].id),
            source="asura",
            series_key=str(series.id),
            available_at=now,
        )
        lease = JobQueue().claim(
            session,
            owner="worker-a",
            lease_for=timedelta(minutes=5),
            now=now,
            pool_limits={"download:asura": 1, "chapter_global": 8},
        )
        assert lease is not None and lease.id == job.id

    class RateLimitedAdapter(PageAdapter):
        async def iter_chapter_pages(self, chapter: ChapterItem) -> AsyncIterator[bytes]:
            raise SourceRateLimited("slow down")
            yield b""  # pragma: no cover

    handler = ChapterDownloadHandler(
        session_factory=sessions,
        storage=storage(tmp_path),
        adapter_factory=lambda _source: RateLimitedAdapter(sessions, []),
        cooldowns={"asura": timedelta(minutes=7), "default": timedelta(minutes=7)},
    )
    with pytest.raises(ReroutedJobError):
        await handler(JobContext(lease=lease, lease_lost=asyncio.Event()))

    with sessions() as session:
        jobs = session.scalars(select(WorkJob).order_by(WorkJob.id)).all()
        assert [row.status for row in jobs] == ["retry_wait"]
        assert jobs[0].id == job.id
        assert jobs[0].source == "mangafire"
        assert jobs[0].payload["chapter_release_id"] == releases[1].id
        attempts = session.scalars(
            select(ChapterReleaseAttempt).order_by(ChapterReleaseAttempt.id)
        ).all()
        assert [row.outcome for row in attempts] == ["failed", "fallback_queued"]
