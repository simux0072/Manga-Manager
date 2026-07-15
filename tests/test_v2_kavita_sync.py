from __future__ import annotations

import asyncio
import io
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import httpx
from PIL import Image
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.kavita import KavitaChapter, KavitaReadProgress, KavitaSeries
from manga_manager.application.cbz_import import LegacyCbzImporter
from manga_manager.application.job_handlers import JobContext
from manga_manager.application.kavita_sync import (
    KavitaSnapshot,
    KavitaSyncHandler,
    KavitaSyncPlanner,
    match_series,
)
from manga_manager.domain.jobs import JobKind, KavitaSyncPayload
from manga_manager.infrastructure.db_models import (
    ArtifactBlob,
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogSeries,
    ChapterArtifact,
    JobBase,
    KavitaProjection,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.storage import ContentAddressedStorage


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


class FakeKavitaClient:
    configured = True

    def __init__(self, sessions: TrackingSessions) -> None:
        self.sessions = sessions
        self.scanned: Path | None = None
        self.wanted: list[int] = []
        self.series_covers: list[tuple[int, str]] = []
        self.chapter_covers: list[tuple[int, str]] = []

    async def scan_folder_or_all(self, folder_path: Path) -> None:
        assert self.sessions.active == 0
        self.scanned = folder_path

    async def list_series(self) -> list[KavitaSeries]:
        assert self.sessions.active == 0
        return [KavitaSeries(id=20, name="Example Series", library_id=3)]

    async def series_detail(self, series_id: int) -> list[KavitaChapter]:
        assert self.sessions.active == 0
        assert series_id == 20
        return [KavitaChapter(id=30, number="1", volume_id=4, pages_total=1)]

    async def chapter_progress(
        self, chapter_id: int, pages_total: int = 0
    ) -> KavitaReadProgress:
        assert self.sessions.active == 0
        return KavitaReadProgress(
            chapter_id=chapter_id,
            pages_read=pages_total,
            pages_total=pages_total,
        )

    async def add_want_to_read(self, series_ids: list[int]) -> None:
        self.wanted.extend(series_ids)

    async def remove_want_to_read(self, series_ids: list[int]) -> None:
        self.wanted = [value for value in self.wanted if value not in series_ids]

    async def upload_series_cover(self, series_id: int, data_url: str) -> None:
        self.series_covers.append((series_id, data_url))

    async def upload_chapter_cover(self, chapter_id: int, data_url: str) -> None:
        self.chapter_covers.append((chapter_id, data_url))


def make_cbz(path: Path) -> None:
    image = io.BytesIO()
    Image.new("RGB", (8, 8), color="red").save(image, format="PNG")
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "ComicInfo.xml",
            "<ComicInfo><Series>Example Series</Series><Number>1</Number></ComicInfo>",
        )
        archive.writestr("0001.png", image.getvalue())


@pytest.mark.asyncio
async def test_kavita_sync_maps_series_and_chapters_without_open_database_session(
    tmp_path: Path,
) -> None:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    JobBase.metadata.create_all(engine)
    sessions = TrackingSessions(
        sessionmaker(engine, autoflush=False, expire_on_commit=False)
    )
    storage = ContentAddressedStorage(
        tmp_path / "storage-v2",
        max_page_bytes=1024 * 1024,
        max_chapter_bytes=10 * 1024 * 1024,
        max_pages=100,
        min_free_bytes=0,
    )
    archive = tmp_path / "chapter.cbz"
    make_cbz(archive)
    LegacyCbzImporter(session_factory=sessions, storage=storage).import_file(archive, dry_run=False)
    now = datetime.now(timezone.utc)
    with sessions() as session, session.begin():
        series = session.scalar(select(CatalogSeries))
        assert series is not None
        series.status = "interested"
        series.cover_url = "https://covers.test/example.png"
        chapter = session.scalar(select(CatalogChapter))
        artifact = session.scalar(select(ChapterArtifact))
        assert chapter is not None and artifact is not None
        blob = session.get(ArtifactBlob, artifact.blob_checksum)
        assert blob is not None
        relative = f"Manga/{series.storage_key}/ch-{chapter.id}.cbz"
        storage.materialize_kavita(blob.relative_path, relative)
        session.add(
            KavitaProjection(
                chapter_id=chapter.id,
                artifact_id=artifact.id,
                relative_path=relative,
            )
        )
        job, _ = JobQueue().enqueue(
            session,
            kind=JobKind.KAVITA_SYNC,
            dedupe_key=f"series:{series.id}",
            payload=KavitaSyncPayload(series_id=series.id),
            available_at=now,
        )
        job.status = "retry_wait"
        job.error_code = "kavita_unconfigured"
        job.error_message = "Kavita is not configured"
        job.available_at = now + timedelta(hours=1)
        pending, queued = KavitaSyncPlanner().enqueue_pending(session)
        assert (pending, queued) == (1, 1)
        assert job.status == "retry_wait"
        assert job.error_code == ""
        assert job.available_at <= datetime.now(timezone.utc)
        lease = JobQueue().claim(
            session,
            owner="worker-a",
            lease_for=timedelta(minutes=5),
            now=datetime.now(timezone.utc),
        )
        assert lease is not None
        series_id = series.id

    client = FakeKavitaClient(sessions)
    cover_image = io.BytesIO()
    Image.new("RGB", (16, 24), color="blue").save(cover_image, format="PNG")

    async def fetch_cover(_url: str) -> bytes:
        return cover_image.getvalue()

    await KavitaSyncHandler(
        session_factory=sessions,
        library_root=storage.kavita_root,
        client_factory=lambda: client,
        cover_fetcher=fetch_cover,
    )(JobContext(lease=lease, lease_lost=asyncio.Event()))

    assert client.scanned is not None
    assert client.wanted == [20]
    assert [row[0] for row in client.series_covers] == [20]
    assert [row[0] for row in client.chapter_covers] == [30]
    assert client.series_covers[0][1] == client.chapter_covers[0][1]
    with sessions() as session:
        series = session.get(CatalogSeries, series_id)
        chapter = session.scalar(select(CatalogChapter))
        assert series is not None and series.kavita_series_id == 20
        assert series.kavita_cover_checksum
        assert (storage.root / series.cover_relative_path).is_file()
        assert series.kavita_library_id == 3
        assert chapter is not None and chapter.kavita_chapter_id == 30
        assert chapter.kavita_cover_checksum == series.kavita_cover_checksum
        assert chapter.kavita_volume_id == 4
        reading = session.get(CatalogChapterReadingState, chapter.id)
        assert reading is not None and reading.status == "read"
        assert series.status == "caught_up"


@pytest.mark.asyncio
async def test_kavita_cover_falls_back_when_preferred_source_cover_is_invalid(
    tmp_path: Path,
) -> None:
    image = io.BytesIO()
    Image.new("RGB", (12, 18), color="green").save(image, format="PNG")
    requested: list[str] = []

    async def fetch_cover(url: str) -> bytes:
        requested.append(url)
        return b"not an image" if url.endswith("preferred") else image.getvalue()

    handler = KavitaSyncHandler(
        session_factory=lambda: None,  # type: ignore[arg-type]
        library_root=tmp_path / "library",
        cover_fetcher=fetch_cover,
    )
    snapshot = KavitaSnapshot(
        series_id=1,
        title="Example",
        existing_kavita_id=None,
        folder_path=tmp_path,
        tracked=True,
        aliases=(),
        external_ids={},
        cover_urls=("https://covers.test/preferred", "https://covers.test/fallback"),
        cover_checksum="",
        kavita_cover_checksum="",
        chapter_cover_checksums={},
    )

    cover = await handler._cover(snapshot)

    assert cover is not None and cover[2] == "image/png"
    assert requested == [
        "https://covers.test/preferred",
        "https://covers.test/fallback",
    ]


@pytest.mark.asyncio
async def test_kavita_cover_write_retries_transient_server_error(monkeypatch) -> None:
    attempts = 0

    async def upload() -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            request = httpx.Request("POST", "http://kavita/api/Upload/chapter")
            response = httpx.Response(500, request=request)
            raise httpx.HTTPStatusError("busy", request=request, response=response)

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("manga_manager.application.kavita_sync.asyncio.sleep", no_sleep)
    await KavitaSyncHandler._retry_cover_write(upload)
    assert attempts == 3


@pytest.mark.asyncio
async def test_kavita_sync_waits_until_asynchronous_scan_exposes_chapters(
    tmp_path: Path, monkeypatch
) -> None:
    class DelayedClient:
        configured = True

        def __init__(self) -> None:
            self.detail_calls = 0

        async def list_series(self):
            return [KavitaSeries(id=2, name="Example", library_id=1)]

        async def series_detail(self, _series_id: int):
            self.detail_calls += 1
            if self.detail_calls < 3:
                return []
            return [KavitaChapter(id=4, number="1", volume_id=3)]

    class Context:
        checks = 0

        def ensure_lease(self) -> None:
            self.checks += 1

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr("manga_manager.application.kavita_sync.asyncio.sleep", no_sleep)
    snapshot = KavitaSnapshot(
        series_id=1,
        title="Example",
        existing_kavita_id=None,
        folder_path=tmp_path,
        tracked=True,
        aliases=(),
        external_ids={},
        cover_urls=(),
        cover_checksum="",
        kavita_cover_checksum="",
        chapter_cover_checksums={},
        expected_chapters=("1",),
    )
    client = DelayedClient()
    context = Context()
    handler = KavitaSyncHandler(
        session_factory=lambda: None,  # type: ignore[arg-type]
        library_root=tmp_path,
    )

    match, chapters = await handler._await_scan(
        client, snapshot, "", context  # type: ignore[arg-type]
    )

    assert match is not None and match.id == 2
    assert [chapter.number for chapter in chapters] == ["1"]
    assert client.detail_calls == 3
    assert context.checks == 3


def test_kavita_match_ignores_stale_numeric_id_and_uses_current_identity(
    tmp_path: Path,
) -> None:
    expected_folder = tmp_path / "Manga" / "correct-series"
    snapshot = KavitaSnapshot(
        series_id=414,
        title="I'm a Hero, but the Heroines are Trying to Kill Me",
        existing_kavita_id=1,
        folder_path=expected_folder,
        tracked=True,
        aliases=("I’m a Hero, but the Heroines are Trying to Kill Me",),
        external_ids={},
        cover_urls=(),
        cover_checksum="",
        kavita_cover_checksum="",
        chapter_cover_checksums={},
    )
    candidates = [
        KavitaSeries(id=1, name="A Different Series", folder_path="/library/Manga/wrong"),
        KavitaSeries(
            id=99,
            name="I’m a Hero, but the Heroines are Trying to Kill Me",
            folder_path=str(expected_folder),
        ),
    ]

    match = match_series(snapshot, candidates, str(expected_folder))

    assert match is not None and match.id == 99


@pytest.mark.asyncio
async def test_kavita_scan_does_not_poll_chapters_from_stale_series_id(
    tmp_path: Path,
) -> None:
    expected_folder = tmp_path / "Manga" / "correct-series"

    class CorrectingClient:
        configured = True

        def __init__(self) -> None:
            self.detail_ids: list[int] = []

        async def list_series(self):
            return [
                KavitaSeries(id=1, name="Wrong", folder_path="/library/Manga/wrong"),
                KavitaSeries(id=99, name="Correct", folder_path=str(expected_folder)),
            ]

        async def series_detail(self, series_id: int):
            self.detail_ids.append(series_id)
            return [KavitaChapter(id=101, number="1")]

    class Context:
        def ensure_lease(self) -> None:
            return None

    snapshot = KavitaSnapshot(
        series_id=414,
        title="Correct",
        existing_kavita_id=1,
        folder_path=expected_folder,
        tracked=True,
        aliases=(),
        external_ids={},
        cover_urls=(),
        cover_checksum="",
        kavita_cover_checksum="",
        chapter_cover_checksums={},
        expected_chapters=("1",),
    )
    client = CorrectingClient()
    handler = KavitaSyncHandler(
        session_factory=lambda: None,  # type: ignore[arg-type]
        library_root=tmp_path,
    )

    match, chapters = await handler._await_scan(
        client, snapshot, str(expected_folder), Context()  # type: ignore[arg-type]
    )

    assert match is not None and match.id == 99
    assert [chapter.number for chapter in chapters] == ["1"]
    assert client.detail_ids == [99]


def test_exhausted_kavita_match_clears_remote_mapping_for_rescheduling(
    tmp_path: Path,
) -> None:
    engine = create_engine("sqlite://")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    storage = ContentAddressedStorage(
        tmp_path / "storage-v2",
        max_page_bytes=1024 * 1024,
        max_chapter_bytes=10 * 1024 * 1024,
        max_pages=100,
        min_free_bytes=0,
    )
    archive = tmp_path / "chapter.cbz"
    make_cbz(archive)
    LegacyCbzImporter(session_factory=sessions, storage=storage).import_file(archive, dry_run=False)
    with sessions() as session, session.begin():
        series = session.scalar(select(CatalogSeries))
        chapter = session.scalar(select(CatalogChapter))
        assert series is not None and chapter is not None
        series.kavita_series_id = 1
        series.kavita_library_id = 2
        series.kavita_synced_at = datetime.now(timezone.utc)
        series.kavita_cover_checksum = "old-cover"
        chapter.kavita_chapter_id = 3
        chapter.kavita_volume_id = 4
        chapter.kavita_mapped_at = datetime.now(timezone.utc)
        chapter.kavita_cover_checksum = "old-cover"
        series_id = series.id

    handler = KavitaSyncHandler(session_factory=sessions, library_root=storage.kavita_root)
    handler._invalidate_kavita_mapping(series_id, expected_kavita_id=1)

    with sessions() as session:
        series = session.get(CatalogSeries, series_id)
        chapter = session.scalar(select(CatalogChapter))
        assert series is not None and chapter is not None
        assert series.kavita_series_id is None
        assert series.kavita_synced_at is None
        assert series.kavita_cover_checksum == ""
        assert chapter.kavita_chapter_id is None
        assert chapter.kavita_mapped_at is None
        assert chapter.kavita_cover_checksum == ""
