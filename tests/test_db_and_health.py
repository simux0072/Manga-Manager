from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from starlette.requests import Request
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from starlette.responses import PlainTextResponse

import app.db as db
import app.main as main
import app.scheduler as scheduler_module
import app.services as services
from app.db import Base
from app.domain import ChapterItem, SeriesItem
from app.models import ChapterProgress, DownloadJob, KavitaSyncJob, SourceHealth, SourcePullJob
from app.services import merge_series_item, upsert_release


def make_request(path: str = "/"):
    parsed = urlsplit(path)
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": parsed.path or "/",
            "query_string": parsed.query.encode(),
            "headers": [],
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "app": main.app,
            "router": main.app.router,
        }
    )


def make_request_with_headers(path: str, headers: dict[str, str]):
    request = make_request(path)
    request.scope["headers"] = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in headers.items()
    ]
    return request


def patch_sqlite_engine(monkeypatch, path):
    database_url = f"sqlite:///{path}"
    engine = create_engine(database_url, connect_args={"check_same_thread": False})
    monkeypatch.setattr(db, "engine", engine)
    monkeypatch.setattr(db.settings, "database_url", database_url)
    return engine


def test_non_sqlite_existing_unstamped_schema_raises():
    with pytest.raises(RuntimeError, match="Refusing to stamp"):
        db.ensure_compatible_unversioned_schema("postgresql", {"series"})


def test_fresh_sqlite_migrations_create_version_and_unique_job_constraint(tmp_path, monkeypatch):
    engine = patch_sqlite_engine(monkeypatch, tmp_path / "fresh.db")

    db.run_migrations()

    inspector = inspect(engine)
    assert "alembic_version" in inspector.get_table_names()
    assert "uq_download_job_chapter_release" in {
        constraint["name"] for constraint in inspector.get_unique_constraints("download_job")
    }


def test_existing_sqlite_compat_migration_adds_columns_and_unique_index(tmp_path, monkeypatch):
    engine = patch_sqlite_engine(monkeypatch, tmp_path / "legacy.db")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE series (id INTEGER PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE source_series (id INTEGER PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE source_health (source VARCHAR(50) PRIMARY KEY)"))
        connection.execute(
            text("CREATE TABLE chapter_release (id INTEGER PRIMARY KEY, published_at DATETIME)")
        )
        connection.execute(
            text(
                "CREATE TABLE download_job ("
                "id INTEGER PRIMARY KEY, "
                "chapter_release_id INTEGER, "
                "status VARCHAR(30), "
                "created_at DATETIME, "
                "updated_at DATETIME"
                ")"
            )
        )
        connection.execute(
            text(
                "INSERT INTO download_job "
                "(id, chapter_release_id, status, created_at, updated_at) VALUES "
                "(1, 10, 'queued', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), "
                "(2, 10, 'delayed', '2026-01-02 00:00:00', '2026-01-02 00:00:00')"
            )
        )

    db.run_migrations()

    inspector = inspect(engine)
    series_columns = {column["name"] for column in inspector.get_columns("series")}
    source_columns = {column["name"] for column in inspector.get_columns("source_series")}
    health_columns = {column["name"] for column in inspector.get_columns("source_health")}
    release_columns = {column["name"] for column in inspector.get_columns("chapter_release")}
    job_columns = {column["name"] for column in inspector.get_columns("download_job")}
    assert {"cover_path", "external_ids"} <= series_columns
    assert {"cover_path", "external_ids", "detail_fetched_at", "metadata_json"} <= source_columns
    assert {"download_cooldown_until", "download_cooldown_reason"} <= health_columns
    assert "first_seen_at" in release_columns
    assert "retry_after" in job_columns
    assert "uq_download_job_chapter_release" in {
        index["name"] for index in inspector.get_indexes("download_job")
    }
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM download_job")).scalar_one() == 1
        assert connection.execute(text("SELECT id FROM download_job")).scalar_one() == 2


def test_existing_sqlite_compat_migration_deduplicates_active_pull_jobs(tmp_path, monkeypatch):
    engine = patch_sqlite_engine(monkeypatch, tmp_path / "legacy_pull.db")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE series (id INTEGER PRIMARY KEY)"))
        connection.execute(text("CREATE TABLE source_series (id INTEGER PRIMARY KEY)"))
        connection.execute(
            text("CREATE TABLE chapter_release (id INTEGER PRIMARY KEY, published_at DATETIME)")
        )
        connection.execute(
            text(
                "CREATE TABLE download_job ("
                "id INTEGER PRIMARY KEY, "
                "chapter_release_id INTEGER, "
                "status VARCHAR(30), "
                "created_at DATETIME, "
                "updated_at DATETIME"
                ")"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE source_pull_job ("
                "id INTEGER PRIMARY KEY, "
                "source VARCHAR(50) NOT NULL, "
                "status VARCHAR(30) NOT NULL, "
                "total_items INTEGER DEFAULT 0 NOT NULL, "
                "processed_items INTEGER DEFAULT 0 NOT NULL, "
                "error TEXT DEFAULT '' NOT NULL, "
                "created_at DATETIME NOT NULL, "
                "updated_at DATETIME NOT NULL, "
                "completed_at DATETIME"
                ")"
            )
        )
        connection.execute(
            text(
                "INSERT INTO source_pull_job "
                "(id, source, status, created_at, updated_at) VALUES "
                "(1, 'asura', 'queued', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), "
                "(2, 'asura', 'running', '2026-01-02 00:00:00', '2026-01-02 00:00:00'), "
                "(3, 'mangafire', 'queued', '2026-01-01 00:00:00', '2026-01-01 00:00:00')"
            )
        )

    db.run_migrations()

    inspector = inspect(engine)
    assert "uq_source_pull_job_active_source" in {
        index["name"] for index in inspector.get_indexes("source_pull_job")
    }
    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT id, source, status, error FROM source_pull_job ORDER BY id")
        ).all()
    assert rows == [
        (1, "asura", "failed", "deduplicated before active pull job constraint"),
        (2, "asura", "running", ""),
        (3, "mangafire", "queued", ""),
    ]


def test_versioned_sqlite_upgrade_adds_download_job_unique_index(tmp_path, monkeypatch):
    engine = patch_sqlite_engine(monkeypatch, tmp_path / "versioned.db")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
        connection.execute(text("INSERT INTO alembic_version (version_num) VALUES ('9d37ef72a2c1')"))
        connection.execute(
            text(
                "CREATE TABLE download_job ("
                "id INTEGER PRIMARY KEY, "
                "chapter_release_id INTEGER, "
                "status VARCHAR(30), "
                "created_at DATETIME, "
                "updated_at DATETIME"
                ")"
            )
        )
        connection.execute(
            text(
                "INSERT INTO download_job "
                "(id, chapter_release_id, status, created_at, updated_at) VALUES "
                "(1, 10, 'queued', '2026-01-02 00:00:00', '2026-01-02 00:00:00'), "
                "(2, 10, 'complete', '2026-01-01 00:00:00', '2026-01-01 00:00:00')"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE source_pull_job ("
                "id INTEGER PRIMARY KEY, "
                "source VARCHAR(50) NOT NULL, "
                "status VARCHAR(30) NOT NULL, "
                "total_items INTEGER DEFAULT 0 NOT NULL, "
                "processed_items INTEGER DEFAULT 0 NOT NULL, "
                "error TEXT DEFAULT '' NOT NULL, "
                "created_at DATETIME NOT NULL, "
                "updated_at DATETIME NOT NULL, "
                "completed_at DATETIME"
                ")"
            )
        )
        connection.execute(
            text(
                "INSERT INTO source_pull_job "
                "(id, source, status, created_at, updated_at) VALUES "
                "(1, 'asura', 'queued', '2026-01-01 00:00:00', '2026-01-01 00:00:00'), "
                "(2, 'asura', 'running', '2026-01-02 00:00:00', '2026-01-02 00:00:00')"
            )
        )

    db.run_migrations()

    inspector = inspect(engine)
    assert "uq_download_job_chapter_release" in {
        index["name"] for index in inspector.get_indexes("download_job")
    }
    assert "uq_source_pull_job_active_source" in {
        index["name"] for index in inspector.get_indexes("source_pull_job")
    }
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM download_job")).scalar_one() == 1
        assert connection.execute(text("SELECT id FROM download_job")).scalar_one() == 2
        assert connection.execute(
            text(
                "SELECT COUNT(*) FROM source_pull_job "
                "WHERE source = 'asura' AND status IN ('queued', 'running')"
            )
        ).scalar_one() == 1
        assert (
            connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            == "c41d7a2f5b80"
        )
        try:
            connection.execute(
                text("INSERT INTO download_job (id, chapter_release_id) VALUES (3, 10)")
            )
        except IntegrityError:
            pass
        else:
            raise AssertionError("expected duplicate download_job insert to fail")


def test_healthz_includes_scheduler_and_source_health(monkeypatch):
    class State:
        scheduler = None
        scheduler_start_scheduled = True
        scheduler_start_error = ""

    class Request:
        class App:
            state = State()

        app = App()

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    session.add(SourceHealth(source="asura", enabled=False, last_error="down", consecutive_failures=5))
    session.add(DownloadJob(chapter_release_id=1, status="queued"))
    session.add(DownloadJob(chapter_release_id=2, status="failed"))
    session.add(DownloadJob(chapter_release_id=3, status="failed"))
    session.add(DownloadJob(chapter_release_id=4, status="skipped"))
    session.commit()

    monkeypatch.setattr(main, "enabled_source_names", lambda: ["asura"])
    try:
        payload = main.healthz(Request(), session)
    finally:
        session.close()

    assert payload["scheduler"]["start_scheduled"]
    assert payload["enabled_sources"] == ["asura"]
    assert payload["download_jobs"] == {"queued": 1, "failed": 2, "skipped": 1}
    assert payload["sources"] == [
        {
            "source": "asura",
            "enabled": False,
            "last_poll_at": None,
            "last_error": "down",
            "consecutive_failures": 5,
        }
    ]


def test_csrf_input_is_empty_without_basic_auth(monkeypatch):
    monkeypatch.setattr(main.settings, "basic_auth_username", "")
    monkeypatch.setattr(main.settings, "basic_auth_password", "")

    assert main.csrf_input() == ""


def test_csrf_input_renders_when_basic_auth_is_configured(monkeypatch):
    monkeypatch.setattr(main.settings, "basic_auth_username", "user")
    monkeypatch.setattr(main.settings, "basic_auth_password", "pass")

    token = main.csrf_token()

    assert token
    assert f'value="{token}"' in main.csrf_input()


def test_healthz_includes_kavita_configuration_and_pending_sync(monkeypatch, tmp_path):
    class FakeKavitaClient:
        configured = True

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    source_series.series.status = "interested"
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    chapter = session.get(main.Chapter, release.chapter_id)
    chapter.downloaded_source = "mangafire"
    chapter.cbz_path = str(tmp_path / "library" / "Manga" / "Example" / "Example Ch. 1.cbz")
    session.commit()
    monkeypatch.setattr(main, "enabled_source_names", lambda: [])
    monkeypatch.setattr(main, "configured_kavita_client", lambda: FakeKavitaClient())

    try:
        payload = main.healthz(make_request("/healthz"), session)
    finally:
        session.close()

    assert payload["kavita"] == {"configured": True, "pending_sync_series": 1}


def test_retry_failed_requeues_download_and_kavita_sync_jobs():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.add(DownloadJob(chapter_release_id=1, status="failed", attempts=3, error="download"))
    session.add(
        KavitaSyncJob(
            series_id=source_series.series_id,
            status="failed",
            attempts=3,
            error="sync",
        )
    )
    session.commit()

    try:
        response = main.retry_failed(make_request("/retry-failed"), session)
        download_job = session.query(DownloadJob).one()
        sync_job = session.query(KavitaSyncJob).one()
    finally:
        session.close()

    assert response.status_code == 303
    assert download_job.status == "queued"
    assert download_job.attempts == 0
    assert sync_job.status == "queued"
    assert sync_job.attempts == 0


def test_info_shows_retry_job_metadata_and_library_does_not():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    retry_after = datetime.now(timezone.utc)
    session.add(DownloadJob(chapter_release_id=1, status="failed", attempts=2, error="download"))
    session.add(
        KavitaSyncJob(
            series_id=source_series.series_id,
            status="queued",
            attempts=1,
            error="sync",
            retry_after=retry_after,
        )
    )
    session.commit()

    try:
        library_html = main.library(make_request("/library"), session).body.decode()
        info_html = main.info(make_request("/info"), session).body.decode()
    finally:
        session.close()

    assert "Retry failed jobs" not in library_html
    assert "Today" in library_html
    assert "Operations" not in library_html
    assert "Retry failed jobs" in info_html
    assert "Operations" in info_html
    assert "Run download drain" in info_html
    assert 'data-jobs-status="active"' in info_html
    assert 'data-jobs-status="failed"' in info_html
    assert 'data-jobs-status="completed"' in info_html


def test_library_renders_structured_notice_and_highlights():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    source_series.series.status = "reading"
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    chapter = session.get(main.Chapter, release.chapter_id)
    chapter.downloaded_source = "mangafire"
    source_series.series.kavita_series_id = 7
    source_series.series.kavita_library_id = 2
    session.commit()

    try:
        response = main.library(make_request("/library?notice=download-drain&count=2"), session)
        html = response.body.decode()
    finally:
        session.close()

    assert "Download drain processed 2 jobs." in html
    assert "Kavita-mapped series" in html
    assert "chapters on disk" in html
    assert "Pick something" in html
    assert f'href="/series/{source_series.series_id}/open"' in html
    assert "Rescan mangafire chapters" in html
    assert '<details class="library-details">' in html
    assert 'class="chapter-list latest-chapters"' in html
    assert 'data-source-row="mangafire"' in html


def test_library_pick_something_shows_pending_sync_for_downloaded_unmapped_series():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Downloaded Example", url="x"),
    )
    source_series.series.status = "reading"
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    chapter = session.get(main.Chapter, release.chapter_id)
    chapter.downloaded_source = "mangafire"
    session.commit()

    try:
        response = main.library(make_request("/library"), session)
        html = response.body.decode()
    finally:
        session.close()

    assert "Downloaded, awaiting Kavita sync" in html
    assert f'href="/series/{source_series.series_id}/open"' not in html


def test_library_pick_something_is_not_actionable_without_ready_series():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Queued Example", url="x"),
    )
    source_series.series.status = "reading"
    upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.commit()

    try:
        response = main.library(make_request("/library"), session)
        html = response.body.decode()
    finally:
        session.close()

    assert "Nothing ready yet" in html
    assert f'href="/series/{source_series.series_id}/open"' not in html


def test_new_chapters_lists_unread_downloaded_chapters():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Readable Example", url="x"),
    )
    source_series.series.status = "reading"
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    chapter = session.get(main.Chapter, release.chapter_id)
    chapter.downloaded_source = "mangafire"
    session.add(ChapterProgress(chapter_id=chapter.id, status="unread"))
    session.commit()

    try:
        html = main.new_chapters(make_request("/new-chapters"), session).body.decode()
    finally:
        session.close()

    assert "Readable Example" in html
    assert "Ch. 12" in html
    assert f'href="/chapters/{chapter.id}/open"' in html


async def test_rescan_source_detail_reports_failures(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()

    async def fail_rescan(active_session, source_series_id):
        assert active_session is session
        assert source_series_id == 10
        raise RuntimeError("source is disabled or unavailable")

    monkeypatch.setattr(main, "rescan_source_series", fail_rescan)

    try:
        response = await main.rescan_source_detail(10, make_request("/source-series/10/rescan"), session)
    finally:
        session.close()

    assert response.status_code == 303
    assert (
        response.headers["location"]
        == "/library?notice=rescan-error&error=source+is+disabled+or+unavailable"
    )


async def test_rescan_source_detail_reports_adapter_failures(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()

    async def fail_rescan(active_session, source_series_id):
        assert active_session is session
        assert source_series_id == 10
        raise ValueError("adapter parse failed")

    monkeypatch.setattr(main, "rescan_source_series", fail_rescan)

    try:
        response = await main.rescan_source_detail(10, make_request("/source-series/10/rescan"), session)
    finally:
        session.close()

    assert response.status_code == 303
    assert response.headers["location"] == "/library?notice=rescan-error&error=adapter+parse+failed"


def test_per_job_retry_routes_requeue_failed_jobs():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.add(DownloadJob(chapter_release_id=1, status="failed", attempts=3, error="download"))
    session.add(
        KavitaSyncJob(
            series_id=source_series.series_id,
            status="failed",
            attempts=3,
            error="sync",
        )
    )
    session.commit()

    try:
        download_response = main.retry_download(1, make_request("/download-jobs/1/retry"), session)
        sync_response = main.retry_kavita_sync(1, make_request("/kavita-sync-jobs/1/retry"), session)
        download_job = session.query(DownloadJob).filter_by(id=1).one()
        sync_job = session.query(KavitaSyncJob).filter_by(id=1).one()
    finally:
        session.close()

    assert download_response.status_code == 303
    assert sync_response.status_code == 303
    assert download_job.status == "queued"
    assert download_job.attempts == 0
    assert sync_job.status == "queued"
    assert sync_job.attempts == 0


def test_healthz_reports_scheduler_job_inspection_failure(monkeypatch):
    class Scheduler:
        running = True

        def get_jobs(self):
            raise RuntimeError("job store unavailable")

    class State:
        scheduler = Scheduler()
        scheduler_create_error = ""
        scheduler_start_scheduled = True
        scheduler_start_error = ""

    class Request:
        class App:
            state = State()

        app = App()

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    monkeypatch.setattr(main, "enabled_source_names", lambda: [])
    try:
        payload = main.healthz(Request(), session)
    finally:
        session.close()

    assert not payload["ok"]
    assert payload["scheduler"]["jobs"] == 0
    assert payload["scheduler"]["jobs_error"] == "job store unavailable"


def test_discovery_poll_buttons_follow_configured_sources(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    session.add(SourceHealth(source="asura", enabled=False, last_error="removed from config"))
    session.commit()
    monkeypatch.setattr(main, "enabled_source_names", lambda: ["mangafire"])
    try:
        response = main.discovery(make_request(), session)
        html = response.body.decode()
    finally:
        session.close()

    assert 'action="/pull/mangafire"' in html
    assert 'action="/poll/asura"' not in html
    assert 'action="/poll/kingofshojo"' not in html
    assert "removed from config" in html
    assert 'action="/sources/asura/' not in html


def test_api_pull_status_reports_active_and_latest_jobs():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    session.add(
        SourcePullJob(
            source="mangafire",
            status="running",
            total_items=5,
            processed_items=2,
        )
    )
    session.commit()
    try:
        payload = main.api_pull_status(session)
    finally:
        session.close()

    assert payload["active"]
    assert payload["status"] == "running"
    assert payload["processed"] == 2
    assert payload["total"] == 5


def test_api_pull_status_reports_completed_and_failed_latest_jobs():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    session.add(SourcePullJob(source="mangafire", status="complete", total_items=3, processed_items=3))
    session.add(SourcePullJob(source="asura", status="failed", error="down"))
    session.commit()
    try:
        payload = main.api_pull_status(session)
    finally:
        session.close()

    assert not payload["active"]
    assert payload["status"] == "failed"
    assert payload["error"] == "down"


def test_api_jobs_status_includes_all_job_types():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    second_release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "13", "Chapter 13", "x2", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id, status="failed", attempts=2, error="bad page"))
    session.add(DownloadJob(chapter_release_id=second_release.id, status="complete"))
    session.add(KavitaSyncJob(series_id=source_series.series_id, status="queued"))
    session.add(SourcePullJob(source="mangafire", status="running", total_items=10, processed_items=4))
    session.commit()
    try:
        payload = main.api_jobs_status(session)
    finally:
        session.close()

    assert payload["overall"]["active"]
    assert payload["overall"]["processed"] == 4
    assert payload["overall"]["total"] == 10
    assert payload["downloads"][0]["kind"] == "download_series"
    assert payload["downloads"][0]["retryable"]
    assert payload["downloads"][0]["series_title"] == "Example"
    assert payload["downloads"][0]["processed"] == 2
    assert payload["downloads"][0]["total"] == 2
    assert payload["downloads"][0]["counts"]["failed"] == 1
    assert payload["downloads"][0]["counts"]["complete"] == 1
    assert {chapter["chapter_number"] for chapter in payload["downloads"][0]["chapters"]} == {"12", "13"}
    assert payload["kavita"][0]["status"] == "queued"
    assert payload["kavita"][0]["series_title"] == "Example"
    assert payload["pulls"][0]["source"] == "mangafire"


async def test_jobs_events_returns_sse_stream():
    response = await main.api_jobs_events(make_request("/api/jobs/events"))

    assert response.media_type == "text/event-stream"


def test_retry_download_returns_json_when_requested():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    session.add(DownloadJob(chapter_release_id=1, status="failed", attempts=2, error="bad page"))
    session.commit()
    try:
        response = main.retry_download(1, make_request_with_headers("/download-jobs/1/retry", {"accept": "application/json"}), session)
        payload = response.body.decode()
        job = session.query(DownloadJob).one()
    finally:
        session.close()

    assert response.status_code == 200
    assert '"ok":true' in payload
    assert job.status == "queued"


async def test_pull_all_schedules_limited_runner(monkeypatch):
    class Job:
        def __init__(self, job_id):
            self.id = job_id

    ran_with = []
    scheduled = []

    def fake_create_pull_job(session, source):
        return Job({"asura": 10, "mangafire": 20}[source]), True

    async def fake_run_pull_jobs_limited(job_ids):
        ran_with.append(job_ids)

    def fake_log_background_task(coroutine, description):
        scheduled.append(description)
        return asyncio.create_task(coroutine)

    monkeypatch.setattr(main, "enabled_source_names", lambda: ["asura", "mangafire"])
    monkeypatch.setattr(main, "create_pull_job", fake_create_pull_job)
    monkeypatch.setattr(main, "run_pull_jobs_limited", fake_run_pull_jobs_limited)
    monkeypatch.setattr(main, "log_background_task", fake_log_background_task)

    response = await main.pull_all(make_request(), object())
    await asyncio.sleep(0)

    assert response.status_code == 303
    assert response.headers["location"] == "/?pending=pull-all"
    assert scheduled == ["pull-all job_ids=[10, 20]"]
    assert ran_with == [[10, 20]]


async def test_request_logging_middleware_logs_success(monkeypatch):
    records = []

    def fake_info(message, *args):
        records.append(message % args)

    async def call_next(request):
        return PlainTextResponse("ok", status_code=204)

    monkeypatch.setattr(main.logger, "info", fake_info)
    response = await main.request_logging_middleware(make_request("/healthz"), call_next)

    assert response.status_code == 204
    assert any("request complete method=GET path=/healthz status=204" in record for record in records)


async def test_request_logging_middleware_logs_exception(monkeypatch):
    records = []

    def fake_exception(message, *args):
        records.append(message % args)

    async def call_next(request):
        raise RuntimeError("boom")

    monkeypatch.setattr(main.logger, "exception", fake_exception)
    with pytest.raises(RuntimeError, match="boom"):
        await main.request_logging_middleware(make_request("/explode"), call_next)

    assert any("request failed method=GET path=/explode" in record for record in records)


def test_discovery_uses_update_cards_with_series_and_chapter_links(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Very Long Example Title",
            url="x",
            aliases=("Alt Example",),
            description="A useful description for the update card.",
            genres=("Action", "Drama"),
            metadata={"follows": 123, "rating": 9.1},
        ),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    session.commit()
    monkeypatch.setattr(main, "enabled_source_names", lambda: ["mangafire"])
    try:
        response = main.discovery(make_request(), session)
        html = response.body.decode()
    finally:
        session.close()

    assert "update-card" in html
    assert "update-cover-link" in html
    assert "update-content" in html
    assert 'data-sources="mangafire"' in html
    assert 'data-source-toggle value="mangafire"' in html
    assert html.index("update-cover-link") < html.index("update-content") < html.index("update-chapters")
    content_start = html.index('<div class="update-content">')
    content_end = html.index("</div>", html.index("</div>", html.index("update-chapters")) + 1)
    assert html.index("update-summary") > content_start
    assert html.index("update-chapters") > content_start
    assert html.index("update-chapters") < content_end
    assert f'href="/series/{source_series.series_id}/open"' in html
    assert f'href="/chapters/{release.chapter_id}/open"' in html
    assert 'data-source-row="mangafire"' in html
    assert "A useful description" in html
    assert "Alt Example" in html
    assert "rating 9.1" in html
    assert "123 bookmarks" in html
    assert "Ch. 12" in html


def test_discovery_card_css_keeps_cover_left_and_content_right():
    css = Path("app/static/styles.css").read_text()

    assert 'grid-template-areas: "cover content";' in css
    assert "grid-template-columns: minmax(170px, 44%) minmax(0, 1fr);" in css
    assert "grid-area: cover;" in css
    assert "grid-area: content;" in css


def test_static_stylesheet_uses_layout_cache_buster():
    html = Path("app/templates/base.html").read_text()

    assert "styles.css') }}?v=dynamic-1" in html
    assert "app.js') }}?v=dynamic-1" in html


def test_app_js_has_source_filters_and_jobs_drawer():
    script = Path("app/static/app.js").read_text()

    assert "manga-manager.hiddenSources" in script
    assert "/api/jobs/status" in script
    assert "/api/jobs/events" in script
    assert "setupAsyncForms" in script
    assert "EventSource" in script
    assert "download_series" in script
    assert "drawerSections" in script
    assert "statusCounts" in script
    assert 'renderSection("Queued"' in script
    assert 'renderSection("Completed"' in script
    assert 'renderSection("Failed"' in script
    assert ".slice(0, 10)" in script
    assert "data-jobs-status" in script
    assert "data-source-row" in script
    assert ".update-card .sources [data-source]" in script
    assert "showChapters: job.kind === \"download_series\"" in script
    assert "toast-stack" in Path("app/templates/base.html").read_text()


def test_discovery_preview_shows_latest_chapter_numbers_first(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    published_at = datetime(2026, 7, 8, tzinfo=timezone.utc)
    for number in ["1", "2", "3", "126", "127", "128"]:
        upsert_release(
            session,
            source_series,
            ChapterItem("mangafire", source_series.source_id, number, f"Chapter {number}", "x", published_at),
        )
    session.commit()
    monkeypatch.setattr(main, "enabled_source_names", lambda: ["mangafire"])
    try:
        html = main.discovery(make_request(), session).body.decode()
    finally:
        session.close()

    assert html.index("Ch. 128") < html.index("Ch. 127") < html.index("Ch. 126")
    assert "Ch. 3" not in html


def test_discovery_card_order_uses_platform_release_date_not_priority_duplicate(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    old_time = datetime(2026, 7, 6, tzinfo=timezone.utc)
    new_time = old_time + timedelta(days=1)
    newer_time = old_time + timedelta(days=2)
    first = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="first-mf", title="First Series", url="mf"),
    )
    first_high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="first-asura", title="First Series", url="asura"),
    )
    second = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="second-mf", title="Second Series", url="second"),
    )
    upsert_release(
        session,
        first,
        ChapterItem("mangafire", first.source_id, "10", "Chapter 10", "mf", old_time),
    )
    upsert_release(
        session,
        first_high,
        ChapterItem("asura", first_high.source_id, "10", "Chapter 10", "asura", newer_time),
    )
    upsert_release(
        session,
        second,
        ChapterItem("mangafire", second.source_id, "5", "Chapter 5", "second", new_time),
    )
    session.commit()
    monkeypatch.setattr(main, "enabled_source_names", lambda: ["mangafire", "asura"])
    try:
        best_source = session.query(main.Chapter).filter_by(series_id=first.series_id, number="10").one().best_source
        html = main.discovery(make_request(), session).body.decode()
    finally:
        session.close()

    assert best_source == "asura"
    assert html.index("Second Series") < html.index("First Series")
    first_section = html[html.index("First Series") : html.index("Ch. 10") + 120]
    assert f"asura · {main.relative_time(old_time)}" in first_section
    assert "asura · now" not in first_section


async def test_open_unmapped_series_queues_import_without_inline_download(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    session.commit()

    async def fail_run_next_download(session):
        raise AssertionError("run_next_download should not run inline")

    monkeypatch.setattr(main, "run_next_download", fail_run_next_download)
    try:
        response = await main.open_series(source_series.series_id, session)
    finally:
        session.close()

    assert response.status_code == 303
    assert response.headers["location"] == "/?pending=kavita-import"


async def test_open_unmapped_chapter_queues_import_without_inline_download(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    session.commit()

    async def fail_run_next_download(session):
        raise AssertionError("run_next_download should not run inline")

    monkeypatch.setattr(main, "run_next_download", fail_run_next_download)
    try:
        response = await main.open_chapter(release.chapter_id, session)
    finally:
        session.close()

    assert response.status_code == 303
    assert response.headers["location"] == "/?pending=kavita-chapter"


async def test_open_downloaded_unmapped_chapter_queues_kavita_sync(monkeypatch, tmp_path):
    class FakeKavitaClient:
        configured = True

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    chapter = session.get(main.Chapter, release.chapter_id)
    cbz_path = tmp_path / "library" / "Manga" / "Example" / "Example Ch. 12.cbz"
    chapter.downloaded_source = "mangafire"
    chapter.cbz_path = str(cbz_path)
    session.commit()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())

    try:
        response = await main.open_chapter(release.chapter_id, session)
        sync_job = session.query(KavitaSyncJob).one()
    finally:
        session.close()

    assert response.status_code == 303
    assert response.headers["location"] == "/?pending=kavita-chapter"
    assert sync_job.series_id == source_series.series_id
    assert sync_job.folder_path == str(cbz_path.parent)


async def test_run_kavita_sync_queues_pending_before_running(monkeypatch, tmp_path):
    class FakeKavitaClient:
        configured = True

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    source_series.series.status = "interested"
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    chapter = session.get(main.Chapter, release.chapter_id)
    chapter.downloaded_source = "mangafire"
    chapter.cbz_path = str(tmp_path / "library" / "Manga" / "Example" / "Example Ch. 12.cbz")
    session.commit()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())

    async def assert_pending_job_exists(active_session):
        assert active_session.query(KavitaSyncJob).one().series_id == source_series.series_id
        return False

    monkeypatch.setattr(main, "run_next_kavita_sync", assert_pending_job_exists)
    try:
        response = await main.run_kavita_sync(make_request("/run-kavita-sync"), session)
    finally:
        session.close()

    assert response.status_code == 303
    assert response.headers["location"] == "/info"


def test_env_example_does_not_override_compose_database_url():
    active_env_lines = [
        line.strip()
        for line in Path(".env.example").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert not any(line.startswith("DATABASE_URL=") for line in active_env_lines)


def test_docker_compose_keeps_postgres_database_url_fallback():
    compose_text = Path("docker-compose.yml").read_text()

    assert (
        "DATABASE_URL: ${DATABASE_URL:-postgresql+psycopg://manga:manga@postgres:5432/"
        "manga_manager}" in compose_text
    )


def test_enable_source_resets_disabled_health(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    session.add(
        SourceHealth(
            source="mangafire",
            enabled=False,
            last_error="all 2 discovered items failed",
            consecutive_failures=5,
        )
    )
    session.commit()
    monkeypatch.setattr(main, "enabled_source_names", lambda: ["mangafire"])

    response = main.enable_source("mangafire", session)

    health = session.get(SourceHealth, "mangafire")
    assert response.status_code == 303
    assert health.enabled
    assert health.last_error == ""
    assert health.consecutive_failures == 0


def test_enable_source_ignores_unconfigured_source(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    monkeypatch.setattr(main, "enabled_source_names", lambda: ["mangafire"])

    response = main.enable_source("unknown", session)

    assert response.status_code == 303
    assert session.get(SourceHealth, "unknown") is None


def test_disable_source_sets_configured_source_disabled(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    monkeypatch.setattr(main, "enabled_source_names", lambda: ["mangafire"])

    response = main.disable_source("mangafire", session)

    health = session.get(SourceHealth, "mangafire")
    assert response.status_code == 303
    assert not health.enabled
    assert health.last_error == "manually disabled"
    assert health.consecutive_failures == 0


def test_disable_source_ignores_unconfigured_source(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    monkeypatch.setattr(main, "enabled_source_names", lambda: ["mangafire"])

    response = main.disable_source("unknown", session)

    assert response.status_code == 303
    assert session.get(SourceHealth, "unknown") is None


async def test_lifespan_schedules_scheduler_start(monkeypatch):
    class FakeScheduler:
        def __init__(self):
            self.running = False

        def start(self):
            self.running = True

        def shutdown(self, wait=False):
            self.running = False

        def get_jobs(self):
            return [object()]

    @contextmanager
    def fake_session():
        yield object()

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()

    def get_test_session():
        yield session

    recovered = []

    monkeypatch.setattr(main, "init_db", lambda: None)
    monkeypatch.setattr(main, "recover_stale_download_jobs", lambda session: recovered.append("download"))
    monkeypatch.setattr(
        main, "recover_stale_kavita_sync_jobs", lambda session: recovered.append("kavita")
    )
    monkeypatch.setattr(main, "SessionLocal", fake_session)
    monkeypatch.setattr(main, "create_scheduler", FakeScheduler)
    monkeypatch.setattr(main, "enabled_source_names", lambda: [])
    main.app.dependency_overrides[main.get_session] = get_test_session
    try:
        async with main.lifespan(main.app):
            await asyncio.sleep(0)
            payload = main.healthz(type("Request", (), {"app": main.app})(), session)
    finally:
        main.app.dependency_overrides.clear()
        session.close()

    assert payload["scheduler"]["configured"]
    assert payload["scheduler"]["start_scheduled"]
    assert payload["scheduler"]["create_error"] == ""
    assert payload["scheduler"]["jobs"] == 1
    assert recovered == ["download", "kavita"]


async def test_lifespan_reports_scheduler_creation_failure(monkeypatch):
    @contextmanager
    def fake_session():
        yield object()

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()

    def fail_create_scheduler():
        raise RuntimeError("scheduler unavailable")

    recovered = []

    monkeypatch.setattr(main, "init_db", lambda: None)
    monkeypatch.setattr(main, "recover_stale_download_jobs", lambda session: recovered.append("download"))
    monkeypatch.setattr(
        main, "recover_stale_kavita_sync_jobs", lambda session: recovered.append("kavita")
    )
    monkeypatch.setattr(main, "SessionLocal", fake_session)
    monkeypatch.setattr(main, "create_scheduler", fail_create_scheduler)
    monkeypatch.setattr(main, "enabled_source_names", lambda: [])
    try:
        async with main.lifespan(main.app):
            payload = main.healthz(type("Request", (), {"app": main.app})(), session)
    finally:
        session.close()

    assert not payload["ok"]
    assert not payload["scheduler"]["configured"]
    assert not payload["scheduler"]["start_scheduled"]
    assert payload["scheduler"]["create_error"] == "scheduler unavailable"
    assert recovered == ["download", "kavita"]


async def test_scheduler_uses_non_overlapping_coalesced_jobs(monkeypatch):
    monkeypatch.setattr(scheduler_module, "enabled_source_names", lambda: [])

    scheduler = scheduler_module.create_scheduler()

    assert scheduler._job_defaults["coalesce"] is True
    assert scheduler._job_defaults["max_instances"] == 1
    assert len(scheduler.get_jobs()) == 2


async def test_scheduler_source_poll_uses_pull_job(monkeypatch):
    calls = []
    session = object()

    class Job:
        id = 42

    class FakeSessionLocal:
        def __enter__(self):
            return session

        def __exit__(self, exc_type, exc, traceback):
            return False

    def fake_create_pull_job(active_session, source):
        assert active_session is session
        calls.append(("create", source))
        return Job(), True

    async def fake_run_pull_job(job_id):
        calls.append(("run", job_id))

    monkeypatch.setattr(scheduler_module, "enabled_source_names", lambda: ["mangafire"])
    monkeypatch.setattr(scheduler_module, "SessionLocal", FakeSessionLocal)
    monkeypatch.setattr(scheduler_module, "create_pull_job", fake_create_pull_job)
    monkeypatch.setattr(scheduler_module, "run_pull_job", fake_run_pull_job)

    scheduler = scheduler_module.create_scheduler()
    poll_job = next(job for job in scheduler.get_jobs() if job.args == ("mangafire",))

    await poll_job.func(*poll_job.args)

    assert calls == [("create", "mangafire"), ("run", 42)]


async def test_scheduler_kavita_drain_queues_pending_before_running(monkeypatch):
    calls = []
    session = object()

    class FakeSessionLocal:
        def __enter__(self):
            return session

        def __exit__(self, exc_type, exc, traceback):
            return False

    def fake_queue_pending(active_session):
        assert active_session is session
        calls.append("queue")

    async def fake_run_next(active_session):
        assert active_session is session
        calls.append("run")
        return False

    monkeypatch.setattr(scheduler_module, "enabled_source_names", lambda: [])
    monkeypatch.setattr(scheduler_module, "SessionLocal", FakeSessionLocal)
    monkeypatch.setattr(scheduler_module, "queue_pending_kavita_syncs", fake_queue_pending)
    monkeypatch.setattr(scheduler_module, "run_next_kavita_sync", fake_run_next)

    scheduler = scheduler_module.create_scheduler()
    kavita_job = next(
        job for job in scheduler.get_jobs() if job.func.__name__ == "drain_kavita_sync"
    )

    await kavita_job.func()

    assert calls == ["queue", "run"]
