from __future__ import annotations

import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from starlette.requests import Request
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import app.db as db
import app.main as main
import app.scheduler as scheduler_module
import app.services as services
from app.db import Base
from app.domain import ChapterItem, SeriesItem
from app.models import DownloadJob, KavitaSyncJob, SourceHealth
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
    release_columns = {column["name"] for column in inspector.get_columns("chapter_release")}
    job_columns = {column["name"] for column in inspector.get_columns("download_job")}
    assert {"cover_path", "external_ids"} <= series_columns
    assert {"cover_path", "external_ids", "detail_fetched_at"} <= source_columns
    assert "first_seen_at" in release_columns
    assert "retry_after" in job_columns
    assert "uq_download_job_chapter_release" in {
        index["name"] for index in inspector.get_indexes("download_job")
    }
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM download_job")).scalar_one() == 1
        assert connection.execute(text("SELECT id FROM download_job")).scalar_one() == 2


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

    db.run_migrations()

    inspector = inspect(engine)
    assert "uq_download_job_chapter_release" in {
        index["name"] for index in inspector.get_indexes("download_job")
    }
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM download_job")).scalar_one() == 1
        assert connection.execute(text("SELECT id FROM download_job")).scalar_one() == 2
        assert (
            connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            == "f2a7c9d4e1b3"
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
        response = main.retry_failed(session)
        download_job = session.query(DownloadJob).one()
        sync_job = session.query(KavitaSyncJob).one()
    finally:
        session.close()

    assert response.status_code == 303
    assert download_job.status == "queued"
    assert download_job.attempts == 0
    assert sync_job.status == "queued"
    assert sync_job.attempts == 0


def test_library_shows_retry_job_metadata():
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
        response = main.library(make_request("/library"), session)
        html = response.body.decode()
    finally:
        session.close()

    assert "Retry failed jobs" in html
    assert "Today" in html
    assert "Operations" in html
    assert "Run download drain" in html
    assert 'action="/download-jobs/1/retry"' in html
    assert "attempts 2" in html
    assert "attempts 1" in html
    assert "retry" in html


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
        response = await main.rescan_source_detail(10, session)
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
        response = await main.rescan_source_detail(10, session)
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
        download_response = main.retry_download(1, session)
        sync_response = main.retry_kavita_sync(1, session)
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

    assert 'action="/poll/mangafire"' in html
    assert 'action="/poll/asura"' not in html
    assert 'action="/poll/kingofshojo"' not in html
    assert "removed from config" in html
    assert 'action="/sources/asura/' not in html


def test_discovery_uses_update_cards_with_series_and_chapter_links(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = Session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Very Long Example Title", url="x"),
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
    assert f'href="/series/{source_series.series_id}/open"' in html
    assert f'href="/chapters/{release.chapter_id}/open"' in html
    assert "Ch. 12" in html


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
        response = await main.run_kavita_sync(session)
    finally:
        session.close()

    assert response.status_code == 303
    assert response.headers["location"] == "/library"


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
