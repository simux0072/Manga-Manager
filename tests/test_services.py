import asyncio
import io
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from PIL import Image
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

import app.services as services
import app.db as db
from app.adapters.base import ChapterTemporarilyUnavailable
from app.kavita import KavitaChapter, KavitaReadProgress, KavitaSeries
from app.db import Base
from app.domain import ChapterItem, SeriesItem
from app.models import (
    ActivityEvent,
    Chapter,
    ChapterProgress,
    ChapterRelease,
    DownloadJob,
    DownloadedFile,
    KavitaSyncJob,
    MatchCandidate,
    Series,
    SeriesProgress,
    SourceHealth,
    SourcePullJob,
    SourceSeries,
)
from app.services import (
    deactivate_downloaded_files,
    drain_download_queue,
    merge_match_candidate,
    merge_series_item,
    poll_all_sources,
    queue_downloads,
    cleanup_replaced_files,
    recover_stale_download_jobs,
    rescan_source_series,
    retry_failed_downloads,
    mark_series_caught_up,
    set_chapter_progress,
    set_series_progress,
    sync_kavita_want_to_read,
    upsert_release,
)


def make_session():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)()


def make_file_session_factory(path):
    engine = create_engine(f"sqlite:///{path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def test_commit_with_retry_rolls_back_and_retries_sqlite_lock(monkeypatch):
    class Session:
        commits = 0
        rollbacks = 0

        def commit(self):
            self.commits += 1
            if self.commits == 1:
                raise OperationalError("commit", {}, Exception("database is locked"))

        def rollback(self):
            self.rollbacks += 1

    session = Session()
    monkeypatch.setattr(services.time, "sleep", lambda delay: None)

    services.commit_with_retry(session)

    assert session.commits == 2
    assert session.rollbacks == 1


def image_bytes(image_format: str = "PNG") -> bytes:
    buffer = io.BytesIO()
    Image.new("RGB", (1, 1), color="white").save(buffer, format=image_format)
    return buffer.getvalue()


def test_merge_series_auto_matches_exact_normalized_title():
    session = make_session()
    first = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="manga/example", title="Example Manga", url="x"),
    )
    session.commit()
    second = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="series/example", title="Example", url="y"),
    )
    session.commit()

    assert first.series_id == second.series_id
    assert session.query(Series).count() == 1


def test_upsert_release_sets_best_source_by_priority():
    session = make_session()
    low = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="manga/example", title="Example", url="x"),
    )
    high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="series/example", title="Example", url="y"),
    )
    session.commit()

    upsert_release(
        session,
        low,
        ChapterItem("kingofshojo", low.source_id, "1", "Chapter 1", "x", datetime.now(timezone.utc)),
    )
    upsert_release(
        session,
        high,
        ChapterItem("asura", high.source_id, "1", "Chapter 1", "y", datetime.now(timezone.utc)),
    )
    session.commit()

    chapter = session.query(Chapter).one()
    assert chapter.best_source == "asura"


def test_queue_downloads_only_for_tracked_best_source():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    source_series.series.status = "interested"
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.commit()

    assert queue_downloads(session) == 1
    assert release.chapter.best_source == "mangafire"


def test_chapter_sort_key_orders_numeric_chapters_descending_with_decimals():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    for number in ["127", "127.5", "128", "special"]:
        upsert_release(
            session,
            source_series,
            ChapterItem("mangafire", source_series.source_id, number, f"Chapter {number}", "x", None),
        )
    session.commit()

    ordered = sorted(source_series.series.chapters, key=services.chapter_sort_key, reverse=True)

    assert [chapter.number for chapter in ordered] == ["128", "127.5", "127", "special"]


def test_platform_release_time_keeps_original_date_when_priority_source_arrives_later():
    session = make_session()
    original_time = datetime(2026, 7, 6, tzinfo=timezone.utc)
    higher_priority_time = datetime(2026, 7, 8, tzinfo=timezone.utc)
    low = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="series/example", title="Example", url="y"),
    )
    low_release = upsert_release(
        session,
        low,
        ChapterItem("mangafire", low.source_id, "10", "Chapter 10", "x", original_time),
    )
    upsert_release(
        session,
        high,
        ChapterItem("asura", high.source_id, "10", "Chapter 10", "y", higher_priority_time),
    )
    session.commit()

    chapter = session.get(Chapter, low_release.chapter_id)

    assert chapter.best_source == "asura"
    assert services.chapter_platform_release_time(chapter) == original_time


def test_upsert_release_keeps_earliest_published_at_for_same_source_repoll():
    session = make_session()
    original_time = datetime(2026, 7, 6, tzinfo=timezone.utc)
    later_time = datetime(2026, 7, 8, tzinfo=timezone.utc)
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "10", "Chapter 10", "x", original_time),
    )
    session.commit()

    same_release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "10", "Chapter 10", "y", later_time),
    )
    session.commit()

    assert same_release.id == release.id
    assert same_release.published_at == original_time
    assert services.chapter_platform_release_time(same_release.chapter) == original_time


def test_upsert_release_fills_missing_published_at_for_same_source_repoll():
    session = make_session()
    published_at = datetime(2026, 7, 8, tzinfo=timezone.utc)
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "10", "Chapter 10", "x", None),
    )
    session.commit()
    assert release.published_at is None

    same_release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "10", "Chapter 10", "y", published_at),
    )
    session.commit()

    assert same_release.id == release.id
    assert same_release.published_at == published_at


def test_create_pull_job_reuses_active_source_job():
    session = make_session()

    first, first_created = services.create_pull_job(session, "mangafire")
    second, second_created = services.create_pull_job(session, "mangafire")

    assert first_created
    assert not second_created
    assert second.id == first.id
    assert session.query(SourcePullJob).count() == 1

    first.status = "complete"
    session.commit()

    third, third_created = services.create_pull_job(session, "mangafire")
    assert third_created
    assert third.id != first.id
    assert session.query(SourcePullJob).count() == 2


async def test_run_pull_job_updates_progress_without_sqlite_lock(tmp_path, monkeypatch):
    class Adapter:
        async def list_recent(self):
            return [
                SeriesItem(source="mangafire", source_id="good", title="Good", url="good"),
                SeriesItem(source="mangafire", source_id="bad", title="Bad", url="bad"),
            ]

        async def get_chapters(self, item):
            if item.source_id == "bad":
                raise RuntimeError("detail failed")
            return [ChapterItem("mangafire", item.source_id, "1", "Chapter 1", item.url, None)]

        async def aclose(self):
            return None

    Session = make_file_session_factory(tmp_path / "pull.db")
    monkeypatch.setattr(db, "SessionLocal", Session)
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())
    monkeypatch.setattr(services, "enabled_source_names", lambda: ["mangafire"])
    monkeypatch.setattr(services.settings, "downloads_enabled", False)
    with Session() as session:
        job, created = services.create_pull_job(session, "mangafire")
        assert created
        job_id = job.id

    await services.run_pull_job(job_id)

    with Session() as session:
        job = session.get(SourcePullJob, job_id)
        assert job.status == "complete"
        assert job.processed_items == 2
        assert job.total_items == 2
        assert job.error == ""


async def test_run_pull_jobs_limited_respects_source_pull_concurrency(monkeypatch):
    running = 0
    max_running = 0
    calls = []

    async def fake_run_pull_job(job_id):
        nonlocal running, max_running
        calls.append(job_id)
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0)
        running -= 1

    monkeypatch.setattr(services.settings, "source_pull_concurrency", 1)
    monkeypatch.setattr(services, "run_pull_job", fake_run_pull_job)

    await services.run_pull_jobs_limited([1, 2, 3])

    assert calls == [1, 2, 3]
    assert max_running == 1


def test_recover_stale_pull_jobs_marks_old_active_jobs_failed(monkeypatch):
    session = make_session()
    monkeypatch.setattr(services.settings, "download_stale_minutes", 60)
    stale = SourcePullJob(
        source="mangafire",
        status="running",
        updated_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    fresh = SourcePullJob(source="asura", status="running", updated_at=datetime.now(timezone.utc))
    session.add_all([stale, fresh])
    session.commit()

    assert services.recover_stale_pull_jobs(session) == 1
    assert stale.status == "failed"
    assert stale.error == "recovered stale pull job"
    assert fresh.status == "running"


def test_progress_helpers_record_series_and_chapter_activity():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.commit()

    set_series_progress(session, source_series.series_id, "reading")
    set_chapter_progress(session, release.chapter_id, "read")

    assert session.query(SeriesProgress).one().status == "reading"
    assert session.query(ChapterProgress).one().status == "read"
    assert session.query(ActivityEvent).filter_by(kind="chapter_progress").count() == 1


def test_mark_series_caught_up_commits_once_and_marks_chapters():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "2", "Chapter 2", "x", None),
    )
    session.commit()

    assert mark_series_caught_up(session, source_series.series_id) == 2

    assert session.query(SeriesProgress).one().status == "caught_up"
    assert {row.status for row in session.query(ChapterProgress).all()} == {"read"}


def test_cleanup_replaced_files_removes_old_inactive_download(tmp_path, monkeypatch):
    session = make_session()
    path = tmp_path / "old.cbz"
    path.write_text("old")
    session.add(
        DownloadedFile(
            chapter_id=1,
            chapter_release_id=1,
            source="mangafire",
            path=str(path),
            active=False,
            replaced_at=datetime.now(timezone.utc) - timedelta(days=10),
        )
    )
    session.commit()
    monkeypatch.setattr(services.settings, "retention_replaced_days", 1)
    monkeypatch.setattr(services.settings, "retention_replaced_max_per_chapter", 0)

    assert cleanup_replaced_files(session) == 1
    assert not path.exists()
    assert session.query(DownloadedFile).count() == 0


async def test_sync_kavita_want_to_read_marks_mapped_series(monkeypatch):
    class FakeClient:
        configured = True

        async def want_to_read(self):
            return [KavitaSeries(id=42, name="Example")]

    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    source_series.series.kavita_series_id = 42
    session.commit()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeClient())

    assert await sync_kavita_want_to_read(session) == 1
    assert session.query(SeriesProgress).one().status == "interested"


def test_record_activity_ignores_notification_failures(monkeypatch):
    session = make_session()

    def fail_delivery(event):
        raise RuntimeError("webhook down")

    monkeypatch.setattr(services.settings, "notification_webhook_url", "https://example.invalid")
    monkeypatch.setattr(services.httpx, "Client", lambda timeout: fail_delivery)

    event = services.record_activity(session, "download", "success", "done")
    session.commit()

    assert event.created_at is not None
    assert session.query(ActivityEvent).one().message == "done"


def test_queue_downloads_falls_back_after_best_source_max_attempts(monkeypatch):
    session = make_session()
    low = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="series/example", title="Example", url="y"),
    )
    high.series.status = "interested"
    upsert_release(
        session,
        low,
        ChapterItem("mangafire", low.source_id, "1", "Chapter 1", "x", None),
    )
    high_release = upsert_release(
        session,
        high,
        ChapterItem("asura", high.source_id, "1", "Chapter 1", "y", None),
    )
    session.flush()
    session.add(
        DownloadJob(
            chapter_release_id=high_release.id,
            status="failed",
            attempts=services.settings.max_download_attempts,
        )
    )
    session.commit()

    assert queue_downloads(session) == 1


def test_queue_downloads_waits_for_temporarily_delayed_best_source():
    session = make_session()
    low = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="manga/example", title="Example", url="x"),
    )
    high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="series/example", title="Example", url="y"),
    )
    high.series.status = "interested"
    upsert_release(
        session,
        low,
        ChapterItem("kingofshojo", low.source_id, "1", "Chapter 1", "x", None),
    )
    upsert_release(
        session,
        high,
        ChapterItem("asura", high.source_id, "1", "Chapter 1", "y", None),
    )
    session.commit()

    assert queue_downloads(session) == 0
    assert session.query(DownloadJob).count() == 0


async def test_run_next_download_completes_fallback_after_best_source_max_attempts(
    tmp_path, monkeypatch
):
    class FallbackAdapter:
        async def download_chapter_pages(self, chapter):
            return [image_bytes(), image_bytes(), image_bytes()]

    async def noop_sync(session, series, folder_path=None):
        return True

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "staging_root", tmp_path / "staging")
    monkeypatch.setattr(services.settings, "archive_root", tmp_path / "archive")
    monkeypatch.setattr(services, "adapter_for_source", lambda source: FallbackAdapter())
    monkeypatch.setattr(services, "sync_series_with_kavita", noop_sync)
    low = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="manga/example", title="Example", url="x"),
    )
    high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="series/example", title="Example", url="y"),
    )
    high.series.status = "interested"
    low_release = upsert_release(
        session,
        low,
        ChapterItem("mangafire", low.source_id, "1", "Chapter 1", "x", None),
    )
    high_release = upsert_release(
        session,
        high,
        ChapterItem("asura", high.source_id, "1", "Chapter 1", "y", None),
    )
    session.flush()
    session.add(
        DownloadJob(
            chapter_release_id=high_release.id,
            status="failed",
            attempts=services.settings.max_download_attempts,
        )
    )
    session.add(DownloadJob(chapter_release_id=low_release.id))
    session.commit()

    assert await services.run_next_download(session)

    fallback_job = session.query(DownloadJob).filter_by(chapter_release_id=low_release.id).one()
    best_job = session.query(DownloadJob).filter_by(chapter_release_id=high_release.id).one()
    assert fallback_job.status == "complete"
    assert best_job.status == "failed"
    assert low_release.downloaded
    assert low_release.chapter.downloaded_source == "mangafire"
    assert Path(low_release.chapter.cbz_path).exists()


def test_queue_downloads_requeues_skipped_fallback_after_best_source_exhausts():
    session = make_session()
    low = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="manga/example", title="Example", url="x"),
    )
    high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="series/example", title="Example", url="y"),
    )
    high.series.status = "interested"
    low_release = upsert_release(
        session,
        low,
        ChapterItem("kingofshojo", low.source_id, "1", "Chapter 1", "x", None),
    )
    high_release = upsert_release(
        session,
        high,
        ChapterItem("asura", high.source_id, "1", "Chapter 1", "y", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=low_release.id, status="skipped", attempts=1))
    session.add(
        DownloadJob(
            chapter_release_id=high_release.id,
            status="failed",
            attempts=services.settings.max_download_attempts,
        )
    )
    session.commit()

    assert queue_downloads(session) == 1

    job = session.query(DownloadJob).filter_by(chapter_release_id=low_release.id).one()
    assert job.status == "queued"
    assert job.error == ""


def test_asura_download_delay_uses_first_seen_when_publish_time_missing():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="series/example", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("asura", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.commit()

    assert release.first_seen_at is not None
    assert release.downloadable_after > release.first_seen_at


def test_retry_failed_downloads_resets_attempts():
    session = make_session()
    job = DownloadJob(chapter_release_id=1, status="failed", attempts=3, error="no images")
    session.add(job)
    session.commit()

    assert retry_failed_downloads(session) == 1
    assert job.status == "queued"
    assert job.attempts == 0
    assert job.error == ""
    assert job.retry_after is None


def test_retry_failed_downloads_does_not_retry_skipped_job():
    session = make_session()
    session.add(DownloadJob(chapter_release_id=1, status="skipped", attempts=1, error="obsolete"))
    session.commit()

    assert retry_failed_downloads(session) == 0
    job = session.query(DownloadJob).one()
    assert job.status == "skipped"
    assert job.error == "obsolete"


def test_deactivate_downloaded_files_marks_replaced():
    session = make_session()
    downloaded = DownloadedFile(
        chapter_id=1,
        chapter_release_id=1,
        source="kingofshojo",
        path="/tmp/example.cbz",
        checksum="abc",
        image_count=10,
        active=True,
    )
    session.add(downloaded)
    session.commit()

    deactivate_downloaded_files(session, 1)
    session.commit()

    assert not downloaded.active
    assert downloaded.replaced_at is not None


def test_queue_downloads_does_not_duplicate_failed_job():
    session = make_session()
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
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id, status="failed", attempts=3))
    session.commit()

    assert queue_downloads(session) == 0
    assert session.query(DownloadJob).count() == 1


def test_queue_downloads_duplicate_attempts_create_one_job():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    source_series.series.status = "interested"
    upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.commit()

    assert queue_downloads(session) == 1
    assert queue_downloads(session) == 0
    assert session.query(DownloadJob).count() == 1


def test_queue_downloads_handles_reloaded_sqlite_delayed_release(tmp_path):
    Session = make_file_session_factory(tmp_path / "delayed.db")
    setup = Session()
    source_series = merge_series_item(
        setup,
        SeriesItem(source="asura", source_id="comics/example", title="Example", url="x"),
    )
    source_series.series.status = "interested"
    release = upsert_release(
        setup,
        source_series,
        ChapterItem("asura", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    release.downloadable_after = datetime.now(timezone.utc) + timedelta(days=1)
    setup.commit()
    setup.close()

    session = Session()
    assert queue_downloads(session) == 0
    assert session.query(DownloadJob).count() == 0


def test_claim_next_download_job_prevents_stale_double_claim(tmp_path):
    Session = make_file_session_factory(tmp_path / "jobs.db")
    setup = Session()
    source_series = merge_series_item(
        setup,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        setup,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    setup.flush()
    setup.add(DownloadJob(chapter_release_id=release.id))
    setup.commit()
    job_id = setup.query(DownloadJob.id).scalar()
    setup.close()

    first = Session()
    stale = Session()
    stale.get(DownloadJob, job_id)

    assert services.claim_next_download_job(first).id == job_id
    assert services.claim_next_download_job(stale) is None


async def test_run_next_download_marks_temporary_unavailable_as_delayed(monkeypatch):
    class DelayedAdapter:
        async def download_chapter_pages(self, chapter):
            raise ChapterTemporarilyUnavailable("premium", datetime(2030, 1, 1, tzinfo=timezone.utc))

    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="comics/example", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("asura", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    release.downloadable_after = None
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()
    monkeypatch.setattr(services, "adapter_for_source", lambda source: DelayedAdapter())

    assert await services.run_next_download(session)

    job = session.query(DownloadJob).one()
    assert job.status == "delayed"
    assert job.error == "premium"
    assert services.ensure_aware(job.retry_after) == datetime(2030, 1, 1, tzinfo=timezone.utc)
    assert services.ensure_aware(release.downloadable_after) == datetime(
        2030, 1, 1, tzinfo=timezone.utc
    )


async def test_run_next_download_skips_obsolete_best_source(monkeypatch):
    session = make_session()
    low = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="manga/example", title="Example", url="x"),
    )
    low.series.status = "interested"
    low_release = upsert_release(
        session,
        low,
        ChapterItem("kingofshojo", low.source_id, "1", "Chapter 1", "x", None),
    )
    session.commit()
    assert queue_downloads(session) == 1

    high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="comics/example", title="Example", url="y"),
    )
    upsert_release(
        session,
        high,
        ChapterItem("asura", high.source_id, "1", "Chapter 1", "y", None),
    )
    session.commit()
    monkeypatch.setattr(
        services,
        "adapter_for_source",
        lambda source: (_ for _ in ()).throw(AssertionError("adapter should not be used")),
    )

    assert await services.run_next_download(session)

    job = session.query(DownloadJob).one()
    assert job.chapter_release_id == low_release.id
    assert job.status == "skipped"
    assert "no longer best source" in job.error


async def test_run_next_download_delays_job_when_release_not_due(monkeypatch):
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="comics/example", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("asura", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    release.downloadable_after = datetime.now(timezone.utc) + timedelta(days=1)
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()
    monkeypatch.setattr(
        services,
        "adapter_for_source",
        lambda source: (_ for _ in ()).throw(AssertionError("adapter should not be used")),
    )

    assert await services.run_next_download(session)

    job = session.query(DownloadJob).one()
    assert job.status == "delayed"
    assert job.retry_after is not None
    assert job.error == "release is not downloadable yet"


async def test_run_next_download_skips_when_downloaded_source_is_better(monkeypatch):
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    release.chapter.best_source = "mangafire"
    release.chapter.downloaded_source = "asura"
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()
    monkeypatch.setattr(
        services,
        "adapter_for_source",
        lambda source: (_ for _ in ()).throw(AssertionError("adapter should not be used")),
    )

    assert await services.run_next_download(session)

    job = session.query(DownloadJob).one()
    assert job.status == "skipped"
    assert "not replaceable" in job.error


async def test_run_next_download_fails_cleanly_for_disabled_source(monkeypatch):
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()
    monkeypatch.setattr(services, "adapter_for_source", lambda source: None)

    assert await services.run_next_download(session)

    job = session.query(DownloadJob).one()
    assert job.status == "failed"
    assert "disabled or unavailable" in job.error


async def test_run_next_download_archives_old_file_before_replacement(tmp_path, monkeypatch):
    class ReplacementAdapter:
        async def download_chapter_pages(self, chapter):
            return [image_bytes(), image_bytes(), image_bytes()]

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "staging_root", tmp_path / "staging")
    monkeypatch.setattr(services.settings, "archive_root", tmp_path / "archive")
    monkeypatch.setattr(services.settings, "keep_replaced_files", True)
    monkeypatch.setattr(services, "adapter_for_source", lambda source: ReplacementAdapter())
    monkeypatch.setattr(services, "trigger_kavita_scan", noop_scan)

    source_series = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="comics/example", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("asura", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    release.downloadable_after = None
    chapter = session.get(Chapter, release.chapter_id)
    old_path = tmp_path / "library" / "Manga" / "Example" / "Example Ch. 1.cbz"
    old_path.parent.mkdir(parents=True)
    old_path.write_bytes(b"old cbz")
    chapter.cbz_path = str(old_path)
    session.add(
        DownloadedFile(
            chapter_id=chapter.id,
            chapter_release_id=release.id,
            source="kingofshojo",
            path=str(old_path),
            checksum="old",
            image_count=3,
            active=True,
        )
    )
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()

    assert await services.run_next_download(session)

    archived = list((tmp_path / "archive" / "replaced").glob("*.cbz"))
    assert len(archived) == 1
    assert archived[0].read_bytes() == b"old cbz"
    assert session.query(DownloadJob).one().status == "complete"
    assert not session.query(DownloadedFile).filter_by(source="kingofshojo").one().active


async def test_run_next_download_streams_pages_to_cbz(tmp_path, monkeypatch):
    class StreamingAdapter:
        async def iter_chapter_pages(self, chapter):
            for page in [image_bytes(), image_bytes(), image_bytes()]:
                yield page

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "staging_root", tmp_path / "staging")
    monkeypatch.setattr(services, "adapter_for_source", lambda source: StreamingAdapter())
    monkeypatch.setattr(services, "trigger_kavita_scan", noop_scan)
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()

    assert await services.run_next_download(session)

    downloaded = session.query(DownloadedFile).one()
    assert downloaded.image_count == 3
    assert session.query(DownloadJob).one().status == "complete"
    with zipfile.ZipFile(downloaded.path) as cbz:
        assert "0001.png" in cbz.namelist()


async def test_stage_cbz_uses_job_and_release_in_staging_name(tmp_path, monkeypatch):
    async def pages():
        for page in [image_bytes(), image_bytes(), image_bytes()]:
            yield page

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "staging_root", tmp_path / "staging")
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    job = DownloadJob(chapter_release_id=release.id)
    session.add(job)
    session.flush()

    staging_path, final_path, image_count = await services.stage_cbz(job, release, pages())

    assert f"job-{job.id}" in staging_path.name
    assert f"release-{release.id}" in staging_path.name
    assert staging_path.parent == final_path.parent
    assert final_path.name == "Example Ch. 1.cbz"
    assert image_count == 3
    staging_path.unlink()


async def test_run_next_download_rejects_streamed_too_few_pages(tmp_path, monkeypatch):
    class ShortAdapter:
        async def iter_chapter_pages(self, chapter):
            yield image_bytes()
            yield image_bytes()

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "staging_root", tmp_path / "staging")
    monkeypatch.setattr(services, "adapter_for_source", lambda source: ShortAdapter())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()

    assert await services.run_next_download(session)

    job = session.query(DownloadJob).one()
    assert job.status == "queued"
    assert "only 2 chapter images" in job.error
    assert not list((tmp_path / "library").rglob("*.tmp"))


async def test_run_next_download_rejects_corrupt_page_bytes(tmp_path, monkeypatch):
    class CorruptAdapter:
        async def iter_chapter_pages(self, chapter):
            yield b"not an image"

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "max_download_attempts", 1)
    monkeypatch.setattr(services, "adapter_for_source", lambda source: CorruptAdapter())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()
    records = []

    def fake_info(message, *args, **kwargs):
        records.append(message % args)

    def fake_warning(message, *args, **kwargs):
        records.append(message % args)

    monkeypatch.setattr(services.logger, "info", fake_info)
    monkeypatch.setattr(services.logger, "warning", fake_warning)

    assert await services.run_next_download(session)

    job = session.query(DownloadJob).one()
    assert job.status == "failed"
    assert "invalid image page" in job.error
    assert any(f"download job started job_id={job.id}" in record for record in records)
    assert any(f"download job failed job_id={job.id}" in record for record in records)
    assert not list((tmp_path / "library").rglob("*.cbz"))
    assert not list((tmp_path / "library").rglob("*.tmp"))


async def test_kavita_scan_failure_keeps_download_complete(tmp_path, monkeypatch):
    class Adapter:
        async def download_chapter_pages(self, chapter):
            return [image_bytes(), image_bytes(), image_bytes()]

    async def failing_sync(session, series, folder_path=None):
        raise RuntimeError("scan unavailable")

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "staging_root", tmp_path / "staging")
    monkeypatch.setattr(services.settings, "archive_root", tmp_path / "archive")
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())
    monkeypatch.setattr(services, "sync_series_with_kavita", failing_sync)

    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()

    assert await services.run_next_download(session)

    job = session.query(DownloadJob).one()
    assert job.status == "complete"
    assert job.error == ""
    assert release.chapter.downloaded_source == "mangafire"


async def test_successful_download_queues_kavita_sync_job(tmp_path, monkeypatch):
    class Adapter:
        async def download_chapter_pages(self, chapter):
            return [image_bytes(), image_bytes(), image_bytes()]

    class FakeKavitaClient:
        configured = True

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "staging_root", tmp_path / "staging")
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()

    assert await services.run_next_download(session)

    sync_job = session.query(KavitaSyncJob).one()
    assert sync_job.series_id == source_series.series_id
    assert sync_job.status == "queued"
    assert sync_job.folder_path.endswith("Manga/Example")


async def test_run_next_kavita_sync_maps_series(monkeypatch):
    class FakeKavitaClient:
        configured = True

        async def scan_folder_or_all(self, folder_path):
            return None

        async def list_series(self):
            return [KavitaSeries(id=7, name="Example", library_id=2)]

        async def series_detail(self, series_id):
            return [KavitaChapter(id=42, number="1", volume_id=3)]

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.commit()
    assert services.queue_kavita_sync(session, source_series.series)

    assert await services.run_next_kavita_sync(session)

    sync_job = session.query(KavitaSyncJob).one()
    chapter = session.get(Chapter, release.chapter_id)
    assert sync_job.status == "complete"
    assert source_series.series.kavita_series_id == 7
    assert chapter.kavita_chapter_id == 42


async def test_sync_series_with_kavita_imports_read_progress(monkeypatch):
    class FakeKavitaClient:
        configured = True

        async def list_series(self):
            return [KavitaSeries(id=7, name="Example", library_id=2)]

        async def series_detail(self, series_id):
            return [KavitaChapter(id=42, number="1", volume_id=3, pages_total=12)]

        async def chapter_progress(self, chapter_id, pages_total=0):
            return KavitaReadProgress(chapter_id=chapter_id, pages_read=12, pages_total=pages_total)

    session = make_session()
    monkeypatch.setattr(services.settings, "kavita_sync_read_progress", True)
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.commit()

    assert await services.sync_series_with_kavita(session, source_series.series)

    progress = session.query(ChapterProgress).filter_by(chapter_id=release.chapter_id).one()
    assert progress.status == "read"
    assert progress.read_at is not None


async def test_run_next_kavita_sync_retries_no_match(monkeypatch):
    class FakeKavitaClient:
        configured = True

        async def scan_folder_or_all(self, folder_path):
            return None

        async def list_series(self):
            return []

        async def series_detail(self, series_id):
            return []

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.commit()
    assert services.queue_kavita_sync(session, source_series.series)

    assert await services.run_next_kavita_sync(session)

    sync_job = session.query(KavitaSyncJob).one()
    assert sync_job.status == "queued"
    assert sync_job.retry_after is not None
    assert "not found" in sync_job.error


async def test_run_next_kavita_sync_leaves_queued_job_when_unconfigured(monkeypatch):
    class FakeKavitaClient:
        configured = False

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.add(KavitaSyncJob(series_id=source_series.series_id, status="queued"))
    session.commit()

    assert not await services.run_next_kavita_sync(session)

    sync_job = session.query(KavitaSyncJob).one()
    assert sync_job.status == "queued"
    assert sync_job.attempts == 0
    assert sync_job.error == ""


def test_queue_pending_kavita_syncs_finds_tracked_downloaded_unmapped(monkeypatch, tmp_path):
    class FakeKavitaClient:
        configured = True

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    tracked = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="tracked", title="Tracked", url="x"),
    )
    tracked.series.status = "interested"
    tracked_release = upsert_release(
        session,
        tracked,
        ChapterItem("mangafire", tracked.source_id, "1", "Chapter 1", "x", None),
    )
    tracked_chapter = session.get(Chapter, tracked_release.chapter_id)
    tracked_chapter.downloaded_source = "mangafire"
    tracked_chapter.cbz_path = str(tmp_path / "library" / "Manga" / "Tracked" / "Tracked Ch. 1.cbz")
    untracked = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="untracked", title="Untracked", url="x"),
    )
    untracked_release = upsert_release(
        session,
        untracked,
        ChapterItem("mangafire", untracked.source_id, "1", "Chapter 1", "x", None),
    )
    session.get(Chapter, untracked_release.chapter_id).downloaded_source = "mangafire"
    mapped = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="mapped", title="Mapped", url="x"),
    )
    mapped.series.status = "reading"
    mapped_release = upsert_release(
        session,
        mapped,
        ChapterItem("mangafire", mapped.source_id, "1", "Chapter 1", "x", None),
    )
    mapped_chapter = session.get(Chapter, mapped_release.chapter_id)
    mapped_chapter.downloaded_source = "mangafire"
    mapped_chapter.kavita_chapter_id = 42
    session.commit()

    assert services.queue_pending_kavita_syncs(session) == 1

    sync_job = session.query(KavitaSyncJob).one()
    assert sync_job.series_id == tracked.series_id
    assert sync_job.folder_path == str(Path(tracked_chapter.cbz_path).parent)


def test_queue_pending_kavita_syncs_requires_same_chapter_downloaded_unmapped(monkeypatch):
    class FakeKavitaClient:
        configured = True

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    source_series.series.status = "interested"
    downloaded_release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    downloaded_chapter = session.get(Chapter, downloaded_release.chapter_id)
    downloaded_chapter.downloaded_source = "mangafire"
    downloaded_chapter.kavita_chapter_id = 42
    upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "2", "Chapter 2", "x", None),
    )
    session.commit()

    assert services.queue_pending_kavita_syncs(session) == 0
    assert session.query(KavitaSyncJob).count() == 0


def test_queue_kavita_sync_derives_folder_from_downloaded_chapter(monkeypatch, tmp_path):
    class FakeKavitaClient:
        configured = True

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    chapter = session.get(Chapter, release.chapter_id)
    chapter.downloaded_source = "mangafire"
    chapter.cbz_path = str(tmp_path / "library" / "Manga" / "Example" / "Example Ch. 1.cbz")
    session.commit()

    assert services.queue_kavita_sync(session, source_series.series)

    sync_job = session.query(KavitaSyncJob).one()
    assert sync_job.folder_path == str(Path(chapter.cbz_path).parent)


def test_queue_kavita_sync_resets_failed_job_attempts(monkeypatch):
    class FakeKavitaClient:
        configured = True

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.add(
        KavitaSyncJob(
            series_id=source_series.series_id,
            status="failed",
            attempts=services.settings.max_download_attempts,
            error="old failure",
            retry_after=datetime.now(timezone.utc),
        )
    )
    session.commit()

    assert services.queue_kavita_sync(session, source_series.series)

    sync_job = session.query(KavitaSyncJob).one()
    assert sync_job.status == "queued"
    assert sync_job.attempts == 0
    assert sync_job.error == ""
    assert sync_job.retry_after is None


def test_queue_kavita_sync_updates_completed_job_folder(monkeypatch, tmp_path):
    class FakeKavitaClient:
        configured = True

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.add(
        KavitaSyncJob(
            series_id=source_series.series_id,
            status="complete",
            attempts=2,
            folder_path=str(tmp_path / "old"),
        )
    )
    session.commit()

    assert services.queue_kavita_sync(session, source_series.series, tmp_path / "new")

    sync_job = session.query(KavitaSyncJob).one()
    assert sync_job.status == "queued"
    assert sync_job.attempts == 0
    assert sync_job.folder_path == str(tmp_path / "new")


def test_queue_kavita_sync_handles_duplicate_insert_race(monkeypatch):
    class FakeKavitaClient:
        configured = True

    session = make_session()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.add(KavitaSyncJob(series_id=source_series.series_id, status="queued"))
    session.commit()
    monkeypatch.setattr(session, "scalar", lambda statement: None)

    assert not services.queue_kavita_sync(session, source_series.series)

    assert session.query(KavitaSyncJob).count() == 1


def test_retry_failed_kavita_syncs_resets_attempts():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    sync_job = KavitaSyncJob(
        series_id=source_series.series_id,
        status="failed",
        attempts=3,
        error="no match",
        retry_after=datetime.now(timezone.utc),
    )
    session.add(sync_job)
    session.commit()

    assert services.retry_failed_kavita_syncs(session) == 1

    assert sync_job.status == "queued"
    assert sync_job.attempts == 0
    assert sync_job.error == ""
    assert sync_job.retry_after is None


def test_retry_failed_kavita_syncs_recovers_unconfigured_skips():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    sync_job = KavitaSyncJob(
        series_id=source_series.series_id,
        status="skipped",
        attempts=1,
        error="Kavita is not configured",
    )
    session.add(sync_job)
    session.commit()

    assert services.retry_failed_kavita_syncs(session) == 1

    assert sync_job.status == "queued"
    assert sync_job.attempts == 0
    assert sync_job.error == ""


def test_recover_stale_kavita_sync_jobs_only_recovers_old_running_jobs():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    fresh_source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl4", title="Fresh Example", url="x"),
    )
    stale_job = KavitaSyncJob(
        series_id=source_series.series_id,
        status="running",
        attempts=1,
        error="old",
        updated_at=datetime.now(timezone.utc)
        - timedelta(minutes=services.settings.download_stale_minutes + 1),
    )
    fresh_job = KavitaSyncJob(
        series_id=fresh_source_series.series_id,
        status="running",
        attempts=1,
        error="fresh",
        updated_at=datetime.now(timezone.utc),
    )
    session.add_all([stale_job, fresh_job])
    session.commit()

    assert services.recover_stale_kavita_sync_jobs(session) == 1

    assert stale_job.status == "queued"
    assert stale_job.error == "recovered stale running sync job"
    assert stale_job.retry_after is None
    assert fresh_job.status == "running"
    assert fresh_job.error == "fresh"


async def test_sync_series_with_kavita_clears_missing_stored_series(monkeypatch):
    class FakeKavitaClient:
        configured = True

        async def list_series(self):
            return []

        async def series_detail(self, series_id):
            raise AssertionError("missing series should not fetch details")

    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    source_series.series.kavita_series_id = 99
    source_series.series.kavita_library_id = 2
    chapter = session.get(Chapter, release.chapter_id)
    chapter.kavita_chapter_id = 42
    chapter.kavita_volume_id = 3
    session.commit()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())

    assert not await services.sync_series_with_kavita(session, source_series.series)

    assert source_series.series.kavita_series_id is None
    assert source_series.series.kavita_library_id is None
    assert chapter.kavita_chapter_id is None
    assert chapter.kavita_volume_id is None


async def test_sync_series_with_kavita_clears_missing_chapter_mapping(monkeypatch):
    class FakeKavitaClient:
        configured = True

        async def list_series(self):
            return [KavitaSeries(id=7, name="Example", library_id=2)]

        async def series_detail(self, series_id):
            return [KavitaChapter(id=43, number="13", volume_id=3)]

    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "12", "Chapter 12", "x", None),
    )
    source_series.series.kavita_series_id = 7
    source_series.series.kavita_library_id = 2
    chapter = session.get(Chapter, release.chapter_id)
    chapter.kavita_chapter_id = 42
    chapter.kavita_volume_id = 3
    session.commit()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())

    assert await services.sync_series_with_kavita(session, source_series.series)

    assert source_series.series.kavita_series_id == 7
    assert chapter.kavita_chapter_id is None
    assert chapter.kavita_volume_id is None


async def test_sync_series_with_kavita_matches_explicit_folder_path(monkeypatch, tmp_path):
    old_folder = tmp_path / "library" / "Manga" / "Old Folder"

    class FakeKavitaClient:
        configured = True

        async def scan_folder_or_all(self, folder_path):
            assert folder_path == old_folder

        async def list_series(self):
            return [
                KavitaSeries(
                    id=7,
                    name="Unrelated Kavita Title",
                    library_id=2,
                    folder_path=str(old_folder),
                )
            ]

        async def series_detail(self, series_id):
            assert series_id == 7
            return [KavitaChapter(id=42, number="1", volume_id=3)]

    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Current Title", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    chapter = session.get(Chapter, release.chapter_id)
    chapter.downloaded_source = "mangafire"
    chapter.cbz_path = str(old_folder / "Old Folder Ch. 1.cbz")
    session.commit()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())

    assert await services.sync_series_with_kavita(session, source_series.series, old_folder)

    assert source_series.series.kavita_series_id == 7
    assert source_series.series.kavita_library_id == 2
    assert chapter.kavita_chapter_id == 42
    assert chapter.kavita_volume_id == 3


async def test_sync_series_with_kavita_matches_translated_folder_path(monkeypatch, tmp_path):
    local_folder = tmp_path / "library" / "Manga" / "Example"
    kavita_folder = Path("/kavita-library/Manga/Example")

    class FakeKavitaClient:
        configured = True

        async def scan_folder_or_all(self, folder_path):
            assert folder_path == local_folder

        async def list_series(self):
            return [
                KavitaSeries(
                    id=7,
                    name="Unrelated Kavita Title",
                    library_id=2,
                    folder_path=str(kavita_folder),
                )
            ]

        async def series_detail(self, series_id):
            assert series_id == 7
            return [KavitaChapter(id=42, number="1", volume_id=3)]

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services.settings, "kavita_library_root", Path("/kavita-library"))
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Current Title", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    chapter = session.get(Chapter, release.chapter_id)
    chapter.downloaded_source = "mangafire"
    chapter.cbz_path = str(local_folder / "Example Ch. 1.cbz")
    session.commit()
    monkeypatch.setattr(services, "configured_kavita_client", lambda: FakeKavitaClient())

    assert await services.sync_series_with_kavita(session, source_series.series, local_folder)

    assert source_series.series.kavita_series_id == 7
    assert chapter.kavita_chapter_id == 42


def test_external_ids_auto_match_different_titles():
    session = make_session()
    first = merge_series_item(
        session,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Gun X Clover",
            url="x",
            external_ids={"mal": "39281"},
        ),
    )
    session.commit()

    second = merge_series_item(
        session,
        SeriesItem(
            source="asura",
            source_id="comics/other-title",
            title="Different Localized Title",
            url="y",
            external_ids={"mal": "39281"},
        ),
    )
    session.commit()

    assert first.series_id == second.series_id
    assert session.query(Series).count() == 1


def test_recover_stale_running_jobs_requeues_old_job(monkeypatch):
    session = make_session()
    monkeypatch.setattr(services.settings, "download_stale_minutes", 60)
    job = DownloadJob(
        chapter_release_id=1,
        status="running",
        updated_at=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    session.add(job)
    session.commit()

    assert recover_stale_download_jobs(session) == 1
    assert job.status == "queued"
    assert job.retry_after is None
    assert job.error == "recovered stale running job"


def test_recover_stale_running_jobs_handles_reloaded_sqlite_datetime(tmp_path, monkeypatch):
    Session = make_file_session_factory(tmp_path / "stale.db")
    setup = Session()
    monkeypatch.setattr(services.settings, "download_stale_minutes", 60)
    setup.add(
        DownloadJob(
            chapter_release_id=1,
            status="running",
            updated_at=datetime.now(timezone.utc) - timedelta(hours=2),
        )
    )
    setup.commit()
    setup.close()

    session = Session()
    assert recover_stale_download_jobs(session) == 1
    assert session.query(DownloadJob).one().status == "queued"


async def test_failed_download_uses_backoff_and_is_not_immediately_retried(monkeypatch):
    class FailingAdapter:
        async def download_chapter_pages(self, chapter):
            raise RuntimeError("network down")

    session = make_session()
    monkeypatch.setattr(services.settings, "download_retry_base_minutes", 15)
    monkeypatch.setattr(services, "adapter_for_source", lambda source: FailingAdapter())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("mangafire", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    session.add(DownloadJob(chapter_release_id=release.id))
    session.commit()

    assert await services.run_next_download(session)
    job = session.query(DownloadJob).one()
    assert job.status == "queued"
    assert job.retry_after is not None
    assert job.attempts == 1

    assert not await services.run_next_download(session)
    assert job.attempts == 1


async def test_terminal_download_failure_queues_next_source(monkeypatch):
    class FailingAdapter:
        async def download_chapter_pages(self, chapter):
            raise RuntimeError("terminal failure")

    session = make_session()
    monkeypatch.setattr(services.settings, "max_download_attempts", 1)
    monkeypatch.setattr(services, "adapter_for_source", lambda source: FailingAdapter())
    asura = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="asura/example", title="Example", url="a"),
    )
    mangafire = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="mf/example", title="Example", url="m"),
    )
    king = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="king/example", title="Example", url="k"),
    )
    asura_release = upsert_release(
        session,
        asura,
        ChapterItem("asura", asura.source_id, "1", "Chapter 1", "a", None),
    )
    upsert_release(
        session,
        mangafire,
        ChapterItem("mangafire", mangafire.source_id, "1", "Chapter 1", "m", None),
    )
    king_release = upsert_release(
        session,
        king,
        ChapterItem("kingofshojo", king.source_id, "1", "Chapter 1", "k", None),
    )
    asura_release.downloadable_after = None
    session.add(DownloadJob(chapter_release_id=asura_release.id, priority=10))
    session.commit()

    assert await services.run_next_download(session)

    jobs = {
        session.get(ChapterRelease, job.chapter_release_id).source: job
        for job in session.query(DownloadJob).all()
    }
    assert jobs["asura"].status == "failed"
    assert jobs["mangafire"].status == "queued"
    assert jobs["mangafire"].priority == 11
    assert "kingofshojo" not in jobs
    assert king_release.id


def test_manual_merge_recomputes_best_source_for_duplicate_chapter():
    session = make_session()
    low = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="manga/example", title="Example Low", url="x"),
    )
    high = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="comics/example", title="Example High", url="y"),
    )
    upsert_release(
        session,
        low,
        ChapterItem("kingofshojo", low.source_id, "1", "Chapter 1", "x", None),
    )
    upsert_release(
        session,
        high,
        ChapterItem("asura", high.source_id, "1", "Chapter 1", "y", None),
    )
    candidate = MatchCandidate(source_series_id=high.id, candidate_series_id=low.series_id)
    session.add(candidate)
    session.commit()

    merge_match_candidate(session, candidate.id)

    chapter = session.query(Chapter).one()
    assert chapter.best_source == "asura"
    assert {release.source for release in chapter.releases} == {"kingofshojo", "asura"}
    assert session.query(Series).count() == 1


def test_manual_merge_moves_all_old_source_rows_and_deletes_old_series():
    session = make_session()
    target = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="manga/example", title="Example", url="x"),
    )
    duplicate = merge_series_item(
        session,
        SeriesItem(source="asura", source_id="comics/example", title="Example Duplicate", url="y"),
    )
    extra_duplicate_source = merge_series_item(
        session,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example Duplicate",
            url="z",
            external_ids={"mal": "duplicate-only"},
        ),
    )
    extra_duplicate_source.series_id = duplicate.series_id
    candidate = MatchCandidate(source_series_id=duplicate.id, candidate_series_id=target.series_id)
    session.add(candidate)
    session.commit()

    merge_match_candidate(session, candidate.id)

    assert session.query(Series).count() == 1
    remaining = session.query(Series).one()
    assert {source.source for source in remaining.sources} == {
        "kingofshojo",
        "asura",
        "mangafire",
    }
    assert candidate.status == "merged"


async def test_poll_source_continues_when_one_item_fails(monkeypatch):
    closed = False

    class Adapter:
        async def list_recent(self):
            return [
                SeriesItem(source="mangafire", source_id="good", title="Good", url="good"),
                SeriesItem(source="mangafire", source_id="bad", title="Bad", url="bad"),
            ]

        async def get_chapters(self, item):
            if item.source_id == "bad":
                raise RuntimeError("detail failed")
            return [ChapterItem("mangafire", item.source_id, "1", "Chapter 1", item.url, None)]

        async def aclose(self):
            nonlocal closed
            closed = True

    session = make_session()
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())

    assert await services.poll_source(session, "mangafire") == 1
    assert closed
    assert session.query(Series).count() == 1
    health = session.get(SourceHealth, "mangafire")
    assert health.consecutive_failures == 0
    assert health.enabled
    assert health.last_error == "1 item failures"


async def test_poll_source_reports_total_and_processed_progress(monkeypatch):
    class Adapter:
        async def list_recent(self):
            return [
                SeriesItem(source="mangafire", source_id="good", title="Good", url="good"),
                SeriesItem(source="mangafire", source_id="bad", title="Bad", url="bad"),
            ]

        async def get_chapters(self, item):
            if item.source_id == "bad":
                raise RuntimeError("detail failed")
            return [ChapterItem("mangafire", item.source_id, "1", "Chapter 1", item.url, None)]

    session = make_session()
    progress = []
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())

    assert await services.poll_source(
        session,
        "mangafire",
        progress=lambda processed, total: progress.append((processed, total)),
    ) == 1

    assert progress == [(0, 2), (1, 2), (2, 2)]


async def test_poll_source_skips_mangafire_items_without_english_chapters(monkeypatch):
    class Adapter:
        async def list_recent(self):
            return [
                SeriesItem(source="mangafire", source_id="english", title="English", url="english"),
                SeriesItem(source="mangafire", source_id="no-english", title="No English", url="no-english"),
            ]

        async def get_chapters(self, item):
            if item.source_id == "no-english":
                return []
            return [ChapterItem("mangafire", item.source_id, "0", "Oneshot", item.url, None)]

    session = make_session()
    progress = []
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())

    assert await services.poll_source(
        session,
        "mangafire",
        progress=lambda processed, total: progress.append((processed, total)),
    ) == 1

    assert progress == [(0, 2), (1, 2), (2, 2)]
    assert session.query(Series).filter_by(title="English").one()
    assert session.query(Series).filter_by(title="No English").one_or_none() is None
    health = session.get(SourceHealth, "mangafire")
    assert health.consecutive_failures == 0


async def test_poll_source_failed_item_does_not_leave_dangling_cover_path(tmp_path, monkeypatch):
    calls = 0

    async def download_cover_image(url):
        return image_bytes("JPEG"), "image/jpeg"

    class Adapter:
        async def list_recent(self):
            return [
                SeriesItem(
                    source="mangafire",
                    source_id="gl3",
                    title="Example",
                    url="good",
                    cover_url="https://example.com/old.jpg",
                ),
                SeriesItem(
                    source="mangafire",
                    source_id="gl3",
                    title="Example",
                    url="bad",
                    cover_url="https://example.com/new.jpg",
                ),
            ]

        async def get_chapters(self, item):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("detail failed")
            return [ChapterItem("mangafire", item.source_id, "1", "Chapter 1", item.url, None)]

        async def aclose(self):
            return None

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services, "download_cover_image", download_cover_image)
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())

    assert await services.poll_source(session, "mangafire") == 1

    source_series = session.query(Series).one().sources[0]
    assert source_series.cover_path.endswith(".jpg")
    assert Path(source_series.cover_path).exists()
    assert services.cover_url_hash("https://example.com/old.jpg") in source_series.cover_path
    assert len(list((tmp_path / "library" / "_covers").glob("*.jpg"))) == 1


async def test_poll_source_only_constructs_requested_adapter(monkeypatch):
    requested = []

    class Adapter:
        async def list_recent(self):
            return []

        async def aclose(self):
            return None

    def adapter_for_source(source):
        requested.append(source)
        return Adapter()

    session = make_session()
    monkeypatch.setattr(services, "adapter_for_source", adapter_for_source)

    assert await services.poll_source(session, "mangafire") == 0
    assert requested == ["mangafire"]


async def test_poll_source_disabled_health_does_not_construct_adapter(monkeypatch):
    session = make_session()
    session.add(SourceHealth(source="mangafire", enabled=False, last_error="down"))
    session.commit()
    monkeypatch.setattr(services, "enabled_source_names", lambda: ["mangafire"])
    monkeypatch.setattr(
        services,
        "adapter_for_source",
        lambda source: (_ for _ in ()).throw(AssertionError("adapter should not be used")),
    )

    assert await services.poll_source(session, "mangafire") == 0

    health = session.get(SourceHealth, "mangafire")
    assert not health.enabled
    assert health.last_error == "down"


async def test_poll_source_disables_after_repeated_all_item_failures(monkeypatch):
    class Adapter:
        async def list_recent(self):
            return [
                SeriesItem(source="mangafire", source_id="bad-1", title="Bad 1", url="bad-1"),
                SeriesItem(source="mangafire", source_id="bad-2", title="Bad 2", url="bad-2"),
            ]

        async def get_chapters(self, item):
            raise RuntimeError("detail failed")

    session = make_session()
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())

    for _ in range(5):
        assert await services.poll_source(session, "mangafire") == 0

    health = session.get(SourceHealth, "mangafire")
    assert not health.enabled
    assert health.consecutive_failures == 5
    assert health.last_error == "all 2 discovered items failed"


async def test_poll_all_sources_continues_after_source_failure(monkeypatch):
    class FailingAdapter:
        async def list_recent(self):
            raise RuntimeError("source down")

    class GoodAdapter:
        async def list_recent(self):
            return [SeriesItem(source="mangafire", source_id="good", title="Good", url="good")]

        async def get_chapters(self, item):
            return [ChapterItem("mangafire", item.source_id, "1", "Chapter 1", item.url, None)]

    session = make_session()
    monkeypatch.setattr(services, "enabled_source_names", lambda: ["asura", "mangafire"])
    monkeypatch.setattr(
        services,
        "adapter_for_source",
        lambda source: FailingAdapter() if source == "asura" else GoodAdapter(),
    )

    result = await poll_all_sources(session)

    assert not result["asura"]["ok"]
    assert result["mangafire"]["ok"]
    assert session.query(Series).filter_by(title="Good").one()
    assert session.get(SourceHealth, "asura").last_error == "source down"


def test_cleanup_bad_discovery_rows_removes_empty_pseudo_series():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="text-mode", title="Text Mode", url="x"),
    )
    upsert_release(
        session,
        source_series,
        ChapterItem("kingofshojo", source_series.source_id, "{{ number }}", "{{ title }}", "x", None),
    )
    session.add(MatchCandidate(source_series_id=source_series.id, candidate_series_id=source_series.series_id))
    session.commit()

    result = services.cleanup_bad_discovery_rows(session)

    assert result["chapters"] == 1
    assert result["series"] == 1
    assert session.query(Chapter).count() == 0
    assert session.query(SourceSeries).count() == 0
    assert session.query(Series).count() == 0
    assert session.query(MatchCandidate).count() == 0


def test_cleanup_bad_discovery_rows_keeps_pseudo_series_with_files(tmp_path):
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="kingofshojo", source_id="manhwa", title="Manhwa", url="x"),
    )
    release = upsert_release(
        session,
        source_series,
        ChapterItem("kingofshojo", source_series.source_id, "1", "Chapter 1", "x", None),
    )
    session.flush()
    session.add(
        DownloadedFile(
            chapter_id=release.chapter_id,
            chapter_release_id=release.id,
            source="kingofshojo",
            path=str(tmp_path / "chapter.cbz"),
        )
    )
    session.commit()

    result = services.cleanup_bad_discovery_rows(session)

    assert result["series"] == 0
    assert session.query(Series).count() == 1
    assert session.query(SourceSeries).count() == 1


def test_existing_source_refreshes_parent_metadata():
    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            aliases=("Old Alias",),
            description="Old description",
            cover_url="old.jpg",
            genres=("Action",),
            popularity=1,
            external_ids={"mal": "1"},
        ),
    )
    session.commit()

    merge_series_item(
        session,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="y",
            aliases=("New Alias",),
            description="New description",
            cover_url="new.jpg",
            genres=("Drama",),
            popularity=10,
            external_ids={"anilist": "2"},
        ),
    )
    session.commit()

    series = source_series.series
    assert series.cover_url == "new.jpg"
    assert series.description == "New description"
    assert set(series.aliases.split("|")) == {"Old Alias", "New Alias"}
    assert set(series.genres.split(",")) == {"Action", "Drama"}
    assert services.decode_external_ids(series.external_ids) == {"mal": "1", "anilist": "2"}
    assert series.popularity == 10


def test_matched_source_merges_metadata_into_existing_series():
    session = make_session()
    existing = merge_series_item(
        session,
        SeriesItem(
            source="kingofshojo",
            source_id="manga/example",
            title="Example",
            url="x",
            aliases=("Original Alias",),
            genres=("Romance",),
        ),
    )
    session.commit()

    matched = merge_series_item(
        session,
        SeriesItem(
            source="asura",
            source_id="comics/example",
            title="Example",
            url="y",
            aliases=("Asura Alias",),
            genres=("Fantasy",),
            external_ids={"mal": "9"},
        ),
    )
    session.commit()

    assert matched.series_id == existing.series_id
    series = matched.series
    assert set(series.aliases.split("|")) == {"Original Alias", "Asura Alias"}
    assert set(series.genres.split(",")) == {"Romance", "Fantasy"}
    assert services.decode_external_ids(series.external_ids) == {"mal": "9"}


def test_validated_image_extension_uses_actual_image_format():
    assert (
        services.validated_image_extension(
            image_bytes("PNG"), "image/jpeg", "https://example.com/cover.jpg"
        )
        == "png"
    )


def test_validated_image_extension_rejects_unsupported_actual_format():
    content = image_bytes("BMP")

    try:
        services.validated_image_extension(content, "image/jpeg", "https://example.com/cover.jpg")
    except RuntimeError as exc:
        assert "unsupported cover image format" in str(exc)
    else:
        raise AssertionError("expected unsupported cover image format to be rejected")


async def test_download_cover_image_rejects_oversized_content(monkeypatch):
    class Response:
        headers = {"content-type": "image/jpeg", "content-length": "5"}

        def raise_for_status(self):
            return None

        async def aiter_bytes(self):
            yield b"12345"

    class Stream:
        async def __aenter__(self):
            return Response()

        async def __aexit__(self, exc_type, exc, tb):
            return None

    class Client:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        def stream(self, method, url):
            return Stream()

    monkeypatch.setattr(services.settings, "max_cover_bytes", 3)
    monkeypatch.setattr(services.httpx, "AsyncClient", Client)

    try:
        await services.download_cover_image("https://example.com/cover.jpg")
    except RuntimeError as exc:
        assert "max_cover_bytes" in str(exc)
    else:
        raise AssertionError("expected oversized cover to be rejected")


async def test_cache_cover_image_refreshes_when_url_changes(tmp_path, monkeypatch):
    async def download_cover_image(url):
        return image_bytes("JPEG"), "image/jpeg"

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services, "download_cover_image", download_cover_image)
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.commit()

    old_result = await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/old.jpg",
        ),
    )
    assert old_result is not None
    old_path = source_series.cover_path

    new_result = await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/new.jpg",
        ),
    )

    assert source_series.cover_path != old_path
    assert source_series.cover_path.endswith(".jpg")
    assert Path(source_series.cover_path).exists()
    assert Path(old_path).exists()
    assert new_result is not None
    services.cleanup_stale_cover_images([new_result])
    assert not Path(old_path).exists()
    assert not list((tmp_path / "library" / "_covers").glob("*.tmp"))


async def test_cache_cover_image_rollback_preserves_previous_referenced_cover(tmp_path, monkeypatch):
    async def download_cover_image(url):
        return image_bytes("JPEG"), "image/jpeg"

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services, "download_cover_image", download_cover_image)
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.commit()

    await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/old.jpg",
        ),
    )
    session.commit()
    old_path = source_series.cover_path

    await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/new.jpg",
        ),
    )
    session.rollback()

    assert source_series.cover_path == old_path
    assert Path(old_path).exists()


async def test_cache_cover_image_failed_refresh_preserves_existing_cover(tmp_path, monkeypatch):
    calls = 0

    async def download_cover_image(url):
        nonlocal calls
        calls += 1
        if calls == 1:
            return image_bytes("JPEG"), "image/jpeg"
        raise RuntimeError("cover down")

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services, "download_cover_image", download_cover_image)
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.commit()

    await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/old.jpg",
        ),
    )
    old_path = source_series.cover_path

    await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/new.jpg",
        ),
    )

    assert source_series.cover_path == old_path
    assert Path(old_path).read_bytes() == image_bytes("JPEG")
    assert not list((tmp_path / "library" / "_covers").glob("*.tmp"))


async def test_cache_cover_image_rejects_corrupt_refresh_and_preserves_existing_cover(
    tmp_path, monkeypatch
):
    calls = 0

    async def download_cover_image(url):
        nonlocal calls
        calls += 1
        if calls == 1:
            return image_bytes("PNG"), "image/png"
        return b"not an image", "image/jpeg"

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services, "download_cover_image", download_cover_image)
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.commit()

    await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/old.png",
        ),
    )
    old_path = source_series.cover_path

    await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/new.jpg",
        ),
    )

    assert source_series.cover_path == old_path
    assert source_series.series.cover_path == old_path
    assert Path(old_path).read_bytes() == image_bytes("PNG")
    assert not list((tmp_path / "library" / "_covers").glob("*.tmp"))


async def test_poll_source_removes_stale_cover_after_successful_commit(tmp_path, monkeypatch):
    async def download_cover_image(url):
        return image_bytes("JPEG"), "image/jpeg"

    class Adapter:
        async def list_recent(self):
            return [
                SeriesItem(
                    source="mangafire",
                    source_id="gl3",
                    title="Example",
                    url="x",
                    cover_url="https://example.com/new.jpg",
                )
            ]

        async def get_chapters(self, item):
            return [ChapterItem("mangafire", item.source_id, "1", "Chapter 1", item.url, None)]

        async def aclose(self):
            return None

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services, "download_cover_image", download_cover_image)
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.commit()

    await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/old.jpg",
        ),
    )
    session.commit()
    old_path = source_series.cover_path

    assert await services.poll_source(session, "mangafire") == 1

    session.refresh(source_series)
    assert source_series.cover_path != old_path
    assert services.cover_url_hash("https://example.com/new.jpg") in source_series.cover_path
    assert Path(source_series.cover_path).exists()
    assert not Path(old_path).exists()


async def test_cache_cover_image_accepts_valid_cover_with_detected_extension(tmp_path, monkeypatch):
    async def download_cover_image(url):
        return image_bytes("PNG"), "image/jpeg"

    session = make_session()
    monkeypatch.setattr(services.settings, "library_root", tmp_path / "library")
    monkeypatch.setattr(services, "download_cover_image", download_cover_image)
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.commit()

    await services.cache_cover_image(
        session,
        source_series,
        SeriesItem(
            source="mangafire",
            source_id="gl3",
            title="Example",
            url="x",
            cover_url="https://example.com/cover.jpg",
        ),
    )

    assert source_series.cover_path.endswith(".png")
    assert Path(source_series.cover_path).read_bytes() == image_bytes("PNG")


async def test_drain_download_queue_runs_until_no_ready_jobs(monkeypatch):
    session = make_session()
    calls = 0

    async def fake_run_next_download(active_session):
        nonlocal calls
        assert active_session is session
        calls += 1
        return calls <= 2

    monkeypatch.setattr(services, "run_next_download", fake_run_next_download)
    monkeypatch.setattr(services.settings, "download_concurrency", 1)

    assert await drain_download_queue(session) == 2
    assert calls == 3


async def test_rescan_source_series_refreshes_chapters(monkeypatch):
    class Adapter:
        async def get_chapters(self, item):
            assert item.source == "mangafire"
            assert item.source_id == "gl3"
            return [
                ChapterItem("mangafire", item.source_id, "1", "Chapter 1", "x/1", None),
                ChapterItem("mangafire", item.source_id, "2", "Chapter 2", "x/2", None),
            ]

        async def aclose(self):
            return None

    session = make_session()
    source_series = merge_series_item(
        session,
        SeriesItem(source="mangafire", source_id="gl3", title="Example", url="x"),
    )
    session.commit()
    monkeypatch.setattr(services, "enabled_source_names", lambda: ["mangafire"])
    monkeypatch.setattr(services, "adapter_for_source", lambda source: Adapter())

    assert await rescan_source_series(session, source_series.id) == 2

    session.refresh(source_series)
    assert source_series.detail_fetched_at is not None
    assert session.query(Chapter).count() == 2


async def noop_scan():
    return None
