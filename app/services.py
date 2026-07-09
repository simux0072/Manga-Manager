from __future__ import annotations

import io
import asyncio
import hashlib
import json
import logging
import shutil
import re
import smtplib
import time
import zipfile
from email.message import EmailMessage
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import httpx
from PIL import Image
from sqlalchemy import or_, select, update
from sqlalchemy.exc import IntegrityError, OperationalError, PendingRollbackError
from sqlalchemy.orm import Session

from app.adapters import adapter_for_source, enabled_source_names
from app.adapters.base import ChapterTemporarilyUnavailable, SourceAdapter, SourceRateLimited
from app.domain import (
    ChapterItem,
    SOURCE_PRIORITY,
    SeriesItem,
    normalize_chapter_number,
    normalize_title,
    should_replace,
    title_similarity,
)
from app.kavita import configured_kavita_client
from app.kavita import local_path_for_kavita
from app.kavita import KavitaSeries
from app.models import (
    Chapter,
    ChapterProgress,
    ChapterRelease,
    ActivityEvent,
    DownloadedFile,
    DownloadJob,
    KavitaSyncJob,
    ManualMatchRule,
    MatchCandidate,
    Series,
    SeriesProgress,
    SourceHealth,
    SourcePullJob,
    SourceSeries,
    utcnow,
)
from app.settings import settings


logger = logging.getLogger(__name__)
TRACKED_STATUSES = {"reading", "interested"}
FIRST_IMPORT_CHAPTERS = settings.first_import_chapters
SERIES_PROGRESS_STATUSES = {"interested", "reading", "caught_up", "paused"}
CHAPTER_PROGRESS_STATUSES = {"unread", "reading", "read"}


def is_sqlite_locked_error(exc: BaseException) -> bool:
    return "database is locked" in str(exc).lower()


def commit_with_retry(session: Session, *, attempts: int = 4, delay_seconds: float = 0.15) -> None:
    for attempt in range(1, attempts + 1):
        try:
            session.commit()
            return
        except OperationalError as exc:
            session.rollback()
            if not is_sqlite_locked_error(exc) or attempt == attempts:
                raise
            time.sleep(delay_seconds * attempt)
        except PendingRollbackError:
            session.rollback()
            raise


@dataclass(frozen=True)
class CoverCacheResult:
    active_path: Path
    stale_paths: frozenset[Path]


def record_activity(
    session: Session,
    kind: str,
    status: str = "info",
    message: str = "",
    *,
    source: str = "",
    series_id: int | None = None,
    chapter_id: int | None = None,
    download_job_id: int | None = None,
    kavita_sync_job_id: int | None = None,
    metadata: dict[str, object] | None = None,
) -> ActivityEvent:
    event = ActivityEvent(
        kind=kind,
        status=status,
        message=message,
        source=source,
        series_id=series_id,
        chapter_id=chapter_id,
        download_job_id=download_job_id,
        kavita_sync_job_id=kavita_sync_job_id,
        metadata_json=json.dumps(metadata or {}, sort_keys=True) if metadata else "",
        created_at=utcnow(),
    )
    session.add(event)
    deliver_activity_notification(event)
    return event


def deliver_activity_notification(event: ActivityEvent) -> None:
    if event.status not in {"warning", "error", "success"}:
        return
    try:
        if settings.notification_webhook_url:
            with httpx.Client(timeout=settings.notification_timeout_seconds) as client:
                client.post(
                    settings.notification_webhook_url,
                    json={
                        "kind": event.kind,
                        "status": event.status,
                        "message": event.message,
                        "source": event.source,
                        "series_id": event.series_id,
                        "chapter_id": event.chapter_id,
                        "created_at": event.created_at.isoformat(),
                    },
                )
        if settings.smtp_host and settings.smtp_from and settings.smtp_to:
            send_email_notification(event)
    except Exception as exc:
        logger.warning("activity notification delivery failed: %s", exc)


def send_email_notification(event: ActivityEvent) -> None:
    message = EmailMessage()
    message["From"] = settings.smtp_from
    message["To"] = settings.smtp_to
    message["Subject"] = f"Manga Manager: {event.kind} {event.status}"
    message.set_content(event.message or f"{event.kind} {event.status}")
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=settings.notification_timeout_seconds) as smtp:
        if settings.smtp_username:
            smtp.starttls()
            smtp.login(settings.smtp_username, settings.smtp_password)
        smtp.send_message(message)


def slugify(value: str) -> str:
    value = re.sub(r"[^\w\s.-]", "", value, flags=re.ASCII).strip()
    value = re.sub(r"\s+", " ", value)
    return value[:160] or "Untitled"


def merge_series_item(session: Session, item: SeriesItem) -> SourceSeries:
    normalized = normalize_title(item.title)
    external_ids = encode_external_ids(item.external_ids)
    source_series = session.scalar(
        select(SourceSeries).where(
            SourceSeries.source == item.source,
            SourceSeries.source_id == item.source_id,
        )
    )
    if source_series:
        source_series.title = item.title
        source_series.url = item.url
        source_series.cover_url = item.cover_url or source_series.cover_url
        source_series.description = item.description or source_series.description
        source_series.genres = ",".join(item.genres) or source_series.genres
        source_series.aliases = merge_delimited(source_series.aliases, item.aliases, "|")
        source_series.popularity = max(source_series.popularity, item.popularity)
        source_series.external_ids = merge_external_ids(source_series.external_ids, item.external_ids)
        if item.metadata:
            source_series.metadata_json = encode_metadata(item.metadata)
        refresh_series_metadata(source_series.series, item)
        source_series.last_checked_at = utcnow()
        return source_series

    series, confidence, reason = find_matching_series(session, item)
    matched = series is not None
    if series is None:
        series = Series(
            title=item.title,
            normalized_title=normalized,
            aliases="|".join(item.aliases),
            description=item.description,
            cover_url=item.cover_url,
            genres=",".join(item.genres),
            popularity=item.popularity,
            external_ids=external_ids,
        )
        session.add(series)
        session.flush()
    else:
        refresh_series_metadata(series, item)

    source_series = SourceSeries(
        series=series,
        source=item.source,
        source_id=item.source_id,
        title=item.title,
        url=item.url,
        normalized_title=normalized,
        aliases="|".join(item.aliases),
        cover_url=item.cover_url,
        description=item.description,
        genres=",".join(item.genres),
        popularity=item.popularity,
        external_ids=external_ids,
        metadata_json=encode_metadata(item.metadata),
        last_checked_at=utcnow(),
    )
    session.add(source_series)
    session.flush()
    if not matched:
        create_match_candidates(session, source_series, item, confidence, reason)
    return source_series


def refresh_series_metadata(series: Series, item: SeriesItem) -> None:
    if item.cover_url:
        series.cover_url = item.cover_url
    if item.description:
        series.description = item.description
    series.aliases = merge_delimited(series.aliases, item.aliases, "|")
    series.genres = merge_delimited(series.genres, item.genres, ",")
    series.popularity = max(series.popularity, item.popularity)
    series.external_ids = merge_external_ids(series.external_ids, item.external_ids)
    series.updated_at = utcnow()


def encode_metadata(value: dict[str, object] | None) -> str:
    return json.dumps(value or {}, sort_keys=True) if value else ""


def decode_metadata(value: str) -> dict[str, object]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def find_matching_series(session: Session, item: SeriesItem) -> tuple[Series | None, float, str]:
    normalized = normalize_title(item.title)
    if item.external_ids:
        for candidate in session.scalars(select(Series)).all():
            if shared_external_ids(item.external_ids, decode_external_ids(candidate.external_ids)):
                return candidate, 1.0, "shared external id"

    exact = session.scalar(select(Series).where(Series.normalized_title == normalized))
    if exact:
        return exact, 1.0, "exact normalized title"

    candidates = session.scalars(select(Series).limit(500)).all()
    best: tuple[float, Series] | None = None
    names = [item.title, *item.aliases]
    for candidate in candidates:
        candidate_names = [candidate.title, *(candidate.aliases or "").split("|")]
        score = max(
            title_similarity(left, right)
            for left in names
            for right in candidate_names
            if left and right
        )
        if best is None or score > best[0]:
            best = (score, candidate)
    if best and best[0] >= 0.90:
        return best[1], best[0], "title similarity"
    return None, best[0] if best else 0, "below auto-merge threshold"


def create_match_candidates(
    session: Session,
    source_series: SourceSeries,
    item: SeriesItem,
    best_confidence: float,
    reason: str,
) -> None:
    names = [item.title, *item.aliases]
    for candidate in session.scalars(select(Series).where(Series.id != source_series.series_id)).all():
        blocked = session.scalar(
            select(ManualMatchRule).where(
                ManualMatchRule.source_series_id == source_series.id,
                ManualMatchRule.target_series_id == candidate.id,
                ManualMatchRule.action == "separate",
            )
        )
        if blocked:
            continue
        candidate_names = [candidate.title, *(candidate.aliases or "").split("|")]
        score = max(
            title_similarity(left, right)
            for left in names
            for right in candidate_names
            if left and right
        )
        score = max(score, best_confidence if candidate.normalized_title == normalize_title(item.title) else 0)
        if 0.70 <= score < 0.90:
            existing = session.scalar(
                select(MatchCandidate).where(
                    MatchCandidate.source_series_id == source_series.id,
                    MatchCandidate.candidate_series_id == candidate.id,
                )
            )
            if existing is None:
                session.add(
                    MatchCandidate(
                        source_series_id=source_series.id,
                        candidate_series_id=candidate.id,
                        confidence=score,
                        reason=reason,
                    )
                )


def upsert_release(session: Session, source_series: SourceSeries, item: ChapterItem) -> ChapterRelease:
    session.flush()
    number = normalize_chapter_number(item.number)
    now = utcnow()
    release = session.scalar(
        select(ChapterRelease).where(
            ChapterRelease.source_series_id == source_series.id,
            ChapterRelease.number == number,
        )
    )
    first_seen_at = ensure_aware(release.first_seen_at) if release else now
    downloadable_after = None
    if item.source == "asura":
        downloadable_after = (ensure_aware(item.published_at) or first_seen_at) + timedelta(
            hours=settings.asura_delay_hours
        )

    chapter = session.scalar(
        select(Chapter).where(
            Chapter.series_id == source_series.series_id,
            Chapter.number == number,
        )
    )
    if chapter is None:
        chapter = Chapter(
            series_id=source_series.series_id,
            number=number,
            title=item.title,
            best_source=item.source,
        )
        session.add(chapter)
        session.flush()
    elif should_replace(chapter.best_source, item.source):
        chapter.best_source = item.source
        chapter.updated_at = utcnow()

    if release:
        release.title = item.title or release.title
        release.url = item.url
        release.published_at = earliest_datetime(release.published_at, item.published_at)
        release.first_seen_at = release.first_seen_at or first_seen_at
        release.downloadable_after = downloadable_after
        release.chapter_id = chapter.id
        return release

    release = ChapterRelease(
        chapter_id=chapter.id,
        source_series_id=source_series.id,
        source=item.source,
        number=number,
        title=item.title,
        url=item.url,
        published_at=item.published_at,
        first_seen_at=first_seen_at,
        downloadable_after=downloadable_after,
    )
    session.add(release)
    return release


PullProgressCallback = Callable[[int, int], None]


async def poll_source(
    session: Session,
    source: str,
    progress: PullProgressCallback | None = None,
) -> int:
    if source not in enabled_source_names():
        raise RuntimeError(f"source {source} is disabled or unavailable")
    health = session.get(SourceHealth, source) or SourceHealth(
        source=source,
        enabled=True,
        last_error="",
        consecutive_failures=0,
    )
    session.add(health)
    if not health.enabled:
        commit_with_retry(session)
        return 0

    adapter = adapter_for_source(source)
    if adapter is None:
        raise RuntimeError(f"source {source} is disabled or unavailable")
    try:
        try:
            items = await adapter.list_recent()
            if progress is not None:
                progress(0, len(items))
                commit_with_retry(session)
            count = 0
            item_failures = 0
            skipped_items = 0
            cover_cache_results: list[CoverCacheResult] = []
            for index, item in enumerate(items, start=1):
                try:
                    chapters = await adapter.get_chapters(item)
                    if source == "mangafire" and not chapters:
                        skipped_items += 1
                        logger.info(
                            "skipping %s item with no english chapters: %s",
                            source,
                            item.url,
                        )
                        if progress is not None:
                            progress(index, len(items))
                            commit_with_retry(session)
                        continue
                    with session.begin_nested():
                        source_series = merge_series_item(session, item)
                        for chapter in chapters:
                            upsert_release(session, source_series, chapter)
                    cover_cache_result = await cache_cover_image(session, source_series, item)
                    if cover_cache_result is not None:
                        cover_cache_results.append(cover_cache_result)
                    count += 1
                except Exception as exc:
                    item_failures += 1
                    logger.warning("failed to poll %s item %s: %s", source, item.url, exc)
                if progress is not None:
                    progress(index, len(items))
                    commit_with_retry(session)
            if items and item_failures == len(items):
                record_source_failure(health, f"all {item_failures} discovered items failed")
                record_activity(
                    session,
                    "source_poll",
                    "warning",
                    f"{source} poll had {item_failures} item failures",
                    source=source,
                )
            else:
                record_source_success(health, item_failures)
                record_activity(
                    session,
                    "source_poll",
                    "success",
                    f"{source} poll found {count} series",
                    source=source,
                    metadata={
                        "count": count,
                        "item_failures": item_failures,
                        "skipped_items": skipped_items,
                    },
                )
            commit_with_retry(session)
            cleanup_stale_cover_images(cover_cache_results)
            return count
        except Exception as exc:
            record_source_failure(health, str(exc))
            record_activity(session, "source_poll", "error", str(exc), source=source)
            commit_with_retry(session)
            raise
    finally:
        await close_adapter(adapter)


def create_pull_job(session: Session, source: str) -> tuple[SourcePullJob, bool]:
    existing = session.scalar(
        select(SourcePullJob)
        .where(SourcePullJob.source == source)
        .where(SourcePullJob.status.in_(["queued", "running"]))
        .order_by(SourcePullJob.created_at.desc(), SourcePullJob.id.desc())
        .limit(1)
    )
    if existing is not None:
        return existing, False
    job = SourcePullJob(source=source, status="queued", total_items=0, processed_items=0)
    session.add(job)
    try:
        commit_with_retry(session)
    except IntegrityError:
        session.rollback()
        existing = session.scalar(
            select(SourcePullJob)
            .where(SourcePullJob.source == source)
            .where(SourcePullJob.status.in_(["queued", "running"]))
            .order_by(SourcePullJob.created_at.desc(), SourcePullJob.id.desc())
            .limit(1)
        )
        if existing is None:
            raise
        return existing, False
    session.refresh(job)
    return job, True


async def run_pull_job(job_id: int) -> None:
    from app.db import SessionLocal

    with SessionLocal() as session:
        job = session.get(SourcePullJob, job_id)
        if job is None:
            return
        job.status = "running"
        job.updated_at = utcnow()
        commit_with_retry(session)
        logger.info("pull job started job_id=%s source=%s", job.id, job.source)
        try:
            def update_progress(processed: int, total: int) -> None:
                if job.status not in {"queued", "running"}:
                    return
                job.total_items = total
                job.processed_items = processed
                job.updated_at = utcnow()

            count = await poll_source(session, job.source, progress=update_progress)
            queue_downloads(session)
        except Exception as exc:
            logger.exception("pull job failed for %s", job.source)
            job = session.get(SourcePullJob, job_id)
            if job is not None:
                job.status = "failed"
                job.error = str(exc)
                job.updated_at = utcnow()
                job.completed_at = utcnow()
                commit_with_retry(session)
            return
        job = session.get(SourcePullJob, job_id)
        if job is not None:
            if job.total_items == 0:
                job.total_items = count
            job.processed_items = max(job.processed_items, job.total_items)
            job.status = "complete"
            job.updated_at = utcnow()
            job.completed_at = utcnow()
            commit_with_retry(session)
            logger.info(
                "pull job complete job_id=%s source=%s processed=%s total=%s imported=%s",
                job.id,
                job.source,
                job.processed_items,
                job.total_items,
                count,
            )


async def run_pull_jobs_limited(job_ids: list[int]) -> None:
    semaphore = asyncio.Semaphore(settings.source_pull_concurrency)

    async def worker(job_id: int) -> None:
        async with semaphore:
            await run_pull_job(job_id)

    await asyncio.gather(*(worker(job_id) for job_id in job_ids))


def recover_stale_pull_jobs(session: Session) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=settings.download_stale_minutes)
    recovered = 0
    for job in session.scalars(
        select(SourcePullJob)
        .where(SourcePullJob.status.in_(["queued", "running"]))
        .where(SourcePullJob.updated_at < cutoff)
    ):
        job.status = "failed"
        job.error = "recovered stale pull job"
        job.updated_at = utcnow()
        job.completed_at = utcnow()
        recovered += 1
    commit_with_retry(session)
    return recovered


def pull_status(session: Session) -> dict:
    active = session.scalars(
        select(SourcePullJob)
        .where(SourcePullJob.status.in_(["queued", "running"]))
        .order_by(SourcePullJob.created_at.desc(), SourcePullJob.id.desc())
    ).all()
    jobs = session.scalars(
        select(SourcePullJob)
        .order_by(SourcePullJob.created_at.desc(), SourcePullJob.id.desc())
        .limit(10)
    ).all()
    latest = jobs[0] if jobs else None
    total = sum(job.total_items for job in active)
    processed = sum(job.processed_items for job in active)
    return {
        "active": bool(active),
        "status": latest.status if latest else "idle",
        "source": latest.source if latest else "",
        "processed": processed,
        "total": total,
        "error": latest.error if latest else "",
        "jobs": [
            {
                "source": job.source,
                "status": job.status,
                "processed": job.processed_items,
                "total": job.total_items,
                "error": job.error,
            }
            for job in jobs
        ],
    }


def record_source_success(health: SourceHealth, item_failures: int = 0) -> None:
    health.last_poll_at = utcnow()
    health.last_error = f"{item_failures} item failures" if item_failures else ""
    health.consecutive_failures = 0


def record_source_failure(health: SourceHealth, error: str) -> None:
    health.last_poll_at = utcnow()
    health.last_error = error
    health.consecutive_failures += 1
    if health.consecutive_failures >= 5:
        health.enabled = False


def source_download_cooldown(session: Session, source: str) -> datetime | None:
    health = session.get(SourceHealth, source)
    if health is None:
        return None
    cooldown = ensure_aware(health.download_cooldown_until)
    if cooldown and cooldown <= datetime.now(timezone.utc):
        health.download_cooldown_until = None
        health.download_cooldown_reason = ""
        return None
    return cooldown


def source_is_download_ready(session: Session, source: str, now: datetime | None = None) -> bool:
    health = session.get(SourceHealth, source)
    if health is None:
        return True
    cooldown = ensure_aware(health.download_cooldown_until)
    return cooldown is None or cooldown <= (now or datetime.now(timezone.utc))


def set_source_download_cooldown(
    session: Session,
    source: str,
    retry_after: datetime | None,
    reason: str,
) -> datetime:
    if retry_after is None:
        minutes = (
            settings.asura_rate_limit_cooldown_minutes
            if source == "asura"
            else settings.rate_limit_cooldown_minutes
        )
        retry_after = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    retry_after = ensure_aware(retry_after) or datetime.now(timezone.utc)
    health = session.get(SourceHealth, source) or SourceHealth(source=source, enabled=True)
    health.download_cooldown_until = retry_after
    health.download_cooldown_reason = reason
    session.add(health)
    return retry_after


async def poll_all_sources(session: Session) -> dict[str, dict[str, object]]:
    results: dict[str, dict[str, object]] = {}
    for source in enabled_source_names():
        try:
            count = await poll_source(session, source)
            results[source] = {"ok": True, "count": count, "error": ""}
        except Exception as exc:
            logger.warning("failed to poll source %s: %s", source, exc)
            results[source] = {"ok": False, "count": 0, "error": str(exc)}
    return results


def queue_downloads(session: Session) -> int:
    if not settings.downloads_enabled:
        return 0
    recover_stale_download_jobs(session)
    now = datetime.now(timezone.utc)
    tracked_series_ids = [
        row[0]
        for row in session.execute(
            select(Series.id).where(Series.status.in_(TRACKED_STATUSES))
        ).all()
    ]
    if not tracked_series_ids:
        return 0

    queued = 0
    releases = session.scalars(
        select(ChapterRelease)
        .join(Chapter)
        .where(Chapter.series_id.in_(tracked_series_ids))
    ).all()
    for release in releases:
        chapter = release.chapter
        if chapter is None:
            continue
        if release.id != selected_release_for_chapter(session, chapter, now):
            continue
        if chapter.downloaded_source and not should_replace(chapter.downloaded_source, release.source):
            continue
        existing = session.scalar(select(DownloadJob).where(DownloadJob.chapter_release_id == release.id))
        if existing:
            if existing.status == "delayed" and ensure_aware(existing.retry_after) and ensure_aware(existing.retry_after) > now:
                continue
            if existing.status in {"delayed", "failed", "skipped"}:
                existing.status = "queued"
                existing.error = ""
                existing.retry_after = None
                existing.updated_at = utcnow()
                queued += 1
            continue
        try:
            with session.begin_nested():
                session.add(DownloadJob(chapter_release_id=release.id))
                session.flush()
            queued += 1
        except IntegrityError:
            logger.info("download job already exists for release %s", release.id)
    commit_with_retry(session)
    return queued


def selected_release_for_chapter(
    session: Session, chapter: Chapter, now: datetime | None = None
) -> int | None:
    now = now or datetime.now(timezone.utc)
    releases = session.scalars(
        select(ChapterRelease)
        .where(ChapterRelease.chapter_id == chapter.id)
        .order_by(ChapterRelease.id)
    ).all()
    releases = sorted(releases, key=lambda release: SOURCE_PRIORITY.get(release.source, 0), reverse=True)
    for release in releases:
        job = session.scalar(select(DownloadJob).where(DownloadJob.chapter_release_id == release.id))
        if not source_is_download_ready(session, release.source, now):
            continue
        if job and job.status == "failed" and job.attempts >= settings.max_download_attempts:
            continue
        downloadable_after = ensure_aware(release.downloadable_after)
        if downloadable_after and downloadable_after > now:
            return None
        if release.downloaded:
            return release.id
        if job is None:
            return release.id
        if job.status in {"queued", "running", "delayed", "complete"}:
            return release.id
        if job.status == "failed" and job.attempts < settings.max_download_attempts:
            return release.id
        if job.status == "skipped":
            return release.id
    return None


def next_release_for_chapter(
    session: Session,
    chapter: Chapter,
    *,
    exclude_release_id: int | None = None,
    now: datetime | None = None,
) -> ChapterRelease | None:
    release_id = selected_release_for_chapter(session, chapter, now)
    if release_id is None:
        return None
    if exclude_release_id is None or release_id != exclude_release_id:
        return session.get(ChapterRelease, release_id)
    releases = session.scalars(
        select(ChapterRelease)
        .where(ChapterRelease.chapter_id == chapter.id)
        .where(ChapterRelease.id != exclude_release_id)
        .order_by(ChapterRelease.id)
    ).all()
    now = now or datetime.now(timezone.utc)
    for release in sorted(
        releases,
        key=lambda item: SOURCE_PRIORITY.get(item.source, 0),
        reverse=True,
    ):
        if not source_is_download_ready(session, release.source, now):
            continue
        downloadable_after = ensure_aware(release.downloadable_after)
        if downloadable_after and downloadable_after > now:
            continue
        job = session.scalar(select(DownloadJob).where(DownloadJob.chapter_release_id == release.id))
        if job and job.status == "failed" and job.attempts >= settings.max_download_attempts:
            continue
        return release
    return None


def queue_next_chapter_download(
    session: Session,
    chapter: Chapter,
    *,
    exclude_release_id: int,
    priority: int = 100,
    job_type: str = "normal",
) -> bool:
    release = next_release_for_chapter(
        session,
        chapter,
        exclude_release_id=exclude_release_id,
    )
    if release is None:
        return False
    return queue_release(session, release, priority=priority, job_type=job_type)


def queue_release(
    session: Session,
    release: ChapterRelease,
    *,
    priority: int = 100,
    job_type: str = "normal",
) -> bool:
    existing = session.scalar(select(DownloadJob).where(DownloadJob.chapter_release_id == release.id))
    if existing:
        retry_after = ensure_aware(existing.retry_after)
        if existing.status == "delayed" and retry_after and retry_after > datetime.now(timezone.utc):
            return False
        if existing.status in {"failed", "skipped", "delayed"}:
            existing.status = "queued"
            existing.error = ""
            existing.retry_after = None
            existing.priority = min(existing.priority, priority)
            existing.job_type = job_type
            existing.updated_at = utcnow()
            commit_with_retry(session)
            return True
        return False
    session.add(DownloadJob(chapter_release_id=release.id, priority=priority, job_type=job_type))
    commit_with_retry(session)
    return True


def queue_chapter_download(
    session: Session,
    chapter: Chapter,
    *,
    priority: int = 100,
    job_type: str = "normal",
) -> bool:
    release_id = selected_release_for_chapter(session, chapter)
    if release_id is None:
        return False
    release = session.get(ChapterRelease, release_id)
    return (
        queue_release(session, release, priority=priority, job_type=job_type)
        if release is not None
        else False
    )


def newest_missing_chapters(series: Series, limit: int = FIRST_IMPORT_CHAPTERS) -> list[Chapter]:
    chapters = sorted(series.chapters, key=chapter_sort_key, reverse=True)
    return [
        chapter
        for chapter in chapters
        if not chapter.downloaded_source or not chapter.kavita_chapter_id
    ][:limit]


def newest_chapter_time(chapter: Chapter) -> datetime:
    return chapter_platform_release_time(chapter)


def release_display_time(release: ChapterRelease) -> datetime:
    return (
        ensure_aware(release.published_at)
        or ensure_aware(release.first_seen_at)
        or utcnow()
    )


def chapter_platform_release_time(chapter: Chapter) -> datetime:
    values = [
        release_display_time(release)
        for release in chapter.releases
    ]
    return min(values) if values else ensure_aware(chapter.updated_at) or utcnow()


def series_latest_platform_release_time(series: Series) -> datetime:
    values = [chapter_platform_release_time(chapter) for chapter in series.chapters]
    return max(values) if values else ensure_aware(series.updated_at) or utcnow()


def chapter_sort_key(chapter: Chapter) -> tuple[int, Decimal, datetime]:
    try:
        number = Decimal(chapter.number)
    except InvalidOperation:
        return (0, Decimal(0), chapter_platform_release_time(chapter))
    return (1, number, chapter_platform_release_time(chapter))


def queue_newest_missing_chapters(
    session: Session, series: Series, limit: int = FIRST_IMPORT_CHAPTERS
) -> int:
    queued = 0
    for chapter in newest_missing_chapters(series, limit):
        if chapter.downloaded_source and not chapter.kavita_chapter_id:
            folder_path = Path(chapter.cbz_path).parent if chapter.cbz_path else None
            if queue_kavita_sync(session, series, folder_path):
                queued += 1
        elif queue_chapter_download(session, chapter, priority=10, job_type="first_import"):
            queued += 1
    if settings.backfill_downloads_enabled:
        for chapter in newest_missing_chapters(series, limit=10_000)[limit:]:
            if not chapter.downloaded_source and queue_chapter_download(
                session, chapter, priority=200, job_type="backfill"
            ):
                queued += 1
    return queued


async def run_next_download(session: Session) -> bool:
    recover_stale_download_jobs(session)
    job = claim_next_download_job(session)
    if job is None:
        return False

    release = session.get(ChapterRelease, job.chapter_release_id)
    if release is None or release.chapter is None:
        session.delete(job)
        commit_with_retry(session)
        logger.warning("removed orphaned download job job_id=%s release_id=%s", job.id, job.chapter_release_id)
        return True

    logger.info(
        "download job started job_id=%s release_id=%s source=%s series_id=%s chapter_id=%s chapter=%s attempt=%s",
        job.id,
        release.id,
        release.source,
        release.chapter.series_id,
        release.chapter.id,
        release.number,
        job.attempts,
    )

    if delay_job_if_not_due(session, job, release):
        logger.info(
            "download job delayed job_id=%s source=%s chapter_id=%s retry_after=%s",
            job.id,
            release.source,
            release.chapter.id,
            job.retry_after,
        )
        return True
    if skip_job_if_obsolete(session, job, release):
        logger.info("download job skipped job_id=%s reason=%s", job.id, job.error)
        return True

    adapter = adapter_for_source(release.source)
    if adapter is None:
        job.status = "failed"
        job.error = f"source {release.source} is disabled or unavailable"
        job.retry_after = None
        job.updated_at = utcnow()
        commit_with_retry(session)
        logger.error("download job failed job_id=%s reason=%s", job.id, job.error)
        return True

    staging_path: Path | None = None
    try:
        item = ChapterItem(
            source=release.source,
            source_series_id=release.source_series.source_id,
            number=release.number,
            title=release.title,
            url=release.url,
            published_at=release.published_at,
        )
        staging_path, final_path, image_count = await stage_cbz(
            job,
            release,
            iter_adapter_pages(adapter, item),
        )
        old_path = Path(release.chapter.cbz_path) if release.chapter.cbz_path else None
        if old_path:
            archive_previous(old_path)
        staging_path.replace(final_path)
        chapter = release.chapter
        if old_path:
            deactivate_downloaded_files(session, chapter.id)
        checksum = file_checksum(final_path)
        chapter.cbz_path = str(final_path)
        chapter.downloaded_source = release.source
        chapter.best_source = release.source
        chapter.updated_at = utcnow()
        release.downloaded = True
        session.add(
            DownloadedFile(
                chapter_id=chapter.id,
                chapter_release_id=release.id,
                source=release.source,
                path=str(final_path),
                checksum=checksum,
                image_count=image_count,
                active=True,
            )
        )
        job.status = "complete"
        job.error = ""
        job.retry_after = None
        job.updated_at = utcnow()
        commit_with_retry(session)
        logger.info(
            "download job complete job_id=%s source=%s series_id=%s chapter_id=%s pages=%s path=%s",
            job.id,
            release.source,
            chapter.series_id,
            chapter.id,
            image_count,
            final_path,
        )
        record_activity(
            session,
            "download",
            "success",
            f"Downloaded {chapter.series.title} chapter {chapter.number}",
            source=release.source,
            series_id=chapter.series_id,
            chapter_id=chapter.id,
            download_job_id=job.id,
        )
        commit_with_retry(session)
        queue_kavita_sync(session, chapter.series, final_path.parent)
    except ChapterTemporarilyUnavailable as exc:
        retry_after = ensure_aware(exc.retry_after) or (
            datetime.now(timezone.utc) + timedelta(hours=settings.asura_delay_hours or 1)
        )
        release.downloadable_after = retry_after
        job.status = "delayed"
        job.error = str(exc)
        job.retry_after = retry_after
        job.updated_at = utcnow()
        record_activity(
            session,
            "download",
            "warning",
            str(exc),
            source=release.source,
            series_id=release.chapter.series_id,
            chapter_id=release.chapter.id,
            download_job_id=job.id,
        )
        commit_with_retry(session)
        logger.warning(
            "download job delayed job_id=%s source=%s chapter_id=%s retry_after=%s reason=%s",
            job.id,
            release.source,
            release.chapter.id,
            retry_after,
            exc,
        )
    except SourceRateLimited as exc:
        retry_after = set_source_download_cooldown(
            session,
            release.source,
            ensure_aware(exc.retry_after),
            str(exc),
        )
        job.attempts = max(job.attempts - 1, 0)
        job.status = "delayed"
        job.error = str(exc)
        job.retry_after = retry_after
        job.updated_at = utcnow()
        queued_fallback = queue_next_chapter_download(
            session,
            release.chapter,
            exclude_release_id=release.id,
            priority=job.priority + 1,
            job_type=job.job_type,
        )
        record_activity(
            session,
            "download",
            "warning",
            f"{release.source} rate limited; retry after {retry_after.isoformat()}",
            source=release.source,
            series_id=release.chapter.series_id,
            chapter_id=release.chapter.id,
            download_job_id=job.id,
            metadata={"fallback_queued": queued_fallback},
        )
        commit_with_retry(session)
        logger.warning(
            "download job delayed by rate limit job_id=%s source=%s chapter_id=%s retry_after=%s fallback=%s",
            job.id,
            release.source,
            release.chapter.id,
            retry_after,
            queued_fallback,
        )
    except Exception as exc:
        if staging_path is not None:
            staging_path.unlink(missing_ok=True)
        if job.attempts >= settings.max_download_attempts:
            job.status = "failed"
            job.retry_after = None
        else:
            job.status = "queued"
            job.retry_after = next_retry_after(job.attempts)
        job.error = str(exc)
        job.updated_at = utcnow()
        terminal_failure = job.status == "failed"
        record_activity(
            session,
            "download",
            "error" if job.status == "failed" else "warning",
            str(exc),
            source=release.source,
            series_id=release.chapter.series_id,
            chapter_id=release.chapter.id,
            download_job_id=job.id,
        )
        commit_with_retry(session)
        logger.warning(
            "download job %s job_id=%s source=%s chapter_id=%s attempt=%s retry_after=%s reason=%s",
            "failed" if terminal_failure else "retrying",
            job.id,
            release.source,
            release.chapter.id,
            job.attempts,
            job.retry_after,
            exc,
            exc_info=terminal_failure,
        )
        if terminal_failure:
            queued_fallback = queue_next_chapter_download(
                session,
                release.chapter,
                exclude_release_id=release.id,
                priority=job.priority + 1,
                job_type=job.job_type,
            )
            logger.info(
                "download fallback queued job_id=%s source=%s chapter_id=%s queued=%s",
                job.id,
                release.source,
                release.chapter.id,
                queued_fallback,
            )
    finally:
        await close_adapter(adapter)
    return True


def delay_job_if_not_due(session: Session, job: DownloadJob, release: ChapterRelease) -> bool:
    downloadable_after = ensure_aware(release.downloadable_after)
    now = datetime.now(timezone.utc)
    source_cooldown = source_download_cooldown(session, release.source)
    if source_cooldown and source_cooldown > now:
        job.status = "delayed"
        job.error = f"{release.source} rate limited"
        job.retry_after = source_cooldown
        job.updated_at = utcnow()
        commit_with_retry(session)
        return True
    if downloadable_after is None or downloadable_after <= now:
        return False
    job.status = "delayed"
    job.error = "release is not downloadable yet"
    job.retry_after = downloadable_after
    job.updated_at = utcnow()
    commit_with_retry(session)
    return True


def skip_job_if_obsolete(session: Session, job: DownloadJob, release: ChapterRelease) -> bool:
    chapter = release.chapter
    if chapter is None:
        return False
    reason = ""
    selected_release_id = selected_release_for_chapter(session, chapter)
    if release.source != chapter.best_source and not best_source_exhausted(session, chapter):
        reason = f"release source {release.source} is no longer best source {chapter.best_source}"
    elif selected_release_id != release.id:
        reason = f"release source {release.source} is no longer best source {chapter.best_source}"
    elif chapter.downloaded_source and not should_replace(chapter.downloaded_source, release.source):
        reason = (
            f"downloaded source {chapter.downloaded_source} is not replaceable by {release.source}"
        )
    if not reason:
        return False
    job.status = "skipped"
    job.error = reason
    job.retry_after = None
    job.updated_at = utcnow()
    commit_with_retry(session)
    return True


def best_source_exhausted(session: Session, chapter: Chapter) -> bool:
    if not chapter.best_source:
        return False
    best_release = session.scalar(
        select(ChapterRelease)
        .where(ChapterRelease.chapter_id == chapter.id)
        .where(ChapterRelease.source == chapter.best_source)
        .limit(1)
    )
    if best_release is None:
        return False
    job = session.scalar(select(DownloadJob).where(DownloadJob.chapter_release_id == best_release.id))
    return bool(job and job.status == "failed" and job.attempts >= settings.max_download_attempts)


def queue_kavita_sync(
    session: Session, series: Series, folder_path: Path | None = None
) -> bool:
    if not configured_kavita_client().configured:
        return False
    existing = session.scalar(select(KavitaSyncJob).where(KavitaSyncJob.series_id == series.id))
    folder = str(folder_path or kavita_sync_folder_for_series(series))
    if existing:
        if folder and folder != existing.folder_path:
            existing.folder_path = folder
        if existing.status in {"queued", "running"}:
            commit_with_retry(session)
            return False
        existing.status = "queued"
        existing.error = ""
        existing.attempts = 0
        existing.retry_after = None
        existing.updated_at = utcnow()
        commit_with_retry(session)
        return True
    try:
        with session.begin_nested():
            session.add(KavitaSyncJob(series_id=series.id, folder_path=folder))
            session.flush()
    except IntegrityError:
        logger.info("Kavita sync job already exists for series %s", series.id)
        commit_with_retry(session)
        return False
    commit_with_retry(session)
    return True


def kavita_sync_folder_for_series(series: Series) -> Path:
    for chapter in series.chapters:
        if chapter.downloaded_source and chapter.cbz_path:
            return Path(chapter.cbz_path).parent
    return settings.library_root / "Manga" / slugify(series.title)


def series_has_pending_kavita_mapping(series: Series) -> bool:
    return any(chapter.downloaded_source and not chapter.kavita_chapter_id for chapter in series.chapters)


def pending_kavita_sync_series(session: Session) -> list[Series]:
    series_rows = session.scalars(
        select(Series)
        .where(Series.status.in_(TRACKED_STATUSES))
        .where(
            Series.chapters.any(
                (Chapter.downloaded_source != "") & (Chapter.kavita_chapter_id.is_(None))
            )
        )
    )
    return [series for series in series_rows if series_has_pending_kavita_mapping(series)]


def pending_kavita_sync_series_count(session: Session) -> int:
    return len(pending_kavita_sync_series(session))


def queue_pending_kavita_syncs(session: Session) -> int:
    if not configured_kavita_client().configured:
        return 0
    count = 0
    for series in pending_kavita_sync_series(session):
        if queue_kavita_sync(session, series, kavita_sync_folder_for_series(series)):
            count += 1
    return count


async def run_next_kavita_sync(session: Session) -> bool:
    if not configured_kavita_client().configured:
        return False
    recover_stale_kavita_sync_jobs(session)
    job = claim_next_kavita_sync_job(session)
    if job is None:
        return False
    series = session.get(Series, job.series_id)
    if series is None:
        job.status = "failed"
        job.error = "series missing"
        job.updated_at = utcnow()
        commit_with_retry(session)
        return True
    folder_path = Path(job.folder_path) if job.folder_path else None
    try:
        synced = await sync_series_with_kavita(session, series, folder_path)
    except Exception as exc:
        synced = False
        error = str(exc)
    else:
        error = "" if synced else "Kavita series match not found"
    if synced:
        job.status = "complete"
        job.error = ""
        job.retry_after = None
    elif job.attempts >= settings.max_download_attempts:
        job.status = "failed"
        job.error = error
        job.retry_after = None
    else:
        job.status = "queued"
        job.error = error
        job.retry_after = next_retry_after(job.attempts)
    job.updated_at = utcnow()
    record_activity(
        session,
        "kavita_sync",
        "success" if synced else "error",
        f"Kavita sync {'mapped' if synced else 'failed for'} {series.title}",
        series_id=series.id,
        kavita_sync_job_id=job.id,
        metadata={"error": error} if error else None,
    )
    commit_with_retry(session)
    return True


def claim_next_kavita_sync_job(session: Session) -> KavitaSyncJob | None:
    now = datetime.now(timezone.utc)
    ready_filter = or_(KavitaSyncJob.retry_after.is_(None), KavitaSyncJob.retry_after <= now)
    job_id = session.scalar(
        select(KavitaSyncJob.id)
        .where(KavitaSyncJob.status == "queued")
        .where(ready_filter)
        .order_by(KavitaSyncJob.created_at.asc())
        .limit(1)
    )
    if job_id is None:
        return None
    result = session.execute(
        update(KavitaSyncJob)
        .where(KavitaSyncJob.id == job_id)
        .where(KavitaSyncJob.status == "queued")
        .where(ready_filter)
        .values(
            status="running",
            attempts=KavitaSyncJob.attempts + 1,
            retry_after=None,
            updated_at=utcnow(),
        )
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        session.rollback()
        return None
    commit_with_retry(session)
    return session.scalar(
        select(KavitaSyncJob)
        .where(KavitaSyncJob.id == job_id)
        .execution_options(populate_existing=True)
    )


async def close_adapter(adapter: SourceAdapter) -> None:
    close = getattr(adapter, "aclose", None)
    if close is not None:
        await close()


async def iter_adapter_pages(adapter: SourceAdapter, item: ChapterItem) -> AsyncIterator[bytes]:
    iter_pages = getattr(adapter, "iter_chapter_pages", None)
    if iter_pages is not None:
        async for page in iter_pages(item):
            yield page
        return
    for page in await adapter.download_chapter_pages(item):
        yield page


async def stage_cbz(
    job: DownloadJob,
    release: ChapterRelease,
    pages: AsyncIterator[bytes],
) -> tuple[Path, Path, int]:
    chapter = release.chapter
    if chapter is None:
        raise RuntimeError("chapter missing")
    series = chapter.series
    series_dir = settings.library_root / "Manga" / slugify(series.title)
    series_dir.mkdir(parents=True, exist_ok=True)

    final_path = series_dir / f"{slugify(series.title)} Ch. {chapter.number}.cbz"
    staging_path = final_path.with_name(
        f".{final_path.stem}.job-{job.id}.release-{release.id}.tmp"
    )
    image_count = 0
    try:
        with zipfile.ZipFile(staging_path, "w", compression=zipfile.ZIP_DEFLATED) as cbz:
            cbz.writestr("ComicInfo.xml", comic_info_xml(series, chapter, release))
            async for page in pages:
                image_count += 1
                ext = image_extension(page)
                cbz.writestr(f"{image_count:04d}.{ext}", page)
        if image_count < settings.min_pages_per_chapter:
            raise RuntimeError(
                f"only {image_count} chapter images found; "
                f"minimum is {settings.min_pages_per_chapter}"
            )
    except Exception:
        staging_path.unlink(missing_ok=True)
        raise
    return staging_path, final_path, image_count


def claim_next_download_job(session: Session) -> DownloadJob | None:
    now = datetime.now(timezone.utc)
    ready_filter = or_(DownloadJob.retry_after.is_(None), DownloadJob.retry_after <= now)
    job_id = session.scalar(
        select(DownloadJob.id)
        .join(ChapterRelease, ChapterRelease.id == DownloadJob.chapter_release_id)
        .join(SourceSeries, SourceSeries.id == ChapterRelease.source_series_id)
        .outerjoin(SourceHealth, SourceHealth.source == SourceSeries.source)
        .where(DownloadJob.status.in_(["queued", "delayed"]))
        .where(ready_filter)
        .where(
            or_(
                SourceHealth.download_cooldown_until.is_(None),
                SourceHealth.download_cooldown_until <= now,
            )
        )
        .order_by(DownloadJob.priority.asc(), DownloadJob.created_at.asc())
        .limit(1)
    )
    if job_id is None:
        return None

    result = session.execute(
        update(DownloadJob)
        .where(DownloadJob.id == job_id)
        .where(DownloadJob.status.in_(["queued", "delayed"]))
        .where(ready_filter)
        .values(
            status="running",
            attempts=DownloadJob.attempts + 1,
            retry_after=None,
            updated_at=utcnow(),
        )
        .execution_options(synchronize_session=False)
    )
    if result.rowcount != 1:
        session.rollback()
        return None
    commit_with_retry(session)
    return session.scalar(
        select(DownloadJob)
        .where(DownloadJob.id == job_id)
        .execution_options(populate_existing=True)
    )


def image_extension(page: bytes) -> str:
    try:
        with Image.open(io.BytesIO(page)) as image:
            image.verify()
            image_format = image.format
    except Exception as exc:
        raise RuntimeError("invalid image page") from exc
    if not image_format:
        raise RuntimeError("invalid image page")
    return image_format.lower().replace("jpeg", "jpg")


def comic_info_xml(series: Series, chapter: Chapter, release: ChapterRelease) -> str:
    return f"""<?xml version="1.0" encoding="utf-8"?>
<ComicInfo>
  <Series>{escape_xml(series.title)}</Series>
  <Number>{escape_xml(chapter.number)}</Number>
  <Title>{escape_xml(chapter.title or release.title)}</Title>
  <Summary>{escape_xml(series.description)}</Summary>
  <Web>{escape_xml(release.url)}</Web>
  <Genre>{escape_xml(series.genres)}</Genre>
  <Tags>{escape_xml(release.source)}</Tags>
</ComicInfo>
"""


def escape_xml(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def archive_previous(path: Path) -> None:
    if not path.exists():
        return
    if not settings.keep_replaced_files:
        return
    archive_dir = settings.archive_root / "replaced"
    archive_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, archive_dir / f"{path.stem}-{int(datetime.now().timestamp())}{path.suffix}")


def deactivate_downloaded_files(session: Session, chapter_id: int) -> None:
    for downloaded in session.scalars(
        select(DownloadedFile).where(
            DownloadedFile.chapter_id == chapter_id,
            DownloadedFile.active.is_(True),
        )
    ):
        downloaded.active = False
        downloaded.replaced_at = utcnow()


def file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


async def cache_cover_image(
    session: Session, source_series: SourceSeries, item: SeriesItem
) -> CoverCacheResult | None:
    if not item.cover_url:
        return None
    covers_dir = settings.library_root / "_covers"
    covers_dir.mkdir(parents=True, exist_ok=True)

    url_hash = cover_url_hash(item.cover_url)
    stale_paths = frozenset(
        {
            *covers_dir.glob(f"{item.source}-{source_series.id}.*"),
            *covers_dir.glob(f"{item.source}-{source_series.id}-*.*"),
        }
    )
    existing = next(iter(covers_dir.glob(f"{item.source}-{source_series.id}-{url_hash}.*")), None)
    if existing is not None:
        source_series.cover_path = str(existing)
        source_series.series.cover_path = str(existing)
        return CoverCacheResult(
            active_path=existing,
            stale_paths=frozenset(path for path in stale_paths if path != existing),
        )

    try:
        content, content_type = await download_cover_image(item.cover_url)
        if content is None:
            return None
        ext = validated_image_extension(content, content_type, item.cover_url)
        path = covers_dir / f"{item.source}-{source_series.id}-{url_hash}.{ext}"
        tmp_path = path.with_name(f".{path.name}.tmp")
        tmp_path.write_bytes(content)
        tmp_path.replace(path)
        source_series.cover_path = str(path)
        source_series.series.cover_path = str(path)
        return CoverCacheResult(
            active_path=path,
            stale_paths=frozenset(stale_path for stale_path in stale_paths if stale_path != path),
        )
    except Exception as exc:
        tmp_path = locals().get("tmp_path")
        if isinstance(tmp_path, Path):
            tmp_path.unlink(missing_ok=True)
        logger.warning(
            "failed to cache cover for %s source series %s from %s: %s",
            item.source,
            source_series.id,
            item.cover_url,
            exc,
        )
        return None


async def refresh_missing_covers(session: Session, limit: int = 100) -> int:
    refreshed = 0
    results: list[CoverCacheResult] = []
    rows = session.scalars(
        select(SourceSeries)
        .where(SourceSeries.cover_url != "")
        .where(SourceSeries.cover_path == "")
        .order_by(SourceSeries.id.asc())
        .limit(limit)
    ).all()
    for source_series in rows:
        result = await cache_cover_image(
            session,
            source_series,
            SeriesItem(
                source=source_series.source,
                source_id=source_series.source_id,
                title=source_series.title,
                url=source_series.url,
                cover_url=source_series.cover_url,
            ),
        )
        if result is not None:
            results.append(result)
            refreshed += 1
    commit_with_retry(session)
    cleanup_stale_cover_images(results)
    return refreshed


def cleanup_stale_cover_images(results: list[CoverCacheResult]) -> None:
    active_paths = {result.active_path for result in results}
    for result in results:
        for stale_path in result.stale_paths:
            if stale_path in active_paths:
                continue
            try:
                stale_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("failed to remove stale cover %s: %s", stale_path, exc)


def cover_url_hash(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


async def download_cover_image(url: str) -> tuple[bytes | None, str]:
    async with httpx.AsyncClient(
        timeout=settings.request_timeout_seconds,
        headers={"User-Agent": settings.user_agent},
        follow_redirects=True,
    ) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            if not content_type.startswith("image/"):
                return None, content_type
            content_length = response.headers.get("content-length")
            if content_length:
                try:
                    too_large = int(content_length) > settings.max_cover_bytes
                except ValueError:
                    too_large = False
                if too_large:
                    raise RuntimeError(
                        f"cover image exceeds max_cover_bytes ({settings.max_cover_bytes}) for {url}"
                    )
            content = bytearray()
            async for chunk in response.aiter_bytes():
                content.extend(chunk)
                if len(content) > settings.max_cover_bytes:
                    raise RuntimeError(
                        f"cover image exceeds max_cover_bytes ({settings.max_cover_bytes}) for {url}"
                    )
    return bytes(content), content_type


async def trigger_kavita_scan() -> None:
    await configured_kavita_client().scan_all()


async def sync_series_with_kavita(
    session: Session, series: Series, folder_path: Path | None = None
) -> bool:
    client = configured_kavita_client()
    if not client.configured:
        return False
    if folder_path is not None:
        await client.scan_folder_or_all(folder_path)
    kavita_series = await client.list_series()
    match = match_kavita_series(series, kavita_series, folder_path)
    if match is None:
        if series.kavita_series_id:
            clear_kavita_mapping(series)
            commit_with_retry(session)
        return False
    series.kavita_series_id = match.id
    series.kavita_library_id = match.library_id
    series.kavita_synced_at = utcnow()
    chapters = await client.series_detail(match.id)
    chapters_by_number = {normalize_chapter_number(chapter.number): chapter for chapter in chapters}
    chapter_progress = (
        getattr(client, "chapter_progress", None)
        if settings.kavita_sync_read_progress
        else None
    )
    for chapter in series.chapters:
        kavita_chapter = chapters_by_number.get(normalize_chapter_number(chapter.number))
        if kavita_chapter is None:
            if chapter.kavita_chapter_id:
                chapter.kavita_chapter_id = None
                chapter.kavita_volume_id = None
                chapter.kavita_mapped_at = None
            continue
        chapter.kavita_chapter_id = kavita_chapter.id
        chapter.kavita_volume_id = kavita_chapter.volume_id
        chapter.kavita_mapped_at = utcnow()
        kavita_progress = (
            await chapter_progress(kavita_chapter.id, kavita_chapter.pages_total)
            if chapter_progress is not None
            else None
        )
        if (
            kavita_progress is not None
            and kavita_progress.pages_total > 0
            and kavita_progress.pages_read >= kavita_progress.pages_total
        ):
            apply_chapter_progress(session, chapter.id, "read")
    commit_with_retry(session)
    return True


def match_kavita_series(
    series: Series, candidates: list[KavitaSeries], folder_path: Path | None = None
) -> KavitaSeries | None:
    if series.kavita_series_id:
        for candidate in candidates:
            if candidate.id == series.kavita_series_id:
                return candidate
    if series.chapters:
        folders = kavita_match_folders(series, folder_path)
        for candidate in candidates:
            if candidate.folder_path and local_path_for_kavita(candidate.folder_path).resolve() in folders:
                return candidate
    external_ids = decode_external_ids(series.external_ids)
    for candidate in candidates:
        if candidate.mal_id and external_ids.get("mal") == candidate.mal_id:
            return candidate
        if candidate.anilist_id and external_ids.get("anilist") == candidate.anilist_id:
            return candidate
    normalized = normalize_title(series.title)
    title_matches = [candidate for candidate in candidates if normalize_title(candidate.name) == normalized]
    return title_matches[0] if len(title_matches) == 1 else None


def kavita_match_folders(series: Series, folder_path: Path | None = None) -> set[Path]:
    folders: list[Path] = []
    if folder_path is not None:
        folders.append(folder_path)
    folders.append(kavita_sync_folder_for_series(series))
    folders.append(settings.library_root / "Manga" / slugify(series.title))
    return {folder.resolve() for folder in folders}


def clear_kavita_mapping(series: Series) -> None:
    series.kavita_series_id = None
    series.kavita_library_id = None
    series.kavita_synced_at = None
    for chapter in series.chapters:
        chapter.kavita_chapter_id = None
        chapter.kavita_volume_id = None
        chapter.kavita_mapped_at = None


def kavita_series_url(series: Series) -> str:
    if not series.kavita_series_id or not series.kavita_library_id:
        return ""
    return configured_kavita_client().series_url(series.kavita_library_id, series.kavita_series_id)


def kavita_chapter_url(chapter: Chapter) -> str:
    series = chapter.series
    if not series.kavita_series_id or not series.kavita_library_id or not chapter.kavita_chapter_id:
        return ""
    return configured_kavita_client().chapter_url(
        series.kavita_library_id,
        series.kavita_series_id,
        chapter.kavita_chapter_id,
    )


def retry_failed_downloads(session: Session) -> int:
    count = 0
    for job in session.scalars(select(DownloadJob).where(DownloadJob.status == "failed")):
        job.status = "queued"
        job.error = ""
        job.attempts = 0
        job.retry_after = None
        job.updated_at = utcnow()
        count += 1
    commit_with_retry(session)
    return count


def retry_download_job(session: Session, job_id: int) -> bool:
    job = session.get(DownloadJob, job_id)
    if job is None or job.status != "failed":
        return False
    job.status = "queued"
    job.error = ""
    job.attempts = 0
    job.retry_after = None
    job.updated_at = utcnow()
    commit_with_retry(session)
    return True


def retry_failed_kavita_syncs(session: Session) -> int:
    count = 0
    for job in session.scalars(
        select(KavitaSyncJob).where(
            or_(
                KavitaSyncJob.status == "failed",
                (KavitaSyncJob.status == "skipped")
                & (KavitaSyncJob.error == "Kavita is not configured"),
            )
        )
    ):
        job.status = "queued"
        job.error = ""
        job.attempts = 0
        job.retry_after = None
        job.updated_at = utcnow()
        count += 1
    commit_with_retry(session)
    return count


def retry_kavita_sync_job(session: Session, job_id: int) -> bool:
    job = session.get(KavitaSyncJob, job_id)
    if job is None or job.status not in {"failed", "skipped"}:
        return False
    job.status = "queued"
    job.error = ""
    job.attempts = 0
    job.retry_after = None
    job.updated_at = utcnow()
    commit_with_retry(session)
    return True


async def drain_download_queue(session: Session) -> int:
    if settings.download_concurrency <= 1:
        count = 0
        while await run_next_download(session):
            count += 1
        return count
    from app.db import SessionLocal

    lock = asyncio.Lock()
    count = 0

    async def worker() -> None:
        nonlocal count
        while True:
            with SessionLocal() as worker_session:
                processed = await run_next_download(worker_session)
            if not processed:
                return
            async with lock:
                count += 1

    await asyncio.gather(*(worker() for _ in range(settings.download_concurrency)))
    return count


async def drain_kavita_sync_queue(session: Session) -> int:
    queue_pending_kavita_syncs(session)
    count = 0
    while await run_next_kavita_sync(session):
        count += 1
    return count


async def rescan_source_series(session: Session, source_series_id: int) -> int:
    source_series = session.get(SourceSeries, source_series_id)
    if source_series is None:
        return 0
    if source_series.source not in enabled_source_names():
        raise RuntimeError(f"source {source_series.source} is disabled or unavailable")
    adapter = adapter_for_source(source_series.source)
    if adapter is None:
        raise RuntimeError(f"source {source_series.source} is disabled or unavailable")
    item = SeriesItem(
        source=source_series.source,
        source_id=source_series.source_id,
        title=source_series.title,
        url=source_series.url,
        aliases=tuple(value for value in (source_series.aliases or "").split("|") if value),
        description=source_series.description,
        cover_url=source_series.cover_url,
        genres=tuple(value for value in (source_series.genres or "").split(",") if value),
        popularity=source_series.popularity,
        external_ids=decode_external_ids(source_series.external_ids),
    )
    try:
        chapters = await adapter.get_chapters(item)
        with session.begin_nested():
            for chapter in chapters:
                upsert_release(session, source_series, chapter)
            source_series.detail_fetched_at = utcnow()
            source_series.last_checked_at = utcnow()
        commit_with_retry(session)
        return len(chapters)
    finally:
        await close_adapter(adapter)


def merge_match_candidate(session: Session, candidate_id: int) -> bool:
    candidate = session.get(MatchCandidate, candidate_id)
    if candidate is None or candidate.status != "pending":
        return False
    source_series = session.get(SourceSeries, candidate.source_series_id)
    target = session.get(Series, candidate.candidate_series_id)
    if source_series is None or target is None:
        return False
    old_series_id = source_series.series_id
    old_series = session.get(Series, old_series_id)
    moving_source_ids = [
        row[0]
        for row in session.execute(
            select(SourceSeries.id).where(SourceSeries.series_id == old_series_id)
        )
    ]
    for source_row in session.scalars(
        select(SourceSeries).where(SourceSeries.series_id == old_series_id)
    ):
        source_row.series_id = target.id
    for chapter in session.scalars(select(Chapter).where(Chapter.series_id == old_series_id)):
        existing = session.scalar(
            select(Chapter).where(Chapter.series_id == target.id, Chapter.number == chapter.number)
        )
        if existing:
            for release in chapter.releases:
                release.chapter_id = existing.id
                release.chapter = existing
            recompute_chapter_best_source(existing)
            session.delete(chapter)
        else:
            chapter.series_id = target.id
            recompute_chapter_best_source(chapter)
    for related in session.scalars(
        select(MatchCandidate).where(
            or_(
                MatchCandidate.candidate_series_id == old_series_id,
                MatchCandidate.source_series_id.in_(moving_source_ids),
            )
        )
    ):
        if related.id == candidate.id:
            related.status = "merged"
        else:
            session.delete(related)
    for rule in session.scalars(
        select(ManualMatchRule).where(ManualMatchRule.target_series_id == old_series_id)
    ):
        rule.target_series_id = target.id
    session.add(
        ManualMatchRule(
            source_series_id=source_series.id,
            target_series_id=target.id,
            action="merge",
        )
    )
    candidate.status = "merged"
    if old_series is not None and old_series.id != target.id:
        session.flush()
        session.delete(old_series)
    commit_with_retry(session)
    return True


def keep_match_candidate_separate(session: Session, candidate_id: int) -> bool:
    candidate = session.get(MatchCandidate, candidate_id)
    if candidate is None or candidate.status != "pending":
        return False
    session.add(
        ManualMatchRule(
            source_series_id=candidate.source_series_id,
            target_series_id=candidate.candidate_series_id,
            action="separate",
        )
    )
    candidate.status = "separate"
    commit_with_retry(session)
    return True


def set_series_progress(
    session: Session,
    series_id: int,
    status: str,
    *,
    note: str = "",
    rating: int | None = None,
) -> SeriesProgress | None:
    progress = apply_series_progress(session, series_id, status, note=note, rating=rating)
    commit_with_retry(session)
    return progress


def apply_series_progress(
    session: Session,
    series_id: int,
    status: str,
    *,
    note: str = "",
    rating: int | None = None,
) -> SeriesProgress | None:
    if status not in SERIES_PROGRESS_STATUSES:
        status = "interested"
    series = session.get(Series, series_id)
    if series is None:
        return None
    progress = session.scalar(select(SeriesProgress).where(SeriesProgress.series_id == series_id))
    if progress is None:
        progress = SeriesProgress(series_id=series_id)
        session.add(progress)
        session.flush()
    progress.status = status
    progress.note = note
    progress.rating = rating
    progress.updated_at = utcnow()
    if status in {"interested", "reading"}:
        series.status = status
    record_activity(
        session,
        "series_progress",
        "info",
        f"{series.title} marked {status}",
        series_id=series.id,
    )
    return progress


def set_chapter_progress(session: Session, chapter_id: int, status: str) -> ChapterProgress | None:
    progress = apply_chapter_progress(session, chapter_id, status)
    commit_with_retry(session)
    return progress


def apply_chapter_progress(
    session: Session, chapter_id: int, status: str
) -> ChapterProgress | None:
    if status not in CHAPTER_PROGRESS_STATUSES:
        status = "unread"
    chapter = session.get(Chapter, chapter_id)
    if chapter is None:
        return None
    progress = session.scalar(select(ChapterProgress).where(ChapterProgress.chapter_id == chapter_id))
    if progress is None:
        progress = ChapterProgress(chapter_id=chapter_id)
        session.add(progress)
        session.flush()
    progress.status = status
    progress.read_at = utcnow() if status == "read" else None
    progress.updated_at = utcnow()
    if status in {"reading", "read"}:
        apply_series_progress(session, chapter.series_id, "reading")
    record_activity(
        session,
        "chapter_progress",
        "info",
        f"{chapter.series.title} chapter {chapter.number} marked {status}",
        series_id=chapter.series_id,
        chapter_id=chapter.id,
    )
    return progress


def mark_series_caught_up(session: Session, series_id: int) -> int:
    series = session.get(Series, series_id)
    if series is None:
        return 0
    count = 0
    for chapter in series.chapters:
        if apply_chapter_progress(session, chapter.id, "read") is not None:
            count += 1
    apply_series_progress(session, series_id, "caught_up")
    commit_with_retry(session)
    return count


async def sync_kavita_want_to_read(session: Session) -> int:
    client = configured_kavita_client()
    if not client.configured:
        return 0
    wanted = await client.want_to_read()
    wanted_ids = {item.id for item in wanted}
    count = 0
    for series in session.scalars(select(Series).where(Series.kavita_series_id.in_(wanted_ids))):
        apply_series_progress(session, series.id, "interested")
        count += 1
    record_activity(
        session,
        "kavita_want_to_read",
        "success",
        f"Imported {count} Kavita Want to Read series",
        metadata={"count": count},
    )
    commit_with_retry(session)
    return count


def cleanup_replaced_files(session: Session) -> int:
    if settings.retention_replaced_days <= 0 and settings.retention_replaced_max_per_chapter <= 0:
        return 0
    cutoff = (
        utcnow() - timedelta(days=settings.retention_replaced_days)
        if settings.retention_replaced_days > 0
        else None
    )
    removed = 0
    rows = session.scalars(
        select(DownloadedFile)
        .where(DownloadedFile.active.is_(False))
        .order_by(DownloadedFile.chapter_id, DownloadedFile.replaced_at.desc().nullslast())
    ).all()
    by_chapter: dict[int, list[DownloadedFile]] = {}
    for row in rows:
        by_chapter.setdefault(row.chapter_id, []).append(row)
    for chapter_rows in by_chapter.values():
        for index, row in enumerate(chapter_rows):
            too_many = (
                settings.retention_replaced_max_per_chapter > 0
                and index >= settings.retention_replaced_max_per_chapter
            )
            too_old = bool(cutoff and row.replaced_at and ensure_aware(row.replaced_at) < cutoff)
            if not too_many and not too_old:
                continue
            path = Path(row.path)
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                record_activity(
                    session,
                    "retention_cleanup",
                    "warning",
                    f"Failed to remove {path}: {exc}",
                    chapter_id=row.chapter_id,
                )
                continue
            session.delete(row)
            removed += 1
    record_activity(
        session,
        "retention_cleanup",
        "success",
        f"Removed {removed} replaced files",
        metadata={"removed": removed},
    )
    commit_with_retry(session)
    return removed


def cleanup_bad_discovery_rows(session: Session) -> dict[str, int]:
    removed_chapters = 0
    removed_series = 0
    bad_chapters = session.scalars(
        select(Chapter).where(
            or_(
                Chapter.number.contains("{{"),
                Chapter.number.contains("number"),
                Chapter.title.contains("{{"),
                Chapter.title.contains("number"),
            )
        )
    ).all()
    for chapter in bad_chapters:
        has_files = session.scalar(select(DownloadedFile.id).where(DownloadedFile.chapter_id == chapter.id))
        if has_files:
            continue
        for release in list(chapter.releases):
            session.delete(release)
        session.delete(chapter)
        removed_chapters += 1

    for source_series in session.scalars(select(SourceSeries).where(SourceSeries.source == "kingofshojo")).all():
        title = source_series.title.strip().lower()
        if title not in {"manhwa", "manga", "manhua", "text mode", "list mode"}:
            continue
        series = source_series.series
        has_files = session.scalar(
            select(DownloadedFile.id).join(Chapter).where(Chapter.series_id == series.id)
        )
        if has_files or series.status != "new":
            continue
        for candidate in session.scalars(select(MatchCandidate).where(MatchCandidate.source_series_id == source_series.id)):
            session.delete(candidate)
        session.delete(source_series)
        session.flush()
        remaining_sources = session.scalar(
            select(SourceSeries.id).where(SourceSeries.series_id == series.id).limit(1)
        )
        remaining_chapters = session.scalar(
            select(Chapter.id).where(Chapter.series_id == series.id).limit(1)
        )
        if remaining_sources is None and remaining_chapters is None:
            session.delete(series)
        removed_series += 1
    if removed_chapters or removed_series:
        record_activity(
            session,
            "cleanup",
            "success",
            f"Removed {removed_chapters} bad chapters and {removed_series} pseudo-series",
        )
        commit_with_retry(session)
    return {"chapters": removed_chapters, "series": removed_series}


def is_legacy_bad_chapter(chapter: Chapter) -> bool:
    values = [chapter.number or "", chapter.title or ""]
    joined = " ".join(values).lower()
    if "{{" in joined or "chapter {{number}}" in joined:
        return True
    if (chapter.number or "").strip().lower() in {"first chapter", "latest chapter", "chapter"}:
        return True
    try:
        Decimal(chapter.number)
    except InvalidOperation:
        return not bool(chapter.downloaded_source or chapter.cbz_path)
    return False


def remove_legacy_bad_chapters(session: Session) -> int:
    removed = 0
    for chapter in session.scalars(select(Chapter)).all():
        if not is_legacy_bad_chapter(chapter):
            continue
        has_files = session.scalar(select(DownloadedFile.id).where(DownloadedFile.chapter_id == chapter.id))
        if has_files:
            continue
        for release in list(chapter.releases):
            for job in session.scalars(select(DownloadJob).where(DownloadJob.chapter_release_id == release.id)):
                session.delete(job)
            session.delete(release)
        session.delete(chapter)
        removed += 1
    if removed:
        commit_with_retry(session)
    return removed


async def repair_known_series(session: Session, limit: int = 100) -> dict[str, int]:
    cleanup = cleanup_bad_discovery_rows(session)
    removed_chapters = cleanup["chapters"] + remove_legacy_bad_chapters(session)
    refreshed = 0
    rescanned = 0
    rows = session.scalars(
        select(SourceSeries)
        .join(Series)
        .order_by(
            Series.status.in_(["reading", "interested"]).desc(),
            SourceSeries.last_checked_at.asc(),
            SourceSeries.id.asc(),
        )
        .limit(limit)
    ).all()
    for source_series in rows:
        try:
            count = await rescan_source_series(session, source_series.id)
        except Exception as exc:
            logger.warning("repair rescan failed for source_series=%s: %s", source_series.id, exc)
            continue
        rescanned += 1
        if count:
            refreshed += count
    covers = await refresh_missing_covers(session, limit=limit)
    removed_chapters += remove_legacy_bad_chapters(session)
    result = {
        "rescanned": rescanned,
        "chapters": refreshed,
        "removed_chapters": removed_chapters,
        "removed_series": cleanup["series"],
        "covers": covers,
    }
    record_activity(
        session,
        "repair",
        "success",
        (
            f"Repair rescanned {rescanned} source series, refreshed {refreshed} chapters, "
            f"removed {removed_chapters} bad chapters, refreshed {covers} covers"
        ),
        metadata=result,
    )
    commit_with_retry(session)
    return result


def bulk_update_match_candidates(session: Session, candidate_ids: list[int], action: str) -> int:
    count = 0
    for candidate_id in candidate_ids:
        if action == "merge":
            count += int(merge_match_candidate(session, candidate_id))
        elif action == "separate":
            count += int(keep_match_candidate_separate(session, candidate_id))
    return count


def source_priority_label(source: str) -> int:
    return SOURCE_PRIORITY.get(source, 0)


def encode_external_ids(value: dict[str, str]) -> str:
    cleaned = {str(key): str(val) for key, val in value.items() if key and val}
    return json.dumps(cleaned, sort_keys=True) if cleaned else ""


def decode_external_ids(value: str) -> dict[str, str]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(val) for key, val in payload.items() if key and val}


def merge_external_ids(existing: str, incoming: dict[str, str]) -> str:
    merged = decode_external_ids(existing)
    merged.update({str(key): str(val) for key, val in incoming.items() if key and val})
    return encode_external_ids(merged)


def merge_delimited(existing: str, incoming, delimiter: str) -> str:
    values = [value for value in existing.split(delimiter) if value] if existing else []
    seen = set(values)
    for value in incoming:
        if value and value not in seen:
            values.append(str(value))
            seen.add(str(value))
    return delimiter.join(values)


def shared_external_ids(left: dict[str, str], right: dict[str, str]) -> bool:
    return any(right.get(key) == value for key, value in left.items() if value)


def recover_stale_download_jobs(session: Session) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=settings.download_stale_minutes)
    count = 0
    orphaned = session.scalars(
        select(DownloadJob)
        .outerjoin(ChapterRelease, ChapterRelease.id == DownloadJob.chapter_release_id)
        .outerjoin(Chapter, Chapter.id == ChapterRelease.chapter_id)
        .where(or_(ChapterRelease.id.is_(None), Chapter.id.is_(None)))
    ).all()
    for job in orphaned:
        session.delete(job)
        count += 1
    for job in session.scalars(select(DownloadJob).where(DownloadJob.status == "running")):
        updated_at = ensure_aware(job.updated_at)
        if updated_at and updated_at <= cutoff:
            job.status = "queued"
            job.error = "recovered stale running job"
            job.retry_after = None
            job.updated_at = utcnow()
            count += 1
    if count:
        commit_with_retry(session)
    return count


def recover_stale_kavita_sync_jobs(session: Session) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=settings.download_stale_minutes)
    count = 0
    for job in session.scalars(select(KavitaSyncJob).where(KavitaSyncJob.status == "running")):
        updated_at = ensure_aware(job.updated_at)
        if updated_at and updated_at <= cutoff:
            job.status = "queued"
            job.error = "recovered stale running sync job"
            job.retry_after = None
            job.updated_at = utcnow()
            count += 1
    if count:
        commit_with_retry(session)
    return count


def next_retry_after(attempts: int) -> datetime:
    delay = settings.download_retry_base_minutes * (2 ** max(attempts - 1, 0))
    return datetime.now(timezone.utc) + timedelta(minutes=delay)


def ensure_aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def earliest_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    left = ensure_aware(left)
    right = ensure_aware(right)
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def recompute_chapter_best_source(chapter: Chapter) -> None:
    if not chapter.releases:
        return
    best = max(chapter.releases, key=lambda release: SOURCE_PRIORITY.get(release.source, 0))
    chapter.best_source = best.source
    chapter.updated_at = utcnow()


def validated_image_extension(content: bytes, content_type: str, url: str) -> str:
    try:
        with Image.open(io.BytesIO(content)) as image:
            image.verify()
            image_format = (image.format or "").lower().replace("jpeg", "jpg")
    except Exception as exc:
        raise RuntimeError(f"cover image is not a valid image for {url}") from exc
    if image_format in {"jpg", "png", "webp", "gif"}:
        return image_format
    raise RuntimeError(f"unsupported cover image format {image_format or 'unknown'} for {url}")


def cover_extension(content: bytes, content_type: str, url: str) -> str:
    return validated_image_extension(content, content_type, url)
