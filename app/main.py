from __future__ import annotations

import logging
import asyncio
import secrets
import hashlib
import random
from html import escape
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.adapters import enabled_source_names
from app.db import SessionLocal, get_session, init_db
from app.kavita import configured_kavita_client
from app.models import (
    ActivityEvent,
    Chapter,
    ChapterProgress,
    DownloadJob,
    KavitaSyncJob,
    MatchCandidate,
    Series,
    SeriesProgress,
    SourceHealth,
)
from app.scheduler import create_scheduler
from app.services import (
    FIRST_IMPORT_CHAPTERS,
    drain_download_queue,
    drain_kavita_sync_queue,
    bulk_update_match_candidates,
    cleanup_replaced_files,
    keep_match_candidate_separate,
    kavita_chapter_url,
    kavita_series_url,
    merge_match_candidate,
    newest_chapter_time,
    poll_all_sources,
    poll_source,
    pending_kavita_sync_series_count,
    queue_chapter_download,
    queue_downloads,
    queue_kavita_sync,
    queue_newest_missing_chapters,
    queue_pending_kavita_syncs,
    recover_stale_download_jobs,
    recover_stale_kavita_sync_jobs,
    rescan_source_series,
    retry_download_job,
    retry_failed_downloads,
    retry_failed_kavita_syncs,
    retry_kavita_sync_job,
    run_next_download,
    run_next_kavita_sync,
    set_chapter_progress,
    set_series_progress,
    mark_series_caught_up,
    source_priority_label,
    sync_kavita_want_to_read,
)
from app.settings import settings


logger = logging.getLogger(__name__)
SOURCE_LABELS = {
    "asura": "Asura",
    "mangafire": "MangaFire",
    "kingofshojo": "Kingofshojo",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("starting database migrations")
    init_db()
    logger.info("database migrations complete")
    with SessionLocal() as session:
        recover_stale_download_jobs(session)
        recover_stale_kavita_sync_jobs(session)
    logger.info("stale job recovery complete")
    scheduler = None
    app.state.scheduler = None
    app.state.scheduler_create_error = ""
    app.state.scheduler_start_error = ""
    app.state.scheduler_start_scheduled = False
    try:
        scheduler = create_scheduler()
        logger.info("scheduler created")
        app.state.scheduler = scheduler
    except Exception as exc:
        app.state.scheduler_create_error = str(exc)
        logger.exception("scheduler failed to initialize")

    def start_scheduler() -> None:
        if scheduler is None:
            return
        try:
            scheduler.start()
        except Exception as exc:
            app.state.scheduler_start_error = str(exc)
            logger.exception("scheduler failed to start")

    if scheduler is not None:
        app.state.scheduler_start_scheduled = True
        asyncio.get_running_loop().call_soon(start_scheduler)
        logger.info("scheduler start scheduled")
    yield
    if scheduler is not None and scheduler.running:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Manga Manager", lifespan=lifespan)
cover_dir = settings.library_root / "_covers"
cover_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/covers", StaticFiles(directory=cover_dir), name="covers")
templates = Jinja2Templates(directory="app/templates")
templates.env.filters["priority"] = source_priority_label
templates.env.filters["relative_time"] = lambda value: relative_time(value)
templates.env.globals["csrf_input"] = lambda: csrf_input()


@app.middleware("http")
async def basic_auth_middleware(request: Request, call_next):
    if bool(settings.basic_auth_username) != bool(settings.basic_auth_password):
        return PlainTextResponse("Basic Auth requires username and password", status_code=500)
    if not settings.basic_auth_username and not settings.basic_auth_password:
        return await call_next(request)
    if request.url.path == "/healthz" and not settings.auth_protect_healthz:
        return await call_next(request)
    auth = request.headers.get("authorization", "")
    expected = basic_auth_header(settings.basic_auth_username, settings.basic_auth_password)
    if not secrets.compare_digest(auth, expected):
        return PlainTextResponse(
            "Authentication required",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Manga Manager"'},
        )
    if request.method in {"POST", "PUT", "PATCH", "DELETE"} and not await valid_csrf(request):
        return PlainTextResponse("CSRF token invalid", status_code=403)
    return await call_next(request)


def basic_auth_header(username: str, password: str) -> str:
    import base64

    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def csrf_token() -> str:
    secret = f"{settings.basic_auth_username}:{settings.basic_auth_password}"
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def csrf_input() -> str:
    if not settings.basic_auth_username or not settings.basic_auth_password:
        return ""
    return f'<input type="hidden" name="csrf_token" value="{csrf_token()}">'


async def valid_csrf(request: Request) -> bool:
    token = request.headers.get("x-csrf-token", "")
    if not token:
        try:
            form = await request.form()
        except Exception:
            form = {}
        token = str(form.get("csrf_token", ""))
    return secrets.compare_digest(token, csrf_token())


def relative_time(value) -> str:
    if value is None:
        return "unknown"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    seconds = max(0, int((datetime.now(timezone.utc) - value).total_seconds()))
    if seconds < 60:
        return "now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    if days < 7:
        return f"{days}d ago"
    if days < 14:
        return "last week"
    return value.date().isoformat()


def discovery_item(series: Series) -> dict:
    chapters = sorted(series.chapters, key=newest_chapter_time, reverse=True)
    chapter_rows = []
    seen_numbers = set()
    for chapter in chapters:
        if chapter.number in seen_numbers:
            continue
        seen_numbers.add(chapter.number)
        releases = sorted(
            chapter.releases,
            key=lambda release: (
                source_priority_label(release.source),
                release.published_at or release.first_seen_at,
                release.id,
            ),
            reverse=True,
        )
        release = releases[0] if releases else None
        chapter_rows.append(
            {
                "id": chapter.id,
                "number": chapter.number,
                "title": chapter.title or (release.title if release else ""),
                "source": chapter.best_source or (release.source if release else ""),
                "published_at": (release.published_at or release.first_seen_at) if release else None,
                "kavita_url": kavita_chapter_url(chapter),
            }
        )
        if len(chapter_rows) >= 3:
            break
    return {
        "series": series,
        "chapters": chapter_rows,
        "kavita_url": kavita_series_url(series),
    }


def operations_status(request: Request, session: Session) -> dict:
    scheduler = getattr(request.app.state, "scheduler", None)
    scheduler_create_error = getattr(request.app.state, "scheduler_create_error", "")
    scheduler_error = getattr(request.app.state, "scheduler_start_error", "")
    scheduler_jobs_error = ""
    scheduler_jobs = 0
    if scheduler is not None:
        try:
            scheduler_jobs = len(scheduler.get_jobs())
        except Exception as exc:
            scheduler_jobs_error = str(exc)
    health_rows = {row.source: row for row in session.scalars(select(SourceHealth)).all()}
    enabled_names = enabled_source_names()
    source_names = set(enabled_names)
    sources = []
    for source in enabled_names:
        row = health_rows.get(source)
        sources.append(
            {
                "source": source,
                "enabled": row.enabled if row else True,
                "last_poll_at": row.last_poll_at if row else None,
                "last_error": row.last_error if row else "",
                "consecutive_failures": row.consecutive_failures if row else 0,
            }
        )
    for source, row in sorted(health_rows.items()):
        if source not in source_names:
            sources.append(
                {
                    "source": source,
                    "enabled": row.enabled,
                    "last_poll_at": row.last_poll_at,
                    "last_error": row.last_error,
                    "consecutive_failures": row.consecutive_failures,
                }
            )
    return {
        "ok": not scheduler_create_error and not scheduler_error and not scheduler_jobs_error,
        "scheduler": {
            "configured": scheduler is not None,
            "running": bool(getattr(scheduler, "running", False)),
            "start_scheduled": getattr(request.app.state, "scheduler_start_scheduled", False),
            "create_error": scheduler_create_error,
            "start_error": scheduler_error,
            "jobs": scheduler_jobs,
            "jobs_error": scheduler_jobs_error,
        },
        "enabled_sources": enabled_names,
        "sources": sources,
        "download_jobs": {
            status: count
            for status, count in session.execute(
                select(DownloadJob.status, func.count()).group_by(DownloadJob.status)
            )
        },
        "kavita_sync_jobs": {
            status: count
            for status, count in session.execute(
                select(KavitaSyncJob.status, func.count()).group_by(KavitaSyncJob.status)
            )
        },
        "kavita": {
            "configured": configured_kavita_client().configured,
            "pending_sync_series": pending_kavita_sync_series_count(session),
        },
    }


def request_query_value(request: Request, key: str) -> str:
    if "query_string" not in request.scope:
        return ""
    return request.query_params.get(key, "")


def notice_context(request: Request) -> dict[str, str]:
    notice = request_query_value(request, "notice")
    count = request_query_value(request, "count")
    error = request_query_value(request, "error")
    messages = {
        "download-drain": f"Download drain processed {count or '0'} jobs.",
        "kavita-drain": f"Kavita drain processed {count or '0'} jobs.",
        "rescan": f"Source chapters rescan found {count or '0'} chapters.",
        "rescan-error": f"Source chapters rescan failed: {error or 'unknown error'}",
        "progress": "Progress updated.",
        "caught-up": f"Marked {count or '0'} chapters read.",
        "cleanup": f"Retention cleanup removed {count or '0'} files.",
        "wtr": f"Kavita Want to Read imported {count or '0'} series.",
        "bulk-matches": f"Updated {count or '0'} match candidates.",
    }
    return {"type": notice, "message": messages.get(notice, "")}


def notice_redirect(path: str, notice: str, **params: object) -> RedirectResponse:
    query = {"notice": notice, **{key: value for key, value in params.items() if value != ""}}
    return RedirectResponse(f"{path}?{urlencode(query)}", status_code=303)


def truncate_notice_error(value: str, limit: int = 160) -> str:
    value = " ".join(value.split())
    if len(value) <= limit:
        return value
    return f"{value[: limit - 3]}..."


def library_highlights(series: list[Series], session: Session) -> dict:
    series_ids = [item.id for item in series]
    if not series_ids:
        return {
            "mapped_series": 0,
            "downloaded_chapters": 0,
            "new_this_week": 0,
            "failed_jobs": 0,
            "random_mapped_series": None,
            "reading": 0,
            "pending_sync": 0,
            "unread_ready": 0,
        }
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    mapped_series = sum(1 for item in series if item.kavita_series_id and item.kavita_library_id)
    downloaded_chapters = session.scalar(
        select(func.count())
        .select_from(Chapter)
        .where(Chapter.series_id.in_(series_ids))
        .where(Chapter.downloaded_source != "")
    )
    new_this_week = session.scalar(
        select(func.count())
        .select_from(Chapter)
        .where(Chapter.series_id.in_(series_ids))
        .where(Chapter.created_at >= week_ago)
    )
    failed_downloads = session.scalar(
        select(func.count()).select_from(DownloadJob).where(DownloadJob.status == "failed")
    )
    failed_kavita = session.scalar(
        select(func.count()).select_from(KavitaSyncJob).where(KavitaSyncJob.status == "failed")
    )
    reading = session.scalar(
        select(func.count())
        .select_from(SeriesProgress)
        .where(SeriesProgress.series_id.in_(series_ids))
        .where(SeriesProgress.status == "reading")
    )
    unread_ready = session.scalar(
        select(func.count())
        .select_from(Chapter)
        .outerjoin(ChapterProgress)
        .where(Chapter.series_id.in_(series_ids))
        .where(Chapter.kavita_chapter_id.is_not(None))
        .where((ChapterProgress.status.is_(None)) | (ChapterProgress.status != "read"))
    )
    return {
        "mapped_series": mapped_series,
        "downloaded_chapters": downloaded_chapters or 0,
        "new_this_week": new_this_week or 0,
        "failed_jobs": (failed_downloads or 0) + (failed_kavita or 0),
        "random_mapped_series": random_mapped_series(series),
        "reading": reading or 0,
        "pending_sync": pending_kavita_sync_series_count(session),
        "unread_ready": unread_ready or 0,
    }


def filtered_library_query(request: Request):
    query = select(Series).where(Series.status.in_(["reading", "interested"]))
    filter_value = request.query_params.get("filter", "all")
    if filter_value == "ready":
        query = query.where(Series.kavita_series_id.is_not(None), Series.kavita_library_id.is_not(None))
    elif filter_value == "pending-sync":
        query = query.where(
            Series.chapters.any(
                (Chapter.downloaded_source != "") & (Chapter.kavita_chapter_id.is_(None))
            )
        )
    elif filter_value == "failed":
        failed_series = select(KavitaSyncJob.series_id).where(KavitaSyncJob.status == "failed")
        query = query.where(Series.id.in_(failed_series))
    elif filter_value == "unread":
        query = query.where(
            Series.chapters.any(
                (Chapter.kavita_chapter_id.is_not(None))
                & (~Chapter.progress.has(ChapterProgress.status == "read"))
            )
        )
    elif filter_value == "reading":
        query = query.where(Series.progress.has(SeriesProgress.status == "reading"))
    elif filter_value == "caught-up":
        query = query.where(Series.progress.has(SeriesProgress.status == "caught_up"))
    return query


def random_mapped_series(series: list[Series]) -> Series | None:
    mapped = [item for item in series if item.kavita_series_id and item.kavita_library_id]
    if mapped:
        return random.choice(mapped)
    return None


@app.get("/", response_class=HTMLResponse)
def discovery(request: Request, session: Session = Depends(get_session)):
    series = session.scalars(
        select(Series)
        .where(Series.status == "new")
        .options(
            selectinload(Series.sources),
            selectinload(Series.chapters).selectinload(Chapter.releases),
        )
        .order_by(Series.updated_at.desc())
        .limit(200)
    ).all()
    enabled_sources = enabled_source_names()
    health_rows = {row.source: row for row in session.scalars(select(SourceHealth)).all()}
    health = [
        health_rows.get(source) or SourceHealth(source=source, enabled=True)
        for source in enabled_sources
    ]
    health.extend(row for source, row in sorted(health_rows.items()) if source not in enabled_sources)
    return templates.TemplateResponse(
        request,
        "discovery.html",
        {
            "series": series,
            "discovery_items": [discovery_item(item) for item in series],
            "health": health,
            "enabled_sources": enabled_sources,
            "source_labels": SOURCE_LABELS,
            "pending": request_query_value(request, "pending"),
            "active": "discovery",
        },
    )


@app.get("/library", response_class=HTMLResponse)
def library(request: Request, session: Session = Depends(get_session)):
    highlighted_rows = session.scalars(
        select(Series)
        .where(Series.status.in_(["reading", "interested"]))
        .options(selectinload(Series.chapters))
        .order_by(Series.updated_at.desc())
    ).all()
    rows = session.scalars(
        filtered_library_query(request)
        .options(
            selectinload(Series.sources),
            selectinload(Series.progress),
            selectinload(Series.chapters).selectinload(Chapter.progress),
        )
        .order_by(Series.updated_at.desc())
    ).all()
    jobs = session.scalars(
        select(DownloadJob).order_by(DownloadJob.updated_at.desc()).limit(20)
    ).all()
    kavita_jobs = session.scalars(
        select(KavitaSyncJob).order_by(KavitaSyncJob.updated_at.desc()).limit(20)
    ).all()
    return templates.TemplateResponse(
        request,
        "library.html",
        {
            "series": rows,
            "jobs": jobs,
            "kavita_jobs": kavita_jobs,
            "operations": operations_status(request, session),
            "notice": notice_context(request),
            "highlights": library_highlights(highlighted_rows, session),
            "view": request.query_params.get("view", "list"),
            "filter": request.query_params.get("filter", "all"),
            "active": "library",
        },
    )


@app.get("/activity", response_class=HTMLResponse)
def activity(request: Request, session: Session = Depends(get_session)):
    kind = request.query_params.get("kind", "")
    status = request.query_params.get("status", "")
    query = select(ActivityEvent)
    if kind:
        query = query.where(ActivityEvent.kind == kind)
    if status:
        query = query.where(ActivityEvent.status == status)
    events = session.scalars(query.order_by(ActivityEvent.created_at.desc()).limit(200)).all()
    return templates.TemplateResponse(
        request,
        "activity.html",
        {"events": events, "kind": kind, "status": status, "active": "activity"},
    )


@app.get("/notifications/rss.xml")
def notifications_rss(session: Session = Depends(get_session)):
    if not settings.notification_rss_enabled:
        return Response(status_code=404)
    events = session.scalars(
        select(ActivityEvent).order_by(ActivityEvent.created_at.desc()).limit(50)
    ).all()
    items = "\n".join(
        f"<item><title>{escape(event.kind)} {escape(event.status)}</title>"
        f"<description>{escape(event.message)}</description>"
        f"<pubDate>{event.created_at.strftime('%a, %d %b %Y %H:%M:%S GMT')}</pubDate>"
        f"<guid>activity-{event.id}</guid></item>"
        for event in events
    )
    return Response(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        "<rss version=\"2.0\"><channel><title>Manga Manager Activity</title>"
        "<link>/activity</link><description>Recent Manga Manager events</description>"
        f"{items}</channel></rss>",
        media_type="application/rss+xml",
    )


@app.get("/matches", response_class=HTMLResponse)
def matches(request: Request, session: Session = Depends(get_session)):
    candidates = session.scalars(
        select(MatchCandidate)
        .where(MatchCandidate.status == "pending")
        .order_by(MatchCandidate.confidence.desc())
        .limit(100)
    ).all()
    return templates.TemplateResponse(
        request,
        "matches.html",
        {"candidates": candidates, "active": "matches"},
    )


@app.post("/series/{series_id}/status")
def set_status(series_id: int, status: str = Form(...), session: Session = Depends(get_session)):
    if status not in {"new", "reading", "interested", "ignored"}:
        status = "new"
    series = session.get(Series, series_id)
    if series:
        series.status = status
        session.commit()
        queue_downloads(session)
    return RedirectResponse("/", status_code=303)


@app.post("/series/{series_id}/progress")
def update_series_progress(
    series_id: int,
    status: str = Form(...),
    note: str = Form(""),
    rating: int | None = Form(None),
    session: Session = Depends(get_session),
):
    set_series_progress(session, series_id, status, note=note, rating=rating)
    return notice_redirect("/library", "progress")


@app.post("/series/{series_id}/caught-up")
def caught_up(series_id: int, session: Session = Depends(get_session)):
    count = mark_series_caught_up(session, series_id)
    return notice_redirect("/library", "caught-up", count=count)


@app.post("/chapters/{chapter_id}/progress")
def update_chapter_progress(
    chapter_id: int,
    status: str = Form(...),
    session: Session = Depends(get_session),
):
    set_chapter_progress(session, chapter_id, status)
    return notice_redirect("/library", "progress")


@app.get("/series/{series_id}/open")
async def open_series(series_id: int, session: Session = Depends(get_session)):
    series = session.get(Series, series_id)
    if series is None:
        return RedirectResponse("/", status_code=303)
    url = kavita_series_url(series)
    if url:
        return RedirectResponse(url, status_code=303)
    series.status = "interested"
    session.commit()
    queue_newest_missing_chapters(session, series, FIRST_IMPORT_CHAPTERS)
    if any(chapter.downloaded_source and not chapter.kavita_chapter_id for chapter in series.chapters):
        queue_kavita_sync(session, series)
    return RedirectResponse("/?pending=kavita-import", status_code=303)


@app.get("/chapters/{chapter_id}/open")
async def open_chapter(chapter_id: int, session: Session = Depends(get_session)):
    chapter = session.get(Chapter, chapter_id)
    if chapter is None:
        return RedirectResponse("/", status_code=303)
    url = kavita_chapter_url(chapter)
    if url:
        return RedirectResponse(url, status_code=303)
    chapter.series.status = "interested"
    session.commit()
    if chapter.downloaded_source and not chapter.kavita_chapter_id:
        folder_path = Path(chapter.cbz_path).parent if chapter.cbz_path else None
        queue_kavita_sync(session, chapter.series, folder_path)
    else:
        queue_chapter_download(session, chapter)
    return RedirectResponse("/?pending=kavita-chapter", status_code=303)


@app.post("/poll/{source}")
async def poll(source: str, session: Session = Depends(get_session)):
    if source in enabled_source_names():
        await poll_source(session, source)
    return RedirectResponse("/", status_code=303)


@app.post("/poll-all")
async def poll_all(session: Session = Depends(get_session)):
    results = await poll_all_sources(session)
    ok_count = sum(1 for result in results.values() if result["ok"])
    return RedirectResponse(
        f"/?pending=poll-all&ok={ok_count}&total={len(results)}",
        status_code=303,
    )


@app.post("/queue-downloads")
def queue(session: Session = Depends(get_session)):
    queue_downloads(session)
    return RedirectResponse("/library", status_code=303)


@app.post("/run-download")
async def run_download(session: Session = Depends(get_session)):
    await run_next_download(session)
    return RedirectResponse("/library", status_code=303)


@app.post("/drain-downloads")
async def drain_downloads(session: Session = Depends(get_session)):
    count = await drain_download_queue(session)
    return notice_redirect("/library", "download-drain", count=count)


@app.post("/run-kavita-sync")
async def run_kavita_sync(session: Session = Depends(get_session)):
    queue_pending_kavita_syncs(session)
    await run_next_kavita_sync(session)
    return RedirectResponse("/library", status_code=303)


@app.post("/drain-kavita-sync")
async def drain_kavita_sync(session: Session = Depends(get_session)):
    count = await drain_kavita_sync_queue(session)
    return notice_redirect("/library", "kavita-drain", count=count)


@app.post("/maintenance/cleanup-retention")
def cleanup_retention(session: Session = Depends(get_session)):
    count = cleanup_replaced_files(session)
    return notice_redirect("/library", "cleanup", count=count)


@app.post("/kavita/want-to-read/sync")
async def kavita_want_to_read_sync(session: Session = Depends(get_session)):
    count = await sync_kavita_want_to_read(session)
    return notice_redirect("/library", "wtr", count=count)


@app.post("/retry-failed")
def retry_failed(session: Session = Depends(get_session)):
    retry_failed_downloads(session)
    retry_failed_kavita_syncs(session)
    return RedirectResponse("/library", status_code=303)


@app.post("/download-jobs/{job_id}/retry")
def retry_download(job_id: int, session: Session = Depends(get_session)):
    retry_download_job(session, job_id)
    return RedirectResponse("/library", status_code=303)


@app.post("/kavita-sync-jobs/{job_id}/retry")
def retry_kavita_sync(job_id: int, session: Session = Depends(get_session)):
    retry_kavita_sync_job(session, job_id)
    return RedirectResponse("/library", status_code=303)


@app.post("/source-series/{source_series_id}/rescan")
async def rescan_source_detail(source_series_id: int, session: Session = Depends(get_session)):
    try:
        count = await rescan_source_series(session, source_series_id)
    except Exception as exc:
        logger.warning("source series rescan failed for %s: %s", source_series_id, exc)
        return notice_redirect(
            "/library",
            "rescan-error",
            error=truncate_notice_error(str(exc)),
        )
    return notice_redirect("/library", "rescan", count=count)


@app.post("/sources/{source}/enable")
def enable_source(source: str, session: Session = Depends(get_session)):
    if source not in enabled_source_names():
        return RedirectResponse("/", status_code=303)
    health = session.get(SourceHealth, source) or SourceHealth(source=source)
    health.enabled = True
    health.last_error = ""
    health.consecutive_failures = 0
    session.add(health)
    session.commit()
    return RedirectResponse("/", status_code=303)


@app.post("/sources/{source}/disable")
def disable_source(source: str, session: Session = Depends(get_session)):
    if source not in enabled_source_names():
        return RedirectResponse("/", status_code=303)
    health = session.get(SourceHealth, source) or SourceHealth(source=source)
    health.enabled = False
    health.last_error = "manually disabled"
    health.consecutive_failures = 0
    session.add(health)
    session.commit()
    return RedirectResponse("/", status_code=303)


@app.post("/matches/{candidate_id}/merge")
def merge_candidate(candidate_id: int, session: Session = Depends(get_session)):
    merge_match_candidate(session, candidate_id)
    return RedirectResponse("/matches", status_code=303)


@app.post("/matches/{candidate_id}/separate")
def separate_candidate(candidate_id: int, session: Session = Depends(get_session)):
    keep_match_candidate_separate(session, candidate_id)
    return RedirectResponse("/matches", status_code=303)


@app.post("/matches/bulk")
def bulk_matches(
    action: str = Form(...),
    candidate_ids: list[int] = Form(default=[]),
    session: Session = Depends(get_session),
):
    count = bulk_update_match_candidates(session, candidate_ids, action)
    return notice_redirect("/matches", "bulk-matches", count=count)


@app.get("/healthz")
def healthz(request: Request, session: Session = Depends(get_session)):
    return operations_status(request, session)
