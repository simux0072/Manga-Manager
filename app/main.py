from __future__ import annotations

import logging
import asyncio
import json
import secrets
import hashlib
import random
import time
from html import escape
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlencode

from fastapi import Depends, FastAPI, Form, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
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
    ChapterRelease,
    DownloadJob,
    KavitaSyncJob,
    MatchCandidate,
    Series,
    SeriesProgress,
    SourceHealth,
    SourcePullJob,
)
from app.scheduler import create_scheduler
from app.services import (
    FIRST_IMPORT_CHAPTERS,
    cleanup_bad_discovery_rows,
    create_pull_job,
    decode_metadata,
    drain_download_queue,
    drain_kavita_sync_queue,
    bulk_update_match_candidates,
    cleanup_replaced_files,
    commit_with_retry,
    keep_match_candidate_separate,
    chapter_platform_release_time,
    chapter_sort_key,
    kavita_chapter_url,
    kavita_series_url,
    merge_match_candidate,
    release_display_time,
    series_latest_platform_release_time,
    pull_status,
    pending_kavita_sync_series_count,
    queue_chapter_download,
    queue_downloads,
    queue_kavita_sync,
    queue_newest_missing_chapters,
    queue_pending_kavita_syncs,
    recover_stale_download_jobs,
    recover_stale_kavita_sync_jobs,
    recover_stale_pull_jobs,
    refresh_missing_covers,
    repair_known_series,
    rescan_source_series,
    retry_download_job,
    retry_failed_downloads,
    retry_failed_kavita_syncs,
    retry_kavita_sync_job,
    run_next_download,
    run_next_kavita_sync,
    run_pull_job,
    run_pull_jobs_limited,
    set_chapter_progress,
    set_series_progress,
    mark_series_caught_up,
    source_priority_label,
    sync_kavita_want_to_read,
)
from app.settings import settings


def configure_logging() -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
    else:
        root.setLevel(logging.INFO)
    for logger_name in ("app", "app.main", "app.services", "app.scheduler"):
        app_logger = logging.getLogger(logger_name)
        app_logger.disabled = False
        app_logger.propagate = True
        app_logger.setLevel(logging.INFO)


configure_logging()
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
        if hasattr(session, "scalars"):
            recover_stale_pull_jobs(session)
            cleanup_bad_discovery_rows(session)
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
templates.env.filters["metadata"] = decode_metadata
templates.env.globals["csrf_input"] = lambda: csrf_input()


def log_background_task(coroutine, description: str) -> asyncio.Task:
    task = asyncio.create_task(coroutine)

    def report_result(done: asyncio.Task) -> None:
        try:
            done.result()
        except asyncio.CancelledError:
            logger.info("background task cancelled: %s", description)
        except Exception:
            logger.exception("background task failed: %s", description)

    task.add_done_callback(report_result)
    logger.info("background task scheduled: %s", description)
    return task


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    started_at = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        logger.exception(
            "request failed method=%s path=%s duration_ms=%.1f",
            request.method,
            request.url.path,
            elapsed_ms,
        )
        raise
    elapsed_ms = (time.perf_counter() - started_at) * 1000
    logger.info(
        "request complete method=%s path=%s status=%s duration_ms=%.1f",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


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
    return value.date().isoformat()


def discovery_item(series: Series) -> dict:
    chapters = sorted(series.chapters, key=chapter_sort_key, reverse=True)
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
                release_display_time(release),
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
                "published_at": chapter_platform_release_time(chapter) if release else None,
                "kavita_url": kavita_chapter_url(chapter),
            }
        )
        if len(chapter_rows) >= 3:
            break
    source_metrics = []
    for source in series.sources:
        metadata = decode_metadata(source.metadata_json)
        metrics = []
        if metadata.get("rating"):
            metrics.append(f"rating {metadata['rating']}")
        if metadata.get("follows"):
            metrics.append(f"{metadata['follows']} bookmarks")
        if metadata.get("rating_count"):
            metrics.append(f"{metadata['rating_count']} ratings")
        if metadata.get("rank"):
            metrics.append(f"rank {metadata['rank']}")
        if metrics:
            source_metrics.append({"source": source.source, "text": " · ".join(metrics)})
    aliases = [value for value in (series.aliases or "").split("|") if value][:3]
    genres = [value for value in (series.genres or "").split(",") if value][:4]
    return {
        "series": series,
        "chapters": chapter_rows,
        "kavita_url": kavita_series_url(series),
        "description": series.description,
        "aliases": aliases,
        "genres": genres,
        "source_metrics": source_metrics,
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
        "covers": f"Refreshed {count or '0'} missing covers.",
        "wtr": f"Kavita Want to Read imported {count or '0'} series.",
        "bulk-matches": f"Updated {count or '0'} match candidates.",
    }
    return {"type": notice, "message": messages.get(notice, "")}


def notice_redirect(path: str, notice: str, **params: object) -> RedirectResponse:
    query = {"notice": notice, **{key: value for key, value in params.items() if value != ""}}
    return RedirectResponse(f"{path}?{urlencode(query)}", status_code=303)


def json_or_redirect(request: Request, payload: dict, path: str, notice: str = "", **params: object):
    if wants_json(request):
        return JSONResponse(payload)
    if notice:
        return notice_redirect(path, notice, **params)
    return RedirectResponse(path, status_code=303)


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
    page = max(1, int(request.query_params.get("page", "1") or "1"))
    page_size = settings.discovery_page_size
    offset = (page - 1) * page_size
    series = session.scalars(
        select(Series)
        .where(Series.status == "new")
        .options(
            selectinload(Series.sources),
            selectinload(Series.chapters).selectinload(Chapter.releases),
        )
    ).all()
    series = sorted(series, key=series_latest_platform_release_time, reverse=True)
    has_next = len(series) > offset + page_size
    series = series[offset : offset + page_size]
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
            "page": page,
            "has_next": has_next,
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
    return templates.TemplateResponse(
        request,
        "library.html",
        {
            "series": rows,
            "notice": notice_context(request),
            "highlights": library_highlights(highlighted_rows, session),
            "enabled_sources": enabled_source_names(),
            "source_labels": SOURCE_LABELS,
            "view": request.query_params.get("view", "list"),
            "filter": request.query_params.get("filter", "all"),
            "active": "library",
        },
    )


@app.get("/info", response_class=HTMLResponse)
def info(request: Request, session: Session = Depends(get_session)):
    return templates.TemplateResponse(
        request,
        "info.html",
        {
            "operations": operations_status(request, session),
            "jobs_payload": jobs_status_payload(session),
            "notice": notice_context(request),
            "active": "info",
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
    groups = {
        "Warnings and errors": [event for event in events if event.status in {"warning", "error"}],
        "Pulls": [event for event in events if event.kind in {"source_poll", "cleanup"}],
        "Downloads": [event for event in events if event.kind == "download"],
        "Kavita": [event for event in events if event.kind.startswith("kavita")],
        "Progress": [event for event in events if "progress" in event.kind],
    }
    return templates.TemplateResponse(
        request,
        "activity.html",
        {"events": events, "groups": groups, "kind": kind, "status": status, "active": "activity"},
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


@app.get("/new-chapters", response_class=HTMLResponse)
def new_chapters(request: Request, session: Session = Depends(get_session)):
    chapters = session.scalars(
        select(Chapter)
        .join(Series)
        .where(Series.status.in_(["reading", "interested"]))
        .where(Chapter.downloaded_source != "")
        .where(~Chapter.progress.has(ChapterProgress.status == "read"))
        .options(
            selectinload(Chapter.series),
            selectinload(Chapter.progress),
            selectinload(Chapter.releases),
        )
    ).all()
    chapters = sorted(chapters, key=chapter_platform_release_time, reverse=True)[:100]
    return templates.TemplateResponse(
        request,
        "new_chapters.html",
        {
            "chapters": chapters,
            "kavita_chapter_url": kavita_chapter_url,
            "chapter_platform_release_time": chapter_platform_release_time,
            "active": "new",
        },
    )


@app.get("/matches", response_class=HTMLResponse)
def matches(request: Request, session: Session = Depends(get_session)):
    candidates = session.scalars(
        select(MatchCandidate)
        .where(MatchCandidate.status == "pending")
        .options(
            selectinload(MatchCandidate.source_series),
            selectinload(MatchCandidate.candidate_series).selectinload(Series.sources),
        )
        .order_by(MatchCandidate.confidence.desc())
        .limit(100)
    ).all()
    return templates.TemplateResponse(
        request,
        "matches.html",
        {"candidates": candidates, "active": "matches"},
    )


@app.post("/series/{series_id}/status")
def set_status(
    series_id: int,
    request: Request,
    status: str = Form(...),
    session: Session = Depends(get_session),
):
    if status not in {"new", "reading", "interested", "ignored"}:
        status = "new"
    series = session.get(Series, series_id)
    queued = 0
    if series:
        series.status = status
        commit_with_retry(session)
        if status in {"reading", "interested"}:
            queued = queue_downloads(session)
    return json_or_redirect(
        request,
        {
            "ok": series is not None,
            "series_id": series_id,
            "status": status,
            "queued": queued,
            "message": "Manga queued." if queued else "Manga updated.",
            "remove": status in {"new", "ignored", "interested"},
        },
        "/",
    )


@app.post("/series/{series_id}/progress")
def update_series_progress(
    series_id: int,
    request: Request,
    status: str = Form(...),
    note: str = Form(""),
    rating: int | None = Form(None),
    session: Session = Depends(get_session),
):
    set_series_progress(session, series_id, status, note=note, rating=rating)
    return json_or_redirect(
        request,
        {"ok": True, "series_id": series_id, "status": status, "message": "Progress updated."},
        "/library",
        "progress",
    )


@app.post("/series/{series_id}/caught-up")
def caught_up(series_id: int, request: Request, session: Session = Depends(get_session)):
    count = mark_series_caught_up(session, series_id)
    return json_or_redirect(
        request,
        {
            "ok": True,
            "series_id": series_id,
            "count": count,
            "message": f"Marked {count} chapters read.",
        },
        "/library",
        "caught-up",
        count=count,
    )


@app.post("/chapters/{chapter_id}/progress")
def update_chapter_progress(
    chapter_id: int,
    request: Request,
    status: str = Form(...),
    session: Session = Depends(get_session),
):
    set_chapter_progress(session, chapter_id, status)
    return json_or_redirect(
        request,
        {"ok": True, "chapter_id": chapter_id, "status": status, "message": "Progress updated."},
        "/library",
        "progress",
    )


@app.get("/series/{series_id}/open")
async def open_series(series_id: int, session: Session = Depends(get_session)):
    series = session.get(Series, series_id)
    if series is None:
        return RedirectResponse("/", status_code=303)
    url = kavita_series_url(series)
    if url:
        return RedirectResponse(url, status_code=303)
    series.status = "interested"
    commit_with_retry(session)
    queued = queue_newest_missing_chapters(session, series, FIRST_IMPORT_CHAPTERS)
    logger.info("series queued series_id=%s title=%s queued_chapters=%s", series.id, series.title, queued)
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
    commit_with_retry(session)
    if chapter.downloaded_source and not chapter.kavita_chapter_id:
        folder_path = Path(chapter.cbz_path).parent if chapter.cbz_path else None
        queue_kavita_sync(session, chapter.series, folder_path)
        logger.info("chapter queued for Kavita sync chapter_id=%s series_id=%s", chapter.id, chapter.series_id)
    else:
        queued = queue_chapter_download(session, chapter)
        logger.info(
            "chapter download queued chapter_id=%s series_id=%s chapter=%s queued=%s",
            chapter.id,
            chapter.series_id,
            chapter.number,
            queued,
        )
    return RedirectResponse("/?pending=kavita-chapter", status_code=303)


@app.post("/poll/{source}")
async def poll(source: str, request: Request, session: Session = Depends(get_session)):
    return await pull(source, request, session)


@app.post("/pull/{source}")
async def pull(source: str, request: Request, session: Session = Depends(get_session)):
    if source in enabled_source_names():
        job, created = create_pull_job(session, source)
        if created:
            log_background_task(run_pull_job(job.id), f"pull source={source} job_id={job.id}")
            if wants_json(request):
                return JSONResponse(
                    {
                        "ok": True,
                        "created": True,
                        "message": f"Queued {SOURCE_LABELS.get(source, source)} pull.",
                        "job": pull_job_payload(job),
                    }
                )
            return RedirectResponse("/", status_code=303)
        if wants_json(request):
            return JSONResponse(
                {
                    "ok": True,
                    "created": False,
                    "message": f"{SOURCE_LABELS.get(source, source)} is already pulling.",
                    "job": pull_job_payload(job),
                }
            )
        return RedirectResponse("/?pending=pull-existing", status_code=303)
    if wants_json(request):
        return JSONResponse({"ok": False, "message": "Source is not enabled."}, status_code=400)
    return RedirectResponse("/", status_code=303)


@app.post("/poll-all")
async def poll_all(request: Request, session: Session = Depends(get_session)):
    return await pull_all(request, session)


@app.post("/pull-all")
async def pull_all(request: Request, session: Session = Depends(get_session)):
    created = 0
    job_ids = []
    jobs = []
    include_payload = wants_json(request)
    for source in enabled_source_names():
        job, job_created = create_pull_job(session, source)
        if job_created:
            created += 1
            job_ids.append(job.id)
        if include_payload:
            jobs.append(pull_job_payload(job))
    if job_ids:
        log_background_task(run_pull_jobs_limited(job_ids), f"pull-all job_ids={job_ids}")
    if wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "created": created,
                "jobs": jobs,
                "message": f"Queued {created} pull jobs." if created else "Sources are already pulling.",
            }
        )
    return RedirectResponse(
        "/?pending=pull-all" if created else "/?pending=pull-existing",
        status_code=303,
    )


@app.get("/api/pull-status")
def api_pull_status(session: Session = Depends(get_session)):
    return pull_status(session)


def wants_json(request: Request) -> bool:
    return "application/json" in request.headers.get("accept", "")


def iso_time(value) -> str:
    return value.isoformat() if value else ""


def download_job_payload(job: DownloadJob, session: Session) -> dict:
    release = session.get(ChapterRelease, job.chapter_release_id)
    chapter = release.chapter if release else None
    series = chapter.series if chapter else None
    return {
        "id": job.id,
        "kind": "download",
        "status": job.status,
        "attempts": job.attempts,
        "priority": job.priority,
        "job_type": job.job_type,
        "error": job.error,
        "retry_after": iso_time(job.retry_after),
        "created_at": iso_time(job.created_at),
        "updated_at": iso_time(job.updated_at),
        "retryable": job.status == "failed",
        "chapter_release_id": job.chapter_release_id,
        "source": release.source if release else "",
        "series_id": series.id if series else None,
        "series_title": series.title if series else "",
        "chapter_id": chapter.id if chapter else None,
        "chapter_number": chapter.number if chapter else "",
    }


ACTIVE_JOB_STATUSES = {"queued", "running", "delayed"}
PROCESSED_DOWNLOAD_STATUSES = {"complete", "failed", "skipped"}
INFO_JOB_STATUSES = {
    "active": ("queued", "running", "delayed"),
    "failed": ("failed",),
    "completed": ("complete", "skipped"),
}


def download_group_status(chapter_jobs: list[dict]) -> str:
    statuses = {job["status"] for job in chapter_jobs}
    if "running" in statuses:
        return "running"
    if "queued" in statuses:
        return "queued"
    if "delayed" in statuses:
        return "delayed"
    if "failed" in statuses:
        return "failed"
    return "complete"


def download_group_payload(series_id: int | None, chapter_jobs: list[dict]) -> dict:
    sorted_jobs = sorted(chapter_jobs, key=lambda job: job.get("updated_at", ""), reverse=True)
    counts = {
        status: sum(1 for job in chapter_jobs if job["status"] == status)
        for status in ("queued", "running", "delayed", "complete", "failed", "skipped")
    }
    processed = sum(1 for job in chapter_jobs if job["status"] in PROCESSED_DOWNLOAD_STATUSES)
    total = len(chapter_jobs)
    latest = sorted_jobs[0] if sorted_jobs else {}
    title = latest.get("series_title") or "Unknown series"
    status = download_group_status(chapter_jobs)
    group_id = series_id if series_id is not None else f"unknown-{latest.get('id', '0')}"
    return {
        "id": group_id,
        "kind": "download_series",
        "status": status,
        "processed": processed,
        "total": total,
        "active": status in ACTIVE_JOB_STATUSES,
        "retryable": any(job.get("retryable") for job in chapter_jobs),
        "series_id": series_id,
        "series_title": title,
        "updated_at": latest.get("updated_at", ""),
        "created_at": min((job.get("created_at", "") for job in chapter_jobs), default=""),
        "error": next((job.get("error", "") for job in sorted_jobs if job.get("error")), ""),
        "counts": counts,
        "chapters": sorted_jobs,
    }


def grouped_download_payloads(jobs: list[DownloadJob], session: Session) -> list[dict]:
    grouped: dict[int | None, list[dict]] = {}
    for job in jobs:
        payload = download_job_payload(job, session)
        grouped.setdefault(payload["series_id"], []).append(payload)
    return sorted(
        (download_group_payload(series_id, chapter_jobs) for series_id, chapter_jobs in grouped.items()),
        key=lambda group: group.get("updated_at", ""),
        reverse=True,
    )


def paged_jobs(items: list[dict], page: int, page_size: int) -> dict:
    total = len(items)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = min(max(page, 1), total_pages)
    start = (page - 1) * page_size
    return {
        "jobs": items[start : start + page_size],
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
    }


def safe_positive_int(value: str | None, default: int) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def info_section_jobs(session: Session, section: str) -> list[dict]:
    statuses = INFO_JOB_STATUSES[section]
    downloads = session.scalars(
        select(DownloadJob)
        .where(DownloadJob.status.in_(statuses))
        .order_by(DownloadJob.updated_at.desc(), DownloadJob.id.desc())
    ).all()
    download_groups = grouped_download_payloads(downloads, session)
    kavita_jobs = [
        kavita_job_payload(job)
        for job in session.scalars(
            select(KavitaSyncJob)
            .where(KavitaSyncJob.status.in_(statuses))
            .order_by(KavitaSyncJob.updated_at.desc(), KavitaSyncJob.id.desc())
        ).all()
    ]
    pull_jobs = [
        pull_job_payload(job)
        for job in session.scalars(
            select(SourcePullJob)
            .where(SourcePullJob.status.in_(statuses))
            .order_by(SourcePullJob.updated_at.desc(), SourcePullJob.id.desc())
        ).all()
    ]
    return sorted(
        [*download_groups, *kavita_jobs, *pull_jobs],
        key=lambda job: job.get("updated_at", ""),
        reverse=True,
    )


def kavita_job_payload(job: KavitaSyncJob) -> dict:
    series = job.series
    return {
        "id": job.id,
        "kind": "kavita",
        "status": job.status,
        "attempts": job.attempts,
        "error": job.error,
        "retry_after": iso_time(job.retry_after),
        "created_at": iso_time(job.created_at),
        "updated_at": iso_time(job.updated_at),
        "retryable": job.status in {"failed", "skipped"},
        "series_id": job.series_id,
        "series_title": series.title if series else "",
        "folder_path": job.folder_path,
    }


def pull_job_payload(job: SourcePullJob) -> dict:
    return {
        "id": job.id,
        "kind": "pull",
        "source": job.source,
        "status": job.status,
        "processed": job.processed_items,
        "total": job.total_items,
        "error": job.error,
        "created_at": iso_time(job.created_at),
        "updated_at": iso_time(job.updated_at),
        "completed_at": iso_time(job.completed_at),
        "retryable": False,
    }


def jobs_status_payload(
    session: Session,
    *,
    info_pages: dict[str, int] | None = None,
    info_page_size: int | None = None,
) -> dict:
    recover_stale_download_jobs(session)
    recover_stale_kavita_sync_jobs(session)
    group_limit = settings.job_status_group_limit
    download_rows: dict[int, DownloadJob] = {}
    for query in (
        select(DownloadJob)
        .where(DownloadJob.status.in_(["running", "delayed"]))
        .order_by(DownloadJob.updated_at.desc(), DownloadJob.id.desc())
        .limit(group_limit * 4),
        select(DownloadJob)
        .where(DownloadJob.status == "queued")
        .order_by(DownloadJob.priority.asc(), DownloadJob.created_at.asc(), DownloadJob.id.asc())
        .limit(group_limit * 4),
        select(DownloadJob)
        .where(DownloadJob.status == "failed")
        .order_by(DownloadJob.updated_at.desc(), DownloadJob.id.desc())
        .limit(group_limit * 2),
        select(DownloadJob)
        .where(DownloadJob.status.in_(["complete", "skipped"]))
        .order_by(DownloadJob.updated_at.desc(), DownloadJob.id.desc())
        .limit(group_limit * 2),
    ):
        for job in session.scalars(query):
            download_rows[job.id] = job
    downloads = list(download_rows.values())
    kavita_jobs = session.scalars(
        select(KavitaSyncJob).order_by(KavitaSyncJob.updated_at.desc(), KavitaSyncJob.id.desc()).limit(25)
    ).all()
    pull_jobs = session.scalars(
        select(SourcePullJob).order_by(SourcePullJob.updated_at.desc(), SourcePullJob.id.desc()).limit(25)
    ).all()
    active_pulls = [job for job in pull_jobs if job.status in {"queued", "running"}]
    download_groups = grouped_download_payloads(downloads, session)
    pull_total = sum(job.total_items for job in active_pulls)
    pull_processed = sum(job.processed_items for job in active_pulls)
    download_counts = {
        status: count
        for status, count in session.execute(
            select(DownloadJob.status, func.count()).group_by(DownloadJob.status)
        )
    }
    active_downloads = sum(download_counts.get(status, 0) for status in ("queued", "running", "delayed"))
    active_kavita = sum(1 for job in kavita_jobs if job.status in {"queued", "running"})
    payload = {
        "overall": {
            "active": bool(active_pulls or active_downloads or active_kavita),
            "processed": pull_processed,
            "total": pull_total,
            "active_downloads": active_downloads,
            "active_kavita": active_kavita,
        },
        "pulls": [pull_job_payload(job) for job in pull_jobs],
        "downloads": download_groups,
        "kavita": [kavita_job_payload(job) for job in kavita_jobs],
        "counts": {
            "downloads": download_counts,
            "kavita": {
                status: count
                for status, count in session.execute(
                    select(KavitaSyncJob.status, func.count()).group_by(KavitaSyncJob.status)
                )
            },
            "pulls": {
                status: count
                for status, count in session.execute(
                    select(SourcePullJob.status, func.count()).group_by(SourcePullJob.status)
                )
            },
        },
    }
    if info_pages is not None:
        page_size = min(max(info_page_size or settings.job_status_group_limit, 1), 100)
        payload["sections"] = {
            section: paged_jobs(info_section_jobs(session, section), info_pages.get(section, 1), page_size)
            for section in INFO_JOB_STATUSES
        }
    return payload


@app.get("/api/jobs/status")
def api_jobs_status(request: Request, session: Session = Depends(get_session)):
    params = request.query_params
    wants_sections = any(key in params for key in ("active_page", "failed_page", "completed_page", "page_size"))
    info_pages = None
    if wants_sections:
        info_pages = {
            "active": safe_positive_int(params.get("active_page"), 1),
            "failed": safe_positive_int(params.get("failed_page"), 1),
            "completed": safe_positive_int(params.get("completed_page"), 1),
        }
    page_size = safe_positive_int(params.get("page_size"), settings.job_status_group_limit)
    return jobs_status_payload(session, info_pages=info_pages, info_page_size=page_size)


def sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str, separators=(',', ':'))}\n\n"


def job_identity(job: dict) -> str:
    return f"{job.get('kind')}:{job.get('id')}"


@app.get("/api/jobs/events")
async def api_jobs_events(request: Request):
    async def stream():
        seen: set[str] = set()
        with SessionLocal() as session:
            payload = jobs_status_payload(session)
        for collection in ("pulls", "downloads", "kavita"):
            seen.update(job_identity(job) for job in payload.get(collection, []))
        yield sse_event("snapshot", payload)
        while not await request.is_disconnected():
            await asyncio.sleep(2)
            with SessionLocal() as session:
                payload = jobs_status_payload(session)
            current: set[str] = set()
            new_jobs: list[dict] = []
            for collection in ("pulls", "downloads", "kavita"):
                for job in payload.get(collection, []):
                    identity = job_identity(job)
                    current.add(identity)
                    if identity not in seen:
                        new_jobs.append(job)
            for job in new_jobs:
                yield sse_event("new-job", {"job": job})
            seen.update(current)
            yield sse_event("snapshot", payload)
        yield sse_event("heartbeat", {"ok": True})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/queue-downloads")
def queue(request: Request, session: Session = Depends(get_session)):
    count = queue_downloads(session)
    return json_or_redirect(
        request,
        {"ok": True, "queued": count, "message": f"Queued {count} downloads."},
        "/info",
    )


@app.post("/run-download")
async def run_download(request: Request, session: Session = Depends(get_session)):
    processed = await run_next_download(session)
    return json_or_redirect(
        request,
        {"ok": True, "processed": processed, "message": "Download job processed." if processed else "No ready download job."},
        "/info",
    )


@app.post("/drain-downloads")
async def drain_downloads(request: Request, session: Session = Depends(get_session)):
    count = await drain_download_queue(session)
    return json_or_redirect(
        request,
        {"ok": True, "count": count, "message": f"Download drain processed {count} jobs."},
        "/info",
        "download-drain",
        count=count,
    )


@app.post("/run-kavita-sync")
async def run_kavita_sync(request: Request, session: Session = Depends(get_session)):
    queue_pending_kavita_syncs(session)
    processed = await run_next_kavita_sync(session)
    return json_or_redirect(
        request,
        {"ok": True, "processed": processed, "message": "Kavita sync processed." if processed else "No ready Kavita sync job."},
        "/info",
    )


@app.post("/drain-kavita-sync")
async def drain_kavita_sync(request: Request, session: Session = Depends(get_session)):
    count = await drain_kavita_sync_queue(session)
    return json_or_redirect(
        request,
        {"ok": True, "count": count, "message": f"Kavita drain processed {count} jobs."},
        "/info",
        "kavita-drain",
        count=count,
    )


@app.post("/maintenance/cleanup-retention")
def cleanup_retention(request: Request, session: Session = Depends(get_session)):
    count = cleanup_replaced_files(session)
    return json_or_redirect(
        request,
        {"ok": True, "count": count, "message": f"Retention cleanup removed {count} files."},
        "/info",
        "cleanup",
        count=count,
    )


@app.post("/maintenance/refresh-missing-covers")
async def refresh_covers(request: Request, session: Session = Depends(get_session)):
    count = await refresh_missing_covers(session)
    return json_or_redirect(
        request,
        {"ok": True, "count": count, "message": f"Refreshed {count} missing covers."},
        "/info",
        "covers",
        count=count,
    )


@app.post("/maintenance/repair-known-series")
async def repair_series(request: Request, session: Session = Depends(get_session)):
    result = await repair_known_series(session)
    message = (
        f"Repair rescanned {result['rescanned']} source series, "
        f"removed {result['removed_chapters']} bad chapters, "
        f"refreshed {result['covers']} covers."
    )
    return json_or_redirect(
        request,
        {"ok": True, **result, "message": message},
        "/info",
        "repair",
        count=result["rescanned"],
    )


@app.post("/kavita/want-to-read/sync")
async def kavita_want_to_read_sync(request: Request, session: Session = Depends(get_session)):
    count = await sync_kavita_want_to_read(session)
    return json_or_redirect(
        request,
        {"ok": True, "count": count, "message": f"Kavita Want to Read imported {count} series."},
        "/info",
        "wtr",
        count=count,
    )


@app.post("/retry-failed")
def retry_failed(request: Request, session: Session = Depends(get_session)):
    downloads = retry_failed_downloads(session)
    kavita = retry_failed_kavita_syncs(session)
    if wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "downloads": downloads,
                "kavita": kavita,
                "message": f"Retried {downloads + kavita} failed jobs.",
            }
        )
    return RedirectResponse("/info", status_code=303)


@app.post("/download-jobs/{job_id}/retry")
def retry_download(job_id: int, request: Request, session: Session = Depends(get_session)):
    ok = retry_download_job(session, job_id)
    if wants_json(request):
        job = session.get(DownloadJob, job_id)
        return JSONResponse(
            {
                "ok": ok,
                "job": download_job_payload(job, session) if job else None,
                "message": "Download retry queued." if ok else "Download job was not retryable.",
            }
        )
    return RedirectResponse("/info", status_code=303)


@app.post("/kavita-sync-jobs/{job_id}/retry")
def retry_kavita_sync(job_id: int, request: Request, session: Session = Depends(get_session)):
    ok = retry_kavita_sync_job(session, job_id)
    if wants_json(request):
        job = session.get(KavitaSyncJob, job_id)
        return JSONResponse(
            {
                "ok": ok,
                "job": kavita_job_payload(job) if job else None,
                "message": "Kavita retry queued." if ok else "Kavita job was not retryable.",
            }
        )
    return RedirectResponse("/info", status_code=303)


@app.post("/source-series/{source_series_id}/rescan")
async def rescan_source_detail(
    source_series_id: int,
    request: Request,
    session: Session = Depends(get_session),
):
    try:
        count = await rescan_source_series(session, source_series_id)
    except Exception as exc:
        logger.warning("source series rescan failed for %s: %s", source_series_id, exc)
        if wants_json(request):
            return JSONResponse(
                {
                    "ok": False,
                    "source_series_id": source_series_id,
                    "message": f"Source chapters rescan failed: {truncate_notice_error(str(exc))}",
                },
                status_code=500,
            )
        return notice_redirect(
            "/library",
            "rescan-error",
            error=truncate_notice_error(str(exc)),
        )
    return json_or_redirect(
        request,
        {
            "ok": True,
            "source_series_id": source_series_id,
            "count": count,
            "message": f"Source chapters rescan found {count} chapters.",
        },
        "/library",
        "rescan",
        count=count,
    )


@app.post("/sources/{source}/enable")
def enable_source(source: str, session: Session = Depends(get_session)):
    if source not in enabled_source_names():
        return RedirectResponse("/", status_code=303)
    health = session.get(SourceHealth, source) or SourceHealth(source=source)
    health.enabled = True
    health.last_error = ""
    health.consecutive_failures = 0
    session.add(health)
    commit_with_retry(session)
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
    commit_with_retry(session)
    return RedirectResponse("/", status_code=303)


@app.post("/matches/{candidate_id}/merge")
def merge_candidate(candidate_id: int, request: Request, session: Session = Depends(get_session)):
    try:
        ok = merge_match_candidate(session, candidate_id)
    except Exception as exc:
        logger.exception("match merge failed candidate_id=%s", candidate_id)
        session.rollback()
        candidate = session.get(MatchCandidate, candidate_id)
        session.add(
            ActivityEvent(
                kind="match_merge",
                status="error",
                message=f"Merge failed for candidate {candidate_id}: {truncate_notice_error(str(exc))}",
                source=candidate.source_series.source if candidate and candidate.source_series else "",
                series_id=candidate.candidate_series_id if candidate else None,
                metadata_json=json.dumps(
                    {
                        "candidate_id": candidate_id,
                        "source_series_id": candidate.source_series_id if candidate else None,
                        "target_series_id": candidate.candidate_series_id if candidate else None,
                        "error": str(exc),
                    },
                    sort_keys=True,
                ),
            )
        )
        commit_with_retry(session)
        if wants_json(request):
            return JSONResponse({"ok": False, "message": truncate_notice_error(str(exc))}, status_code=500)
        return notice_redirect("/matches", "merge-error", error=truncate_notice_error(str(exc)))
    if wants_json(request):
        return JSONResponse({"ok": ok, "message": "Merged match." if ok else "Match was not mergeable."})
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
