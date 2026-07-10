from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.orm import Session, aliased, sessionmaker

from manga_manager.infrastructure.database import create_database_engine, create_session_factory
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogExternalIdentifier,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    CatalogSourceState,
    JobEvent,
    JobPermit,
    ChapterArtifact,
    LibraryProjection,
    WorkerHeartbeat,
    WorkJob,
)
from manga_manager.settings import V2Settings


ROOT = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=ROOT / "templates")


def create_app(session_factory: sessionmaker[Session] | None = None) -> FastAPI:
    app = FastAPI(title="Manga Manager", version="2")
    app.mount(
        "/static",
        StaticFiles(directory=Path(__file__).parents[2] / "app" / "static"),
        name="static",
    )

    async def sessions():
        factory = session_factory
        if factory is None:
            settings = V2Settings()
            factory = create_session_factory(
                create_database_engine(settings.require_database_url())
            )
        with factory() as session:
            yield session

    @app.get("/healthz")
    async def health(session: Session = Depends(sessions)) -> dict[str, object]:
        session.execute(select(1))
        return {"ok": True, "architecture": "postgresql-v2"}

    @app.get("/", response_class=HTMLResponse)
    @app.get("/discovery", response_class=HTMLResponse)
    async def discovery(
        request: Request,
        cursor: str = Query(default="", max_length=80),
        q: str = Query(default="", max_length=200),
        source: str = Query(default="", max_length=50),
        session: Session = Depends(sessions),
    ) -> HTMLResponse:
        query = select(CatalogSeries)
        if cursor:
            cursor_time, cursor_id = decode_cursor(cursor)
            if cursor_time is None:
                query = query.where(
                    CatalogSeries.latest_release_at.is_(None), CatalogSeries.id < cursor_id
                )
            else:
                query = query.where(
                    or_(
                        CatalogSeries.latest_release_at < cursor_time,
                        CatalogSeries.latest_release_at.is_(None),
                        (
                            (CatalogSeries.latest_release_at == cursor_time)
                            & (CatalogSeries.id < cursor_id)
                        ),
                    )
                )
        if q.strip():
            term = f"%{q.strip()}%"
            query = query.where(
                or_(CatalogSeries.title.ilike(term), CatalogSeries.normalized_title.ilike(term))
            )
        if source:
            from manga_manager.infrastructure.db_models import CatalogSourceSeries

            query = query.join(
                CatalogSourceSeries, CatalogSourceSeries.series_id == CatalogSeries.id
            ).where(CatalogSourceSeries.source == source)
        rows = session.scalars(
            query.order_by(
                CatalogSeries.latest_release_at.desc().nullslast(), CatalogSeries.id.desc()
            ).limit(25)
        ).all()
        context = {
            "request": request,
            "series": rows,
            "next_cursor": encode_cursor(rows[-1].latest_release_at, rows[-1].id)
            if len(rows) == 25
            else None,
            "q": q,
            "source": source,
            "section": "discovery",
        }
        template = (
            "_series_grid.html" if request.headers.get("HX-Request") == "true" else "discovery.html"
        )
        return templates.TemplateResponse(request, template, context)

    @app.get("/library", response_class=HTMLResponse)
    async def library(
        request: Request,
        view: str = Query(default="grid", pattern="^(grid|list)$"),
        state: str = Query(default=""),
        session: Session = Depends(sessions),
    ) -> HTMLResponse:
        query = select(CatalogSeries).where(CatalogSeries.status != "untracked")
        if state:
            query = query.where(CatalogSeries.status == state)
        rows = session.scalars(
            query.order_by(CatalogSeries.updated_at.desc(), CatalogSeries.id.desc()).limit(100)
        ).all()
        return templates.TemplateResponse(
            request,
            "library.html",
            {"series": rows, "section": "library", "view": view, "state": state},
        )

    @app.get("/updates", response_class=HTMLResponse)
    async def updates(
        request: Request,
        cursor: str = Query(default="", max_length=80),
        session: Session = Depends(sessions),
    ) -> HTMLResponse:
        query = (
            select(CatalogChapterRelease, CatalogChapter, CatalogSeries)
            .join(CatalogChapter, CatalogChapter.id == CatalogChapterRelease.chapter_id)
            .join(CatalogSeries, CatalogSeries.id == CatalogChapter.series_id)
        )
        release_time = func.coalesce(
            CatalogChapterRelease.published_at, CatalogChapterRelease.first_seen_at
        )
        if cursor:
            cursor_time, cursor_id = decode_cursor(cursor)
            if cursor_time is None:
                raise HTTPException(422, "invalid updates cursor")
            query = query.where(
                or_(
                    release_time < cursor_time,
                    (release_time == cursor_time) & (CatalogChapterRelease.id < cursor_id),
                )
            )
        rows = session.execute(
            query.order_by(
                release_time.desc(),
                CatalogChapterRelease.id.desc(),
            ).limit(50)
        ).all()
        return templates.TemplateResponse(
            request,
            "updates.html",
            {
                "rows": rows,
                "next_cursor": encode_cursor(
                    rows[-1][0].published_at or rows[-1][0].first_seen_at,
                    rows[-1][0].id,
                )
                if len(rows) == 50
                else None,
                "section": "updates",
            },
        )

    @app.get("/matches", response_class=HTMLResponse)
    async def matches(request: Request, session: Session = Depends(sessions)) -> HTMLResponse:
        left = aliased(CatalogSourceSeries)
        right = aliased(CatalogSourceSeries)
        rows = session.execute(
            select(CatalogMatchDecision, left, right)
            .join(left, left.id == CatalogMatchDecision.left_source_series_id)
            .join(right, right.id == CatalogMatchDecision.right_source_series_id)
            .where(CatalogMatchDecision.decision == "pending")
            .order_by(CatalogMatchDecision.confidence.desc(), CatalogMatchDecision.id)
            .limit(100)
        ).all()
        return templates.TemplateResponse(
            request, "matches.html", {"rows": rows, "section": "matches"}
        )

    @app.post("/matches/{decision_id}/decision")
    async def decide_match(
        decision_id: int,
        decision: str = Form(),
        confirmation: str = Form(default=""),
        session: Session = Depends(sessions),
    ):
        if decision not in {"accepted", "rejected"}:
            raise HTTPException(422, "invalid match decision")
        if decision == "accepted" and confirmation != "MERGE":
            raise HTTPException(422, "type MERGE to confirm")
        with session.begin():
            row = session.get(CatalogMatchDecision, decision_id)
            if row is None:
                raise HTTPException(404, "match decision not found")
            if decision == "accepted":
                merge_match_groups(session, row)
            row.decision = decision
            row.decided_by = "operator"
            row.decided_at = func.now()
        return RedirectResponse("/matches", status_code=303)

    @app.get("/activity", response_class=HTMLResponse)
    async def activity(
        request: Request,
        event_type: str = Query(default="", max_length=30),
        session: Session = Depends(sessions),
    ) -> HTMLResponse:
        query = select(JobEvent)
        if event_type:
            query = query.where(JobEvent.event_type == event_type)
        events = session.scalars(query.order_by(JobEvent.id.desc()).limit(200)).all()
        return templates.TemplateResponse(
            request,
            "activity.html",
            {"events": events, "event_type": event_type, "section": "activity"},
        )

    @app.post("/series/{series_id}/state")
    async def reading_state(
        series_id: int, state: str = Form(), session: Session = Depends(sessions)
    ):
        allowed = {"untracked", "interested", "reading", "caught_up", "paused"}
        if state not in allowed:
            raise HTTPException(422, "invalid reading state")
        with session.begin():
            series = session.get(CatalogSeries, series_id)
            if series is None:
                raise HTTPException(404, "series not found")
            series.status = state
        return RedirectResponse("/library", status_code=303)

    @app.get("/operations", response_class=HTMLResponse)
    async def operations(request: Request, session: Session = Depends(sessions)) -> HTMLResponse:
        counts = dict(
            session.execute(select(WorkJob.status, func.count()).group_by(WorkJob.status)).all()
        )
        jobs = session.scalars(select(WorkJob).order_by(WorkJob.updated_at.desc()).limit(100)).all()
        sources = session.scalars(
            select(CatalogSourceState).order_by(CatalogSourceState.source)
        ).all()
        workers = session.scalars(
            select(WorkerHeartbeat).order_by(WorkerHeartbeat.heartbeat_at.desc())
        ).all()
        permits = session.scalars(select(JobPermit).order_by(JobPermit.pool, JobPermit.id)).all()
        return templates.TemplateResponse(
            request,
            "operations.html",
            {
                "counts": counts,
                "jobs": jobs,
                "sources": sources,
                "workers": workers,
                "permits": permits,
                "section": "operations",
            },
        )

    @app.get("/events/jobs")
    async def job_events(
        request: Request, after: int = Query(default=0, ge=0)
    ) -> StreamingResponse:
        async def stream():
            cursor = after
            while not await request.is_disconnected():
                factory = session_factory
                if factory is None:
                    settings = V2Settings()
                    factory = create_session_factory(
                        create_database_engine(settings.require_database_url())
                    )
                with factory() as session:
                    events = session.scalars(
                        select(JobEvent)
                        .where(JobEvent.id > cursor)
                        .order_by(JobEvent.id)
                        .limit(100)
                    ).all()
                    for event in events:
                        cursor = event.id
                        data = {
                            "event_id": event.id,
                            "id": event.job_id,
                            "state": event.status,
                            "message": event.message,
                            "timestamp": event.created_at.isoformat(),
                        }
                        yield f"id: {event.id}\nevent: job\ndata: {json.dumps(data)}\n\n"
                    if events:
                        counts = dict(
                            session.execute(
                                select(WorkJob.status, func.count()).group_by(WorkJob.status)
                            ).all()
                        )
                        yield f"event: counts\ndata: {json.dumps(counts)}\n\n"
                    else:
                        yield ": heartbeat\n\n"
                await asyncio.sleep(2)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Preserve common legacy bookmarks.
    async def redirect_library():
        return RedirectResponse("/library", 308)

    async def redirect_operations():
        return RedirectResponse("/operations", 308)

    async def redirect_updates():
        return RedirectResponse("/updates", 308)

    app.add_api_route("/new", redirect_updates, methods=["GET"])
    app.add_api_route("/info", redirect_operations, methods=["GET"])
    return app


app = create_app()


def merge_match_groups(session: Session, decision: CatalogMatchDecision) -> None:
    left = session.get(CatalogSourceSeries, decision.left_source_series_id)
    right = session.get(CatalogSourceSeries, decision.right_source_series_id)
    if left is None or right is None:
        raise HTTPException(409, "one match identity no longer exists")
    if left.series_id == right.series_id:
        return
    left_sources = set(
        session.scalars(
            select(CatalogSourceSeries.source).where(
                CatalogSourceSeries.series_id == left.series_id
            )
        ).all()
    )
    right_sources = set(
        session.scalars(
            select(CatalogSourceSeries.source).where(
                CatalogSourceSeries.series_id == right.series_id
            )
        ).all()
    )
    overlap = left_sources.intersection(right_sources)
    if overlap:
        raise HTTPException(
            409, f"merge would create duplicate provider identities: {', '.join(sorted(overlap))}"
        )
    target_series_id = left.series_id
    source_series_id = right.series_id
    source_chapters = session.scalars(
        select(CatalogChapter)
        .where(CatalogChapter.series_id == source_series_id)
        .order_by(CatalogChapter.id)
    ).all()
    for chapter in source_chapters:
        target = session.scalar(
            select(CatalogChapter).where(
                CatalogChapter.series_id == target_series_id,
                CatalogChapter.canonical_number == chapter.canonical_number,
            )
        )
        if target is None:
            chapter.series_id = target_series_id
            continue
        session.execute(
            update(CatalogChapterRelease)
            .where(CatalogChapterRelease.chapter_id == chapter.id)
            .values(chapter_id=target.id)
        )
        target_active = session.scalar(
            select(ChapterArtifact.id).where(
                ChapterArtifact.chapter_id == target.id,
                ChapterArtifact.state == "active",
            )
        )
        if target_active is not None:
            session.execute(
                update(ChapterArtifact)
                .where(ChapterArtifact.chapter_id == chapter.id)
                .where(ChapterArtifact.state == "active")
                .values(state="quarantined")
            )
            session.execute(
                update(ChapterArtifact)
                .where(ChapterArtifact.chapter_id == chapter.id)
                .values(chapter_id=target.id)
            )
            session.execute(
                delete(LibraryProjection).where(LibraryProjection.chapter_id == chapter.id)
            )
        else:
            session.execute(
                update(ChapterArtifact)
                .where(ChapterArtifact.chapter_id == chapter.id)
                .values(chapter_id=target.id)
            )
            session.execute(
                update(LibraryProjection)
                .where(LibraryProjection.chapter_id == chapter.id)
                .values(chapter_id=target.id)
            )
        session.delete(chapter)
    session.execute(
        update(CatalogSourceSeries)
        .where(CatalogSourceSeries.series_id == source_series_id)
        .values(series_id=target_series_id)
    )
    aliases = session.scalars(
        select(CatalogSeriesAlias).where(CatalogSeriesAlias.series_id == source_series_id)
    ).all()
    for alias in aliases:
        duplicate = session.scalar(
            select(CatalogSeriesAlias.id).where(
                CatalogSeriesAlias.series_id == target_series_id,
                CatalogSeriesAlias.normalized_value == alias.normalized_value,
            )
        )
        if duplicate is None:
            alias.series_id = target_series_id
        else:
            session.delete(alias)
    session.execute(
        update(CatalogExternalIdentifier)
        .where(CatalogExternalIdentifier.series_id == source_series_id)
        .values(series_id=target_series_id)
    )
    target_series = session.get(CatalogSeries, target_series_id)
    source_series = session.get(CatalogSeries, source_series_id)
    if target_series is not None:
        target_series.integrity_state = "healthy"
        if source_series and (
            target_series.latest_release_at is None
            or (
                source_series.latest_release_at is not None
                and source_series.latest_release_at > target_series.latest_release_at
            )
        ):
            target_series.latest_release_at = source_series.latest_release_at
            target_series.latest_release_number = source_series.latest_release_number
            target_series.latest_release_source = source_series.latest_release_source
    if source_series is not None:
        session.delete(source_series)


def encode_cursor(value: datetime | None, row_id: int) -> str:
    if value is None:
        return f"0-{row_id}"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return f"{int(value.timestamp() * 1_000_000)}-{row_id}"


def decode_cursor(value: str) -> tuple[datetime | None, int]:
    try:
        micros_text, row_id_text = value.split("-", 1)
        micros = int(micros_text)
        row_id = int(row_id_text)
        if micros < 0 or row_id < 1:
            raise ValueError
    except ValueError as exc:
        raise HTTPException(422, "invalid cursor") from exc
    if micros == 0:
        return None, row_id
    return datetime.fromtimestamp(micros / 1_000_000, tz=timezone.utc), row_id
