from __future__ import annotations

import asyncio
import json
from pathlib import Path

from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from manga_manager.infrastructure.database import create_database_engine, create_session_factory
from manga_manager.infrastructure.db_models import CatalogSeries, JobEvent, WorkJob
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
        cursor: int | None = Query(default=None, ge=1),
        q: str = Query(default="", max_length=200),
        source: str = Query(default="", max_length=50),
        session: Session = Depends(sessions),
    ) -> HTMLResponse:
        query = select(CatalogSeries)
        if cursor:
            query = query.where(CatalogSeries.id < cursor)
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
        rows = session.scalars(query.order_by(CatalogSeries.id.desc()).limit(25)).all()
        context = {
            "request": request,
            "series": rows,
            "next_cursor": rows[-1].id if len(rows) == 25 else None,
            "q": q,
            "source": source,
            "section": "discovery",
        }
        template = (
            "_series_grid.html" if request.headers.get("HX-Request") == "true" else "discovery.html"
        )
        return templates.TemplateResponse(request, template, context)

    @app.get("/library", response_class=HTMLResponse)
    async def library(request: Request, session: Session = Depends(sessions)) -> HTMLResponse:
        rows = session.scalars(
            select(CatalogSeries)
            .where(CatalogSeries.status != "untracked")
            .order_by(CatalogSeries.updated_at.desc(), CatalogSeries.id.desc())
            .limit(100)
        ).all()
        return templates.TemplateResponse(
            request, "library.html", {"series": rows, "section": "library"}
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
        return templates.TemplateResponse(
            request, "operations.html", {"counts": counts, "jobs": jobs, "section": "operations"}
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

    app.add_api_route("/new", redirect_library, methods=["GET"])
    app.add_api_route("/info", redirect_operations, methods=["GET"])
    return app


app = create_app()
