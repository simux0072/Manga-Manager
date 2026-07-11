from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session, sessionmaker

from manga_manager.infrastructure.database import create_database_engine, create_session_factory
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogChapterRelease,
    CatalogExternalIdentifier,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    ChapterArtifact,
    LibraryProjection,
)
from manga_manager.settings import V2Settings
from manga_manager.web.api import create_api_router


ROOT = Path(__file__).parents[2]
PAGE_PATHS = ("/", "/discovery", "/library", "/updates", "/matches", "/activity", "/operations")


def create_app(session_factory: sessionmaker[Session] | None = None) -> FastAPI:
    application = FastAPI(title="Manga Manager", version="2")
    resolved_factory = session_factory

    def get_session_factory() -> sessionmaker[Session]:
        nonlocal resolved_factory
        if resolved_factory is None:
            settings = V2Settings()
            resolved_factory = create_session_factory(
                create_database_engine(settings.require_database_url())
            )
        return resolved_factory

    application.include_router(create_api_router(get_session_factory))
    frontend = ROOT / "frontend" / "dist"
    if (frontend / "assets").exists():
        application.mount(
            "/assets", StaticFiles(directory=frontend / "assets"), name="frontend-assets"
        )

    if (frontend / "index.html").exists():

        async def react_page() -> FileResponse:
            return FileResponse(frontend / "index.html")

        for path in PAGE_PATHS:
            application.add_api_route(path, react_page, methods=["GET"], include_in_schema=False)

    @application.get("/healthz")
    async def health() -> dict[str, object]:
        with get_session_factory()() as session:
            session.execute(select(1))
        return {"ok": True, "architecture": "postgresql-v2"}

    async def redirect_library() -> RedirectResponse:
        return RedirectResponse("/library", 308)

    async def redirect_operations() -> RedirectResponse:
        return RedirectResponse("/operations", 308)

    async def redirect_updates() -> RedirectResponse:
        return RedirectResponse("/updates", 308)

    async def redirect_events() -> RedirectResponse:
        return RedirectResponse("/api/v2/events", 307)

    application.add_api_route("/new", redirect_updates, methods=["GET"])
    application.add_api_route("/new-chapters", redirect_updates, methods=["GET"])
    application.add_api_route("/info", redirect_operations, methods=["GET"])
    application.add_api_route("/catalog", redirect_library, methods=["GET"])
    application.add_api_route("/events/jobs", redirect_events, methods=["GET"])
    return application


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
        source_reading = session.get(CatalogChapterReadingState, chapter.id)
        target_reading = session.get(CatalogChapterReadingState, target.id)
        if source_reading is not None:
            if target_reading is None:
                source_reading.chapter_id = target.id
            else:
                rank = {"unread": 0, "reading": 1, "read": 2}
                if rank[source_reading.status] > rank[target_reading.status]:
                    target_reading.status = source_reading.status
                    target_reading.read_at = source_reading.read_at
                session.delete(source_reading)
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
