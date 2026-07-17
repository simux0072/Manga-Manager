from __future__ import annotations

from pathlib import Path

import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session, sessionmaker

from manga_manager.infrastructure.database import create_database_engine, create_session_factory
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogChapterRelease,
    ChapterReleaseAttempt,
    CatalogAlternateSourceListing,
    CatalogExternalIdentifier,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    ChapterDownloadIntent,
    ChapterArtifact,
    KavitaProjection,
    LibraryProjection,
    SeriesDownloadPlan,
    WorkJob,
)
from manga_manager.settings import V2Settings
from manga_manager.application.library_repair import enqueue_library_repair
from manga_manager.domain.jobs import JobKind
from manga_manager.domain.providers import SOURCE_PRIORITY, provider_names
from manga_manager.domain.matching import provider_identities_equivalent
from manga_manager.web.api import create_api_router
from manga_manager.web.metrics import (
    RequestMetrics,
    begin_request_measurement,
    end_request_measurement,
    install_sql_timing,
    render_database_metrics,
)


ROOT = Path(__file__).parents[2]
PAGE_PATHS = ("/", "/discovery", "/library", "/updates", "/matches", "/activity", "/operations")


def create_app(session_factory: sessionmaker[Session] | None = None) -> FastAPI:
    application = FastAPI(title="Manga Manager", version="2")
    resolved_factory = session_factory
    request_metrics = RequestMetrics()

    def instrument_factory(factory: sessionmaker[Session]) -> sessionmaker[Session]:
        bind = factory.kw.get("bind")
        if bind is not None:
            install_sql_timing(bind)
        return factory

    if resolved_factory is not None:
        instrument_factory(resolved_factory)

    def get_session_factory() -> sessionmaker[Session]:
        nonlocal resolved_factory
        if resolved_factory is None:
            settings = V2Settings()
            resolved_factory = instrument_factory(
                create_session_factory(
                    create_database_engine(settings.require_database_url(), role="web")
                )
            )
        return resolved_factory

    @application.middleware("http")
    async def measure_request(request: Request, call_next):
        started = time.perf_counter()
        measurement, token = begin_request_measurement()
        status = 500
        response_bytes = 0
        try:
            response = await call_next(request)
            status = response.status_code
            try:
                response_bytes = int(response.headers.get("content-length", "0"))
            except ValueError:
                response_bytes = 0
            response.headers["Server-Timing"] = (
                f"app;dur={(time.perf_counter() - started) * 1000:.2f}, "
                f'db;dur={measurement.sql_seconds * 1000:.2f};desc="'
                f'{measurement.sql_queries} queries"'
            )
            response.headers["X-SQL-Query-Count"] = str(measurement.sql_queries)
            if request.url.path.startswith("/assets/"):
                response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
            return response
        finally:
            route = request.scope.get("route")
            route_label = getattr(route, "path", "unmatched")
            end_request_measurement(token)
            request_metrics.observe(
                method=request.method,
                route=route_label,
                status=status,
                duration_seconds=time.perf_counter() - started,
                sql_queries=measurement.sql_queries,
                sql_seconds=measurement.sql_seconds,
                response_bytes=response_bytes,
            )

    application.include_router(create_api_router(get_session_factory))

    @application.get("/metrics", include_in_schema=False)
    def metrics() -> PlainTextResponse:
        with get_session_factory()() as session:
            database_metrics = render_database_metrics(session)
        return PlainTextResponse(
            request_metrics.render_prometheus() + database_metrics,
            media_type="text/plain; version=0.0.4",
        )

    frontend = ROOT / "frontend" / "dist"
    if (frontend / "assets").exists():
        application.mount(
            "/assets", StaticFiles(directory=frontend / "assets"), name="frontend-assets"
        )

    if (frontend / "index.html").exists():

        def react_page() -> FileResponse:
            return FileResponse(frontend / "index.html")

        for path in PAGE_PATHS:
            application.add_api_route(path, react_page, methods=["GET"], include_in_schema=False)

    @application.get("/livez")
    def live() -> dict[str, object]:
        return {"ok": True}

    @application.get("/readyz")
    def ready() -> dict[str, object]:
        with get_session_factory()() as session:
            session.execute(select(1))
        return {"ok": True, "architecture": "postgresql-v2"}

    @application.get("/healthz")
    def health() -> dict[str, object]:
        return ready()

    def redirect_library() -> RedirectResponse:
        return RedirectResponse("/library", 308)

    def redirect_operations() -> RedirectResponse:
        return RedirectResponse("/operations", 308)

    def redirect_updates() -> RedirectResponse:
        return RedirectResponse("/updates", 308)

    def redirect_events() -> RedirectResponse:
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
    _consolidate_overlapping_provider_identities(session, left, right, decision.id)
    session.flush()
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
        source_intent = session.scalar(
            select(ChapterDownloadIntent).where(ChapterDownloadIntent.chapter_id == chapter.id)
        )
        target_intent = session.scalar(
            select(ChapterDownloadIntent).where(ChapterDownloadIntent.chapter_id == target.id)
        )
        if source_intent is not None:
            if target_intent is None:
                source_intent.series_id = target_series_id
                source_intent.chapter_id = target.id
            else:
                session.delete(source_intent)
        session.execute(
            update(CatalogChapterRelease)
            .where(CatalogChapterRelease.chapter_id == chapter.id)
            .values(chapter_id=target.id)
        )
        session.execute(
            update(ChapterReleaseAttempt)
            .where(ChapterReleaseAttempt.chapter_id == chapter.id)
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
            session.execute(
                delete(KavitaProjection).where(KavitaProjection.chapter_id == chapter.id)
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
            session.execute(
                update(KavitaProjection)
                .where(KavitaProjection.chapter_id == chapter.id)
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
    session.execute(
        update(ChapterDownloadIntent)
        .where(ChapterDownloadIntent.series_id == source_series_id)
        .values(series_id=target_series_id)
    )
    source_plan = session.get(SeriesDownloadPlan, source_series_id)
    target_plan = session.get(SeriesDownloadPlan, target_series_id)
    if source_plan is not None:
        if target_plan is None:
            source_plan.series_id = target_series_id
        else:
            session.delete(source_plan)
    session.execute(
        update(WorkJob)
        .where(WorkJob.series_key == str(source_series_id))
        .values(series_key=str(target_series_id))
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
        if source_series is not None:
            if not target_series.description and source_series.description:
                target_series.description = source_series.description
            if not target_series.cover_url and source_series.cover_url:
                target_series.cover_url = source_series.cover_url
            if (
                target_series.kavita_series_id is None
                and source_series.kavita_series_id is not None
            ):
                target_series.kavita_series_id = source_series.kavita_series_id
                target_series.kavita_library_id = source_series.kavita_library_id
                target_series.kavita_synced_at = source_series.kavita_synced_at
                target_series.kavita_cover_checksum = source_series.kavita_cover_checksum
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


def merge_canonical_series(session: Session, series_ids: list[int]) -> int:
    unique_ids = sorted(set(series_ids))
    provider_count = len(provider_names())
    if not 2 <= len(unique_ids) <= provider_count:
        raise HTTPException(422, f"select two to {provider_count} manga")
    rows = session.scalars(
        select(CatalogSeries)
        .where(CatalogSeries.id.in_(unique_ids))
        .order_by(CatalogSeries.id)
        .with_for_update()
    ).all()
    if len(rows) != len(unique_ids):
        raise HTTPException(404, "one or more manga no longer exist")
    leased = session.scalar(
        select(WorkJob.id)
        .where(
            WorkJob.series_key.in_([str(value) for value in unique_ids]),
            WorkJob.status == "leased",
        )
        .limit(1)
    )
    if leased is not None:
        raise HTTPException(409, "wait for active series jobs to finish before merging")
    sources_by_series = {
        row.id: session.scalars(
            select(CatalogSourceSeries).where(CatalogSourceSeries.series_id == row.id)
        ).all()
        for row in rows
    }
    # Validate every provider collision before moving any rows. Corrupted historical groups may
    # contain the same provider twice, but only strong chapter overlap permits consolidation.
    for index, row in enumerate(rows):
        left_by_source = {identity.source: identity for identity in sources_by_series[row.id]}
        for other in rows[index + 1 :]:
            right_by_source = {
                identity.source: identity for identity in sources_by_series[other.id]
            }
            for source in left_by_source.keys() & right_by_source.keys():
                overlap, minimum = _chapter_overlap(
                    session, left_by_source[source].id, right_by_source[source].id
                )
                equivalent = provider_identities_equivalent(
                    left_by_source[source], right_by_source[source]
                )
                if not equivalent and (overlap < 2 or (minimum and overlap / minimum < 0.5)):
                    raise HTTPException(
                        409,
                        f"duplicate {source} identities lack strong chapter overlap",
                    )
    priorities = {
        source: len(SOURCE_PRIORITY) - index for index, source in enumerate(SOURCE_PRIORITY)
    }

    def rank(row: CatalogSeries) -> tuple[int, int, int, int]:
        identities = sources_by_series[row.id]
        provider = max((priorities.get(identity.source, 0) for identity in identities), default=0)
        metadata = int(bool(row.cover_url)) + int(bool(row.description))
        chapters = (
            session.scalar(
                select(func.count())
                .select_from(CatalogChapter)
                .where(CatalogChapter.series_id == row.id)
            )
            or 0
        )
        return provider, metadata, chapters, -row.id

    target = max(rows, key=rank)
    obsolete_storage_keys = tuple(row.storage_key for row in rows if row.id != target.id)
    status_rank = {"untracked": 0, "paused": 1, "caught_up": 2, "interested": 3, "reading": 4}
    target.status = max(rows, key=lambda row: status_rank.get(row.status, 0)).status
    for source_row in rows:
        if source_row.id == target.id:
            continue
        session.flush()
        target_identities = session.scalars(
            select(CatalogSourceSeries)
            .where(CatalogSourceSeries.series_id == target.id)
            .order_by(CatalogSourceSeries.id)
        ).all()
        source_identities = session.scalars(
            select(CatalogSourceSeries)
            .where(CatalogSourceSeries.series_id == source_row.id)
            .order_by(CatalogSourceSeries.id)
        ).all()
        shared = {identity.source for identity in target_identities} & {
            identity.source for identity in source_identities
        }
        left_identity = next(
            (identity for identity in target_identities if identity.source in shared),
            target_identities[0] if target_identities else None,
        )
        right_identity = (
            next(
                (
                    identity
                    for identity in source_identities
                    if identity.source == left_identity.source
                ),
                source_identities[0] if source_identities else None,
            )
            if left_identity
            else None
        )
        if left_identity is None or right_identity is None:
            raise HTTPException(409, "each manga must have a provider identity")
        merge_match_groups(
            session,
            CatalogMatchDecision(
                id=0,
                left_source_series_id=left_identity.id,
                right_source_series_id=right_identity.id,
            ),
        )
    session.flush()
    enqueue_library_repair(
        session,
        series_id=target.id,
        reason="merge",
        obsolete_storage_keys=obsolete_storage_keys,
        priority=90,
    )
    return target.id


def _consolidate_overlapping_provider_identities(
    session: Session,
    left: CatalogSourceSeries,
    right: CatalogSourceSeries,
    current_decision_id: int,
) -> None:
    left_identities = {
        row.source: row
        for row in session.scalars(
            select(CatalogSourceSeries).where(CatalogSourceSeries.series_id == left.series_id)
        )
    }
    right_identities = {
        row.source: row
        for row in session.scalars(
            select(CatalogSourceSeries).where(CatalogSourceSeries.series_id == right.series_id)
        )
    }
    for source in left_identities.keys() & right_identities.keys():
        left_identity, right_identity = left_identities[source], right_identities[source]
        if right.source == source:
            keeper, duplicate = right_identity, left_identity
        elif left.source == source:
            keeper, duplicate = left_identity, right_identity
        else:
            keeper, duplicate = left_identity, right_identity
        overlap, minimum = _chapter_overlap(session, keeper.id, duplicate.id)
        equivalent = provider_identities_equivalent(keeper, duplicate)
        if not equivalent and (overlap < 2 or (minimum and overlap / minimum < 0.5)):
            raise HTTPException(409, f"duplicate {source} identities lack strong chapter overlap")
        session.add(
            CatalogAlternateSourceListing(
                primary_source_series_id=keeper.id,
                source=duplicate.source,
                source_id=duplicate.source_id,
                title=duplicate.title,
                url=duplicate.url,
                evidence_json={"chapter_overlap": overlap, "compared_chapters": minimum},
            )
        )
        for release in session.scalars(
            select(CatalogChapterRelease).where(
                CatalogChapterRelease.source_series_id == duplicate.id
            )
        ):
            existing_release = session.scalar(
                select(CatalogChapterRelease).where(
                    CatalogChapterRelease.source_series_id == keeper.id,
                    CatalogChapterRelease.source_release_id == release.source_release_id,
                )
            )
            if existing_release is None:
                release.source_series_id = keeper.id
            else:
                _replace_release_references(session, release, existing_release)
        for alias in session.scalars(
            select(CatalogSeriesAlias).where(CatalogSeriesAlias.source_series_id == duplicate.id)
        ):
            conflict = session.scalar(
                select(CatalogSeriesAlias.id).where(
                    CatalogSeriesAlias.id != alias.id,
                    CatalogSeriesAlias.series_id == alias.series_id,
                    CatalogSeriesAlias.normalized_value == alias.normalized_value,
                )
            )
            if conflict is None:
                alias.source_series_id = keeper.id
            else:
                # A direct statement keeps ORM state aligned before deleting the duplicate source
                # identity, whose database cascade would otherwise trigger a second DELETE warning.
                session.execute(delete(CatalogSeriesAlias).where(CatalogSeriesAlias.id == alias.id))
        for identifier in session.scalars(
            select(CatalogExternalIdentifier).where(
                CatalogExternalIdentifier.source_series_id == duplicate.id
            )
        ):
            conflict = session.scalar(
                select(CatalogExternalIdentifier.id).where(
                    CatalogExternalIdentifier.source_series_id == keeper.id,
                    CatalogExternalIdentifier.provider == identifier.provider,
                )
            )
            if conflict is None:
                identifier.source_series_id = keeper.id
            else:
                session.delete(identifier)
        session.execute(
            delete(CatalogMatchDecision).where(
                CatalogMatchDecision.id != current_decision_id,
                (CatalogMatchDecision.left_source_series_id == duplicate.id)
                | (CatalogMatchDecision.right_source_series_id == duplicate.id),
            )
        )
        session.delete(duplicate)


def _replace_release_references(
    session: Session,
    duplicate: CatalogChapterRelease,
    keeper: CatalogChapterRelease,
) -> None:
    """Retire a duplicate release without losing provenance or queued work."""
    session.execute(
        update(ChapterArtifact)
        .where(ChapterArtifact.chapter_release_id == duplicate.id)
        .values(chapter_release_id=keeper.id)
    )
    session.execute(
        update(ChapterReleaseAttempt)
        .where(ChapterReleaseAttempt.chapter_release_id == duplicate.id)
        .values(chapter_release_id=keeper.id)
    )
    for job in session.scalars(
        select(WorkJob).where(WorkJob.kind == JobKind.CHAPTER_DOWNLOAD.value)
    ):
        payload = dict(job.payload or {})
        if int(payload.get("chapter_release_id") or 0) == duplicate.id:
            payload["chapter_release_id"] = keeper.id
            job.payload = payload
        pending = dict(job.pending_payload or {})
        if int(pending.get("chapter_release_id") or 0) == duplicate.id:
            pending["chapter_release_id"] = keeper.id
            job.pending_payload = pending
    session.delete(duplicate)


def _chapter_overlap(session: Session, left_id: int, right_id: int) -> tuple[int, int]:
    def numbers(source_series_id: int) -> set[str]:
        return set(
            session.scalars(
                select(CatalogChapter.canonical_number)
                .join(CatalogChapterRelease)
                .where(CatalogChapterRelease.source_series_id == source_series_id)
            ).all()
        )

    left, right = numbers(left_id), numbers(right_id)
    return len(left & right), min(len(left), len(right))
