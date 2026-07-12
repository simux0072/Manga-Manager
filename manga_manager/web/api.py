import asyncio
import json
import shutil
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from collections.abc import Callable
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session, aliased, sessionmaker

from app.kavita import configured_kavita_client

from manga_manager.domain.jobs import (
    JobKind,
    KavitaSyncPayload,
    MaintenancePayload,
    SourcePullPayload,
)
from manga_manager.application.download_plans import DownloadPlanCoordinator

from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogChapterRelease,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    CatalogSourceState,
    ChapterArtifact,
    JobEvent,
    JobPermit,
    LibraryProjection,
    ProviderBenchmarkRun,
    ProviderPolicy,
    WorkerHeartbeat,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.settings import V2Settings


TRACKED_STATES = ("interested", "reading", "caught_up", "paused")
ACTIVE_JOB_STATES = ("queued", "leased", "retry_wait")


class SeriesStateChange(BaseModel):
    status: str


class ChapterStateChange(BaseModel):
    status: str


class MatchChange(BaseModel):
    decision: str
    confirmation: str = ""


class BatchMatchChange(MatchChange):
    ids: list[int]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def create_api_router(
    factory_provider: Callable[[], sessionmaker[Session]],
) -> APIRouter:
    router = APIRouter(prefix="/api/v2")

    async def sessions():
        with factory_provider()() as session:
            yield session

    SessionDep = Annotated[Session, Depends(sessions)]

    @router.get("/discovery")
    async def discovery(
        session: SessionDep,
        q: str = Query(default="", max_length=200),
        source: list[str] = Query(default=[]),
        cursor: str = Query(default="", max_length=80),
        limit: int = Query(default=25, ge=1, le=50),
    ) -> dict[str, Any]:
        query = select(CatalogSeries).where(CatalogSeries.status == "untracked")
        if q.strip():
            term = f"%{q.strip()}%"
            alias_match = select(CatalogSeriesAlias.id).where(
                CatalogSeriesAlias.series_id == CatalogSeries.id,
                CatalogSeriesAlias.display_value.ilike(term),
            )
            query = query.where(
                or_(
                    CatalogSeries.title.ilike(term),
                    CatalogSeries.normalized_title.ilike(term),
                    CatalogSeries.description.ilike(term),
                    alias_match.exists(),
                )
            )
        valid_sources = sorted(set(source).intersection({"asura", "mangafire", "kingofshojo"}))
        if valid_sources:
            query = query.where(
                select(CatalogSourceSeries.id)
                .where(
                    CatalogSourceSeries.series_id == CatalogSeries.id,
                    CatalogSourceSeries.source.in_(valid_sources),
                )
                .exists()
            )
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
                        and_(
                            CatalogSeries.latest_release_at == cursor_time,
                            CatalogSeries.id < cursor_id,
                        ),
                    )
                )
        rows = session.scalars(
            query.order_by(
                CatalogSeries.latest_release_at.desc().nullslast(), CatalogSeries.id.desc()
            ).limit(limit)
        ).all()
        return {
            "items": serialize_series(session, rows),
            "next_cursor": encode_cursor(rows[-1].latest_release_at, rows[-1].id)
            if len(rows) == limit
            else None,
        }

    @router.get("/library")
    async def library(
        session: SessionDep,
        q: str = Query(default="", max_length=200),
        source: list[str] = Query(default=[]),
        state: list[str] = Query(default=[]),
        cursor: str = Query(default="", max_length=80),
        limit: int = Query(default=30, ge=1, le=60),
    ) -> dict[str, Any]:
        query = select(CatalogSeries).where(CatalogSeries.status.in_(TRACKED_STATES))
        valid_states = sorted(set(state).intersection(TRACKED_STATES))
        if valid_states:
            query = query.where(CatalogSeries.status.in_(valid_states))
        if q.strip():
            term = f"%{q.strip()}%"
            query = query.where(
                or_(CatalogSeries.title.ilike(term), CatalogSeries.description.ilike(term))
            )
        valid_sources = sorted(set(source).intersection({"asura", "mangafire", "kingofshojo"}))
        if valid_sources:
            query = query.where(
                select(CatalogSourceSeries.id)
                .where(
                    CatalogSourceSeries.series_id == CatalogSeries.id,
                    CatalogSourceSeries.source.in_(valid_sources),
                )
                .exists()
            )
        if cursor:
            cursor_time, cursor_id = decode_cursor(cursor)
            if cursor_time is None:
                raise HTTPException(422, "invalid library cursor")
            query = query.where(
                or_(
                    CatalogSeries.updated_at < cursor_time,
                    and_(CatalogSeries.updated_at == cursor_time, CatalogSeries.id < cursor_id),
                )
            )
        rows = session.scalars(
            query.order_by(CatalogSeries.updated_at.desc(), CatalogSeries.id.desc()).limit(limit)
        ).all()
        return {
            "items": serialize_series(session, rows, include_progress=True),
            "next_cursor": encode_cursor(rows[-1].updated_at, rows[-1].id)
            if len(rows) == limit
            else None,
        }

    @router.patch("/series/{series_id}")
    async def change_series(series_id: int, change: SeriesStateChange, session: SessionDep):
        allowed = {"untracked", *TRACKED_STATES}
        if change.status not in allowed:
            raise HTTPException(422, "invalid series state")
        with session.begin():
            row = session.get(CatalogSeries, series_id)
            if row is None:
                raise HTTPException(404, "series not found")
            previous = row.status
            row.status = change.status
            row.updated_at = utcnow()
            coordinator = DownloadPlanCoordinator()
            if change.status in TRACKED_STATES:
                coordinator.track(session, row.id)
            else:
                coordinator.untrack(session, row.id)
            if row.kavita_series_id is not None:
                JobQueue().enqueue(
                    session,
                    kind=JobKind.KAVITA_SYNC,
                    dedupe_key=f"series:{row.id}",
                    payload=KavitaSyncPayload(series_id=row.id),
                    priority=10,
                    series_key=str(row.id),
                )
        return {
            "item": serialize_series(session, [row], include_progress=True)[0],
            "previous": previous,
        }

    @router.patch("/chapters/{chapter_id}")
    async def change_chapter(chapter_id: int, change: ChapterStateChange, session: SessionDep):
        if change.status not in {"unread", "reading", "read"}:
            raise HTTPException(422, "invalid chapter state")
        with session.begin():
            chapter = session.get(CatalogChapter, chapter_id)
            if chapter is None:
                raise HTTPException(404, "chapter not found")
            row = session.get(CatalogChapterReadingState, chapter_id)
            if row is None:
                row = CatalogChapterReadingState(chapter_id=chapter_id)
                session.add(row)
            row.status = change.status
            row.read_at = utcnow() if change.status == "read" else None
            row.updated_at = utcnow()
        return {"chapter_id": chapter_id, "status": change.status}

    @router.post("/series/{series_id}/chapters/read")
    async def read_all(series_id: int, session: SessionDep):
        now = utcnow()
        with session.begin():
            chapter_ids = session.scalars(
                select(CatalogChapter.id).where(CatalogChapter.series_id == series_id)
            ).all()
            for chapter_id in chapter_ids:
                row = session.get(CatalogChapterReadingState, chapter_id)
                if row is None:
                    row = CatalogChapterReadingState(chapter_id=chapter_id)
                    session.add(row)
                row.status = "read"
                row.read_at = now
                row.updated_at = now
        return {"series_id": series_id, "updated": len(chapter_ids)}

    @router.get("/updates")
    async def updates(
        session: SessionDep,
        cursor: str = Query(default="", max_length=80),
        limit: int = Query(default=20, ge=1, le=40),
    ) -> dict[str, Any]:
        release_time = func.coalesce(
            CatalogChapterRelease.published_at, CatalogChapterRelease.first_seen_at
        )
        unread = or_(
            CatalogChapterReadingState.chapter_id.is_(None),
            CatalogChapterReadingState.status != "read",
        )
        grouped = (
            select(
                CatalogSeries.id.label("series_id"),
                func.max(release_time).label("latest_at"),
            )
            .join(CatalogChapter, CatalogChapter.series_id == CatalogSeries.id)
            .join(CatalogChapterRelease, CatalogChapterRelease.chapter_id == CatalogChapter.id)
            .outerjoin(
                CatalogChapterReadingState,
                CatalogChapterReadingState.chapter_id == CatalogChapter.id,
            )
            .where(CatalogSeries.status.in_(TRACKED_STATES), unread)
            .group_by(CatalogSeries.id)
        )
        if cursor:
            cursor_time, cursor_id = decode_cursor(cursor)
            if cursor_time is None:
                raise HTTPException(422, "invalid updates cursor")
            grouped = grouped.having(
                or_(
                    func.max(release_time) < cursor_time,
                    and_(
                        func.max(release_time) == cursor_time,
                        CatalogSeries.id < cursor_id,
                    ),
                )
            )
        groups = session.execute(
            grouped.order_by(func.max(release_time).desc(), CatalogSeries.id.desc()).limit(limit)
        ).all()
        series_ids = [row.series_id for row in groups]
        series_rows = {
            row.id: row
            for row in session.scalars(
                select(CatalogSeries).where(CatalogSeries.id.in_(series_ids))
            ).all()
        }
        releases = session.execute(
            select(CatalogChapterRelease, CatalogChapter, CatalogChapterReadingState)
            .join(CatalogChapter, CatalogChapter.id == CatalogChapterRelease.chapter_id)
            .outerjoin(
                CatalogChapterReadingState,
                CatalogChapterReadingState.chapter_id == CatalogChapter.id,
            )
            .where(CatalogChapter.series_id.in_(series_ids), unread)
            .order_by(release_time.desc(), CatalogChapterRelease.id.desc())
        ).all()
        by_series: dict[int, list[dict[str, Any]]] = defaultdict(list)
        seen_chapters: set[int] = set()
        for release, chapter, reading in releases:
            if chapter.id in seen_chapters:
                continue
            seen_chapters.add(chapter.id)
            by_series[chapter.series_id].append(
                {
                    "id": chapter.id,
                    "number": chapter.display_number,
                    "title": chapter.title,
                    "source": release.source,
                    "url": release.url,
                    "released_at": (release.published_at or release.first_seen_at).isoformat(),
                    "reading_status": reading.status if reading else "unread",
                }
            )
        summaries = {
            item["id"]: item for item in serialize_series(session, list(series_rows.values()))
        }
        items = [
            {**summaries[row.series_id], "unread_chapters": by_series[row.series_id]}
            for row in groups
            if row.series_id in summaries
        ]
        return {
            "items": items,
            "next_cursor": encode_cursor(groups[-1].latest_at, groups[-1].series_id)
            if len(groups) == limit
            else None,
        }

    @router.get("/matches")
    async def matches(
        session: SessionDep,
        cursor: int = Query(default=0, ge=0),
        limit: int = Query(default=24, ge=1, le=100),
    ):
        left = aliased(CatalogSourceSeries)
        right = aliased(CatalogSourceSeries)
        query = (
            select(CatalogMatchDecision, left, right)
            .join(left, left.id == CatalogMatchDecision.left_source_series_id)
            .join(right, right.id == CatalogMatchDecision.right_source_series_id)
            .where(CatalogMatchDecision.decision == "pending")
        )
        if cursor:
            query = query.where(CatalogMatchDecision.id > cursor)
        rows = session.execute(query.order_by(CatalogMatchDecision.id).limit(limit)).all()
        canonical_ids = {
            source.series_id for _, left_row, right_row in rows for source in (left_row, right_row)
        }
        canonical = {
            item["id"]: item
            for item in serialize_series(
                session,
                session.scalars(
                    select(CatalogSeries).where(CatalogSeries.id.in_(canonical_ids))
                ).all(),
            )
        }
        return {
            "items": [
                {
                    "id": decision.id,
                    "confidence": decision.confidence,
                    "evidence": human_evidence(decision.evidence_json),
                    "left": {
                        **canonical[left_row.series_id],
                        "source_title": left_row.title,
                        "source": left_row.source,
                        "url": left_row.url,
                    },
                    "right": {
                        **canonical[right_row.series_id],
                        "source_title": right_row.title,
                        "source": right_row.source,
                        "url": right_row.url,
                    },
                }
                for decision, left_row, right_row in rows
            ],
            "next_cursor": rows[-1][0].id if len(rows) == limit else None,
        }

    @router.post("/matches/{decision_id}")
    async def decide_match(decision_id: int, change: MatchChange, session: SessionDep):
        if change.decision not in {"accepted", "rejected"}:
            raise HTTPException(422, "invalid match decision")
        if change.decision == "accepted" and change.confirmation != "MERGE":
            raise HTTPException(422, "MERGE confirmation required")
        from manga_manager.web.app import merge_match_groups

        with session.begin():
            decision = session.get(CatalogMatchDecision, decision_id)
            if decision is None:
                raise HTTPException(404, "match not found")
            if change.decision == "accepted":
                merge_match_groups(session, decision)
            decision.decision = change.decision
            decision.decided_by = "operator"
            decision.decided_at = utcnow()
        return {"id": decision_id, "decision": change.decision}

    @router.post("/match-batch")
    async def decide_match_batch(change: BatchMatchChange, session: SessionDep):
        if not change.ids or change.decision not in {"accepted", "rejected"}:
            raise HTTPException(422, "select matches and a valid decision")
        if change.decision == "accepted" and change.confirmation != "MERGE":
            raise HTTPException(422, "MERGE confirmation required")
        from manga_manager.web.app import merge_match_groups

        with session.begin():
            rows = session.scalars(
                select(CatalogMatchDecision)
                .where(
                    CatalogMatchDecision.id.in_(set(change.ids)),
                    CatalogMatchDecision.decision == "pending",
                )
                .order_by(CatalogMatchDecision.id)
            ).all()
            if len(rows) != len(set(change.ids)):
                raise HTTPException(409, "one or more matches are no longer pending")
            if change.decision == "accepted":
                for row in rows:
                    merge_match_groups(session, row)
            for row in rows:
                row.decision = change.decision
                row.decided_by = "operator"
                row.decided_at = utcnow()
        return {"ids": change.ids, "decision": change.decision}

    @router.get("/jobs")
    async def jobs(
        session: SessionDep,
        state: list[str] = Query(default=[]),
        cursor: int = Query(default=0, ge=0),
        limit: int = Query(default=30, ge=1, le=100),
    ):
        query = select(WorkJob)
        if state:
            query = query.where(WorkJob.status.in_(state))
        if cursor:
            query = query.where(WorkJob.id < cursor)
        rows = session.scalars(query.order_by(WorkJob.id.desc()).limit(limit)).all()
        return {
            "items": serialize_jobs(session, rows),
            "next_cursor": rows[-1].id if len(rows) == limit else None,
        }

    @router.get("/activity")
    async def activity(
        session: SessionDep,
        event_type: list[str] = Query(default=[]),
        source: list[str] = Query(default=[]),
        cursor: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=200),
    ):
        query = select(JobEvent, WorkJob).join(WorkJob, WorkJob.id == JobEvent.job_id)
        if event_type:
            query = query.where(JobEvent.event_type.in_(event_type))
        if source:
            query = query.where(WorkJob.source.in_(source))
        if cursor:
            query = query.where(JobEvent.id < cursor)
        rows = session.execute(query.order_by(JobEvent.id.desc()).limit(limit)).all()
        jobs_by_id = {
            item["id"]: item for item in serialize_jobs(session, [job for _, job in rows])
        }
        return {
            "items": [
                {
                    "id": event.id,
                    "job": jobs_by_id[job.id],
                    "type": event.event_type,
                    "status": event.status,
                    "message": event.message,
                    "details": event.details or {},
                    "created_at": event.created_at.isoformat(),
                }
                for event, job in rows
            ],
            "next_cursor": rows[-1][0].id if len(rows) == limit else None,
        }

    @router.get("/operations")
    async def operations(session: SessionDep):
        counts = dict(
            session.execute(select(WorkJob.status, func.count()).group_by(WorkJob.status)).all()
        )
        sources = session.scalars(
            select(CatalogSourceState).order_by(CatalogSourceState.source)
        ).all()
        # Process-specific worker IDs accumulate across restarts. Show current capacity here;
        # the event timeline retains the historical record.
        worker_cutoff = utcnow() - timedelta(minutes=5)
        workers = session.scalars(
            select(WorkerHeartbeat)
            .where(WorkerHeartbeat.heartbeat_at >= worker_cutoff)
            .order_by(WorkerHeartbeat.heartbeat_at.desc())
        ).all()
        permit_counts = dict(
            session.execute(select(JobPermit.pool, func.count()).group_by(JobPermit.pool)).all()
        )
        settings = V2Settings()
        try:
            disk = shutil.disk_usage(settings.storage_root)
        except OSError:
            disk = shutil.disk_usage(".")
        database_bytes = 0
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            database_bytes = int(
                session.scalar(select(func.pg_database_size(func.current_database()))) or 0
            )
        policies = session.scalars(select(ProviderPolicy).order_by(ProviderPolicy.source)).all()
        kavita = configured_kavita_client(local_library_root=settings.storage_root / "library")
        return {
            "job_counts": counts,
            "health": {
                "series": session.scalar(select(func.count()).select_from(CatalogSeries)) or 0,
                "chapters": session.scalar(select(func.count()).select_from(CatalogChapter)) or 0,
                "active_artifacts": session.scalar(
                    select(func.count())
                    .select_from(ChapterArtifact)
                    .where(ChapterArtifact.state == "active")
                )
                or 0,
                "storage_free_bytes": disk.free,
                "storage_total_bytes": disk.total,
                "database_bytes": database_bytes,
                "kavita_configured": int(kavita.configured),
                "missing_projections": session.scalar(
                    select(func.count())
                    .select_from(ChapterArtifact)
                    .outerjoin(
                        LibraryProjection, LibraryProjection.artifact_id == ChapterArtifact.id
                    )
                    .where(
                        ChapterArtifact.state == "active", LibraryProjection.artifact_id.is_(None)
                    )
                )
                or 0,
            },
            "sources": [
                {
                    "source": row.source,
                    "status": row.health_status,
                    "failures": row.consecutive_failures or 0,
                    "last_error": row.last_error,
                    "last_poll_at": row.last_poll_at.isoformat() if row.last_poll_at else None,
                    "cooldown_until": row.cooldown_until.isoformat()
                    if row.cooldown_until
                    else None,
                    "enabled": row.manual_enabled,
                }
                for row in sources
            ],
            "workers": [
                {
                    "id": row.worker_id,
                    "status": row.status,
                    "active_job_id": row.active_job_id,
                    "heartbeat_at": row.heartbeat_at.isoformat(),
                    "metadata": row.metadata_json or {},
                }
                for row in workers
            ],
            "permits": permit_counts,
            "provider_policies": [
                {
                    "source": row.source,
                    "job_limit": row.learned_job_limit,
                    "page_limit": row.learned_page_limit,
                    "cooldown_seconds": row.cooldown_seconds,
                    "clean_since": row.clean_since.isoformat() if row.clean_since else None,
                    "last_limited_at": row.last_limited_at.isoformat()
                    if row.last_limited_at
                    else None,
                    "next_exploration_at": row.next_exploration_at.isoformat()
                    if row.next_exploration_at
                    else None,
                }
                for row in policies
            ],
            "recent_benchmarks": [
                {
                    "id": row.id,
                    "source": row.source,
                    "state": row.state,
                    "tier": row.requested_tier,
                    "requests": row.request_count,
                    "failures": row.failure_count,
                    "signal": row.limiting_signal,
                }
                for row in session.scalars(
                    select(ProviderBenchmarkRun)
                    .order_by(ProviderBenchmarkRun.id.desc())
                    .limit(10)
                )
            ],
        }

    @router.post("/sources/{source}/pull")
    async def pull_source(source: str, session: SessionDep):
        if source not in {"asura", "mangafire", "kingofshojo"}:
            raise HTTPException(422, "invalid source")
        with session.begin():
            job, _created = JobQueue().enqueue(
                session,
                kind=JobKind.SOURCE_PULL,
                dedupe_key=f"source:{source}",
                payload=SourcePullPayload(source=source),
                priority=10,
            )
        return {"job": serialize_jobs(session, [job])[0]}

    @router.post("/probe")
    async def probe(session: SessionDep):
        with session.begin():
            job, _created = JobQueue().enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key="operations-probe",
                payload=MaintenancePayload(action="stage_probe"),
                priority=1,
            )
        return {"job": serialize_jobs(session, [job])[0]}

    @router.post("/jobs/{job_id}/retry")
    async def retry_job(job_id: int, session: SessionDep):
        with session.begin():
            job = session.get(WorkJob, job_id)
            if job is None or job.status not in {"failed", "cancelled"}:
                raise HTTPException(409, "job cannot be retried")
            job.status = "queued"
            job.max_attempts = max(job.max_attempts, job.attempts + 3)
            job.available_at = utcnow()
            job.lease_owner = ""
            job.lease_expires_at = None
            job.error_code = ""
            job.error_message = ""
            job.completed_at = None
            job.updated_at = utcnow()
        return {"job": serialize_jobs(session, [job])[0]}

    @router.get("/events")
    async def events(request: Request, after: int = Query(default=0, ge=0)):
        async def stream():
            cursor = after
            while not await request.is_disconnected():
                with factory_provider()() as session:
                    rows = session.scalars(
                        select(JobEvent)
                        .where(JobEvent.id > cursor)
                        .order_by(JobEvent.id)
                        .limit(100)
                    ).all()
                    for event in rows:
                        cursor = event.id
                        payload = {
                            "event_id": event.id,
                            "job_id": event.job_id,
                            "state": event.status,
                            "type": event.event_type,
                            "message": event.message,
                            "timestamp": event.created_at.isoformat(),
                        }
                        yield f"id: {event.id}\nevent: job\ndata: {json.dumps(payload)}\n\n"
                    if rows:
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

    return router


def serialize_series(
    session: Session, rows: list[CatalogSeries], include_progress: bool = False
) -> list[dict[str, Any]]:
    ids = [row.id for row in rows]
    sources: dict[int, list[dict[str, str]]] = defaultdict(list)
    for source in session.scalars(
        select(CatalogSourceSeries)
        .where(CatalogSourceSeries.series_id.in_(ids or [-1]))
        .order_by(CatalogSourceSeries.source)
    ).all():
        sources[source.series_id].append(
            {"name": source.source, "title": source.title, "url": source.url}
        )
    aliases: dict[int, list[str]] = defaultdict(list)
    for series_id, value in session.execute(
        select(CatalogSeriesAlias.series_id, CatalogSeriesAlias.display_value)
        .where(CatalogSeriesAlias.series_id.in_(ids or [-1]))
        .order_by(CatalogSeriesAlias.id)
    ).all():
        aliases[series_id].append(value)
    totals: dict[int, int] = {}
    reads: dict[int, int] = {}
    if include_progress and ids:
        totals = dict(
            session.execute(
                select(CatalogChapter.series_id, func.count())
                .where(CatalogChapter.series_id.in_(ids))
                .group_by(CatalogChapter.series_id)
            ).all()
        )
        reads = dict(
            session.execute(
                select(CatalogChapter.series_id, func.count())
                .join(
                    CatalogChapterReadingState,
                    CatalogChapterReadingState.chapter_id == CatalogChapter.id,
                )
                .where(
                    CatalogChapter.series_id.in_(ids), CatalogChapterReadingState.status == "read"
                )
                .group_by(CatalogChapter.series_id)
            ).all()
        )
    return [
        {
            "id": row.id,
            "title": row.title,
            "description": row.description,
            "cover_url": row.cover_url,
            "status": row.status,
            "integrity_state": row.integrity_state,
            "latest_chapter": row.latest_release_number,
            "latest_source": row.latest_release_source,
            "latest_at": row.latest_release_at.isoformat() if row.latest_release_at else None,
            "sources": sources[row.id],
            "aliases": aliases[row.id],
            "chapter_count": totals.get(row.id, 0),
            "read_count": reads.get(row.id, 0),
            "unread_count": max(totals.get(row.id, 0) - reads.get(row.id, 0), 0),
        }
        for row in rows
    ]


def serialize_jobs(session: Session, rows: list[WorkJob]) -> list[dict[str, Any]]:
    release_ids = [
        int(row.payload.get("chapter_release_id"))
        for row in rows
        if row.kind == "chapter_download" and row.payload.get("chapter_release_id")
    ]
    chapter_context: dict[int, dict[str, Any]] = {}
    if release_ids:
        for release, chapter, series in session.execute(
            select(CatalogChapterRelease, CatalogChapter, CatalogSeries)
            .join(CatalogChapter, CatalogChapter.id == CatalogChapterRelease.chapter_id)
            .join(CatalogSeries, CatalogSeries.id == CatalogChapter.series_id)
            .where(CatalogChapterRelease.id.in_(release_ids))
        ).all():
            chapter_context[release.id] = {
                "series_id": series.id,
                "title": series.title,
                "cover_url": series.cover_url,
                "chapter": chapter.display_number,
            }
    result = []
    for row in rows:
        context: dict[str, Any] = {}
        if row.kind == "chapter_download":
            context = chapter_context.get(int(row.payload.get("chapter_release_id", 0)), {})
            description = f"Download {context.get('title', 'chapter')} · Chapter {context.get('chapter', '?')}"
        elif row.kind == "source_pull":
            description = (
                f"Pull latest releases from {row.source or row.payload.get('source', 'source')}"
            )
        elif row.kind == "kavita_sync":
            description = "Synchronize downloaded manga with Kavita"
        elif row.kind == "maintenance":
            description = "Run storage and database health probe"
        else:
            description = row.kind.replace("_", " ").title()
        result.append(
            {
                "id": row.id,
                "kind": row.kind,
                "description": description,
                "source": row.source,
                "pool": row.pool,
                "status": row.status,
                "attempt": row.attempts,
                "max_attempts": row.max_attempts,
                "error_code": row.error_code,
                "error_message": row.error_message,
                "available_at": row.available_at.isoformat(),
                "created_at": row.created_at.isoformat(),
                "updated_at": row.updated_at.isoformat(),
                "completed_at": row.completed_at.isoformat() if row.completed_at else None,
                "progress": {
                    "phase": row.progress_phase,
                    "current": row.progress_current,
                    "total": row.progress_total,
                    "unit": row.progress_unit,
                    "bytes": row.progress_bytes,
                    "message": row.progress_message,
                    "updated_at": row.progress_updated_at.isoformat()
                    if row.progress_updated_at
                    else None,
                    "percent": round(row.progress_current / row.progress_total * 100, 1)
                    if row.progress_total
                    else None,
                },
                "context": context,
            }
        )
    return result


def human_evidence(raw: dict[str, Any] | None) -> list[dict[str, str]]:
    data = raw or {}
    labels: list[dict[str, str]] = []
    if data.get("shared_external_id") or data.get("external_id"):
        labels.append({"tone": "positive", "label": "Shared external ID"})
    if data.get("cover_match") is True:
        labels.append({"tone": "positive", "label": "Cover match"})
    elif data.get("cover_compared") and data.get("cover_match") is False:
        labels.append({"tone": "warning", "label": "Cover mismatch"})
    if data.get("title_or_alias") or data.get("title_match"):
        labels.append({"tone": "positive", "label": "Strong title or alias match"})
    if data.get("chapter_fingerprint"):
        labels.append({"tone": "positive", "label": "Chapter fingerprint match"})
    if "manual_review" in str(data.get("policy", "")):
        labels.append({"tone": "neutral", "label": "Manual review required"})
    return labels or [{"tone": "neutral", "label": "Similarity candidate"}]


def encode_cursor(timestamp: datetime | None, row_id: int) -> str:
    return f"{timestamp.isoformat() if timestamp else 'none'}|{row_id}"


def decode_cursor(value: str) -> tuple[datetime | None, int]:
    try:
        raw_time, raw_id = value.rsplit("|", 1)
        return (None if raw_time == "none" else datetime.fromisoformat(raw_time), int(raw_id))
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, "invalid cursor") from exc
