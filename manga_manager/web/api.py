import asyncio
import json
import shutil
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from collections.abc import Callable
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import String, and_, case, func, or_, select
from sqlalchemy.orm import Session, aliased, sessionmaker

from app.kavita import configured_kavita_client

from manga_manager.domain.jobs import (
    JobKind,
    KavitaSyncPayload,
    MaintenancePayload,
    SourcePullPayload,
)
from manga_manager.application.download_plans import DownloadPlanCoordinator
from manga_manager.application.kavita_sync import KavitaSyncPlanner
from manga_manager.application.library_repair import enqueue_library_repair
from manga_manager.application.match_training import record_training_label
from manga_manager.application.matching_score import score_candidate_set
from manga_manager.application.provider_duplicates import equivalent_series_clusters
from manga_manager.application.cover_evidence import (
    ensure_cover_thumbnail,
    thumbnail_relative_path,
)

from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterReadingState,
    CatalogChapterRelease,
    CatalogCoverAsset,
    CatalogCoverSignature,
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
    ProviderEndpointState,
    ProviderPolicy,
    SeriesDownloadPlan,
    StorageState,
    WorkerHeartbeat,
    WorkJob,
    WorkloadCycle,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.settings import V2Settings
from manga_manager.domain.providers import SOURCE_PRIORITY, provider_names
from manga_manager.domain.matching import provider_identities_equivalent


TRACKED_STATES = ("interested", "reading", "caught_up", "paused")
ACTIVE_JOB_STATES = ("queued", "leased", "retry_wait")


def job_state_predicate(states: list[str]):
    """Show only the latest failed outcome for each logical job.

    A retry is represented by a new row with the same kind/dedupe key. Once that row exists the
    earlier failure is historical, regardless of whether the retry is queued, running, or has
    reached another terminal state.
    """
    if not states:
        return None
    non_failed = [state for state in states if state != "failed"]
    if "failed" not in states:
        return WorkJob.status.in_(non_failed)
    later = aliased(WorkJob)
    unresolved = (
        ~select(later.id)
        .where(
            later.kind == WorkJob.kind,
            later.dedupe_key == WorkJob.dedupe_key,
            later.id > WorkJob.id,
        )
        .exists()
    )
    failed = and_(WorkJob.status == "failed", unresolved)
    return or_(WorkJob.status.in_(non_failed), failed) if non_failed else failed


def merge_blocking_job_predicate():
    """Return active work that can make a catalog merge unsafe.

    Match rescoring only reads evidence.  Treating those low-priority jobs as
    writers made a scorer upgrade lock the entire review queue.
    """
    action = func.coalesce(WorkJob.payload["action"].as_string(), "")
    return and_(
        WorkJob.status.in_(ACTIVE_JOB_STATES),
        or_(WorkJob.kind != JobKind.MAINTENANCE.value, action != "rescore_matches"),
    )


class SeriesStateChange(BaseModel):
    status: str


class ChapterStateChange(BaseModel):
    status: str


class MatchChange(BaseModel):
    decision: str
    confirmation: str = ""


class BatchMatchChange(MatchChange):
    ids: list[int] = []
    entire_queue: bool = False


class MergeSeriesChange(BaseModel):
    series_ids: list[int]
    confirmation: str = ""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def provider_merge_conflicts(session: Session, series_ids: list[int]) -> list[str]:
    identities = session.scalars(
        select(CatalogSourceSeries).where(CatalogSourceSeries.series_id.in_(series_ids))
    ).all()
    grouped: dict[str, list[CatalogSourceSeries]] = defaultdict(list)
    for identity in identities:
        grouped[identity.source].append(identity)
    conflicts: list[str] = []
    for source, source_identities in grouped.items():
        if len(source_identities) < 2:
            continue
        resolvable = True
        for index, left_identity in enumerate(source_identities):
            left_numbers = set(
                session.scalars(
                    select(CatalogChapter.canonical_number)
                    .join(CatalogChapterRelease)
                    .where(CatalogChapterRelease.source_series_id == left_identity.id)
                ).all()
            )
            for right_identity in source_identities[index + 1 :]:
                if provider_identities_equivalent(left_identity, right_identity):
                    continue
                right_numbers = set(
                    session.scalars(
                        select(CatalogChapter.canonical_number)
                        .join(CatalogChapterRelease)
                        .where(CatalogChapterRelease.source_series_id == right_identity.id)
                    ).all()
                )
                overlap = len(left_numbers & right_numbers)
                minimum = min(len(left_numbers), len(right_numbers))
                if overlap < 2 or (minimum and overlap / minimum < 0.5):
                    resolvable = False
        if not resolvable:
            conflicts.append(source)
    return sorted(conflicts)


def storage_health(session: Session) -> dict[str, int]:
    state = session.get(StorageState, 1)
    if state is None:
        return {
            "storage_paused": 0,
            "storage_reserved_bytes": 0,
            "storage_min_free_bytes": V2Settings().min_free_bytes,
        }
    return {
        "storage_paused": int(state.paused),
        "storage_reserved_bytes": state.reserved_bytes,
        "storage_min_free_bytes": state.min_free_bytes,
    }


def workload_cycle_summary(session: Session, cycle: WorkloadCycle) -> dict[str, Any]:
    active_units = int(
        session.scalar(
            select(func.coalesce(func.sum(WorkJob.logical_units), 0)).where(
                WorkJob.cycle_id == cycle.id,
                WorkJob.status.in_(ACTIVE_JOB_STATES),
            )
        )
        or 0
    )
    coalesced = int(
        session.scalar(
            select(func.coalesce(func.sum(WorkJob.logical_units), 0)).where(
                WorkJob.cycle_id == cycle.id,
                WorkJob.status == "cancelled",
                WorkJob.error_message.like("coalesced into library repair job %"),
            )
        )
        or 0
    )
    # A retry is a separate durable attempt, so the cycle's historical failed counter
    # intentionally never decreases.  In the live UI, however, an older failed attempt is
    # superseded once a later attempt exists, and a dismissed latest failure is a
    # cancellation.  Reclassify those units without rewriting audit history.
    unresolved_failed = int(
        session.scalar(
            select(func.coalesce(func.sum(WorkJob.logical_units), 0)).where(
                WorkJob.cycle_id == cycle.id,
                job_state_predicate(["failed"]),
            )
        )
        or 0
    )
    dismissed = int(
        session.scalar(
            select(func.coalesce(func.sum(WorkJob.logical_units), 0)).where(
                WorkJob.cycle_id == cycle.id,
                WorkJob.status == "cancelled",
                select(JobEvent.id)
                .where(
                    JobEvent.job_id == WorkJob.id,
                    JobEvent.event_type == "cancelled",
                    JobEvent.message == "failure dismissed from the live job center",
                )
                .exists(),
            )
        )
        or 0
    )
    superseded_failures = max(cycle.failed_units - unresolved_failed - dismissed, 0)
    cancelled = max(cycle.cancelled_units - coalesced, 0) + dismissed
    superseded = coalesced + superseded_failures
    settled = cycle.successful_units + unresolved_failed + cancelled + superseded
    effective_total = max(cycle.total_units, settled + active_units)
    return {
        "id": cycle.id,
        "status": cycle.status,
        "total": effective_total,
        "successful": cycle.successful_units,
        "failed": unresolved_failed,
        "cancelled": cancelled,
        "superseded": superseded,
        "remaining": active_units,
        "added": cycle.added_units,
        "started_at": cycle.started_at.isoformat(),
        "updated_at": cycle.updated_at.isoformat(),
    }


def pending_match_proposal_index(
    session: Session,
    *,
    series_pair: tuple[int, int] | None = None,
) -> list[dict[str, Any]]:
    """Return proposal membership without loading large JSON evidence documents.

    Match decisions are recorded between provider identities, while review happens between
    canonical manga.  A canonical pair can therefore contain several decisions.  Keep this
    first pass scalar-only: Suggested Matches is cursor-paginated and must not materialize
    every decision ORM object (and its evidence/feature JSON) for every visible page.
    """
    left_alias = aliased(CatalogSourceSeries)
    right_alias = aliased(CatalogSourceSeries)
    query = (
        select(
            CatalogMatchDecision.id,
            CatalogMatchDecision.confidence,
            left_alias.series_id.label("left_series_id"),
            right_alias.series_id.label("right_series_id"),
        )
        .join(left_alias, left_alias.id == CatalogMatchDecision.left_source_series_id)
        .join(right_alias, right_alias.id == CatalogMatchDecision.right_source_series_id)
        .where(CatalogMatchDecision.decision == "pending")
    )
    if series_pair is not None:
        left_series_id, right_series_id = series_pair
        query = query.where(
            or_(
                and_(
                    left_alias.series_id == left_series_id,
                    right_alias.series_id == right_series_id,
                ),
                and_(
                    left_alias.series_id == right_series_id,
                    right_alias.series_id == left_series_id,
                ),
            )
        )
    rows = session.execute(query).all()
    all_series_ids = {
        series_id
        for _decision_id, _confidence, left_series_id, right_series_id in rows
        for series_id in (left_series_id, right_series_id)
    }
    equivalent_clusters = equivalent_series_clusters(session, all_series_ids)
    grouped: dict[tuple[int, int], dict[str, Any]] = {}
    for decision_id, confidence, left_series_id, right_series_id in rows:
        left_cluster = equivalent_clusters.get(left_series_id, (left_series_id,))
        right_cluster = equivalent_clusters.get(right_series_id, (right_series_id,))
        left_root, right_root = left_cluster[0], right_cluster[0]
        if left_root == right_root:
            # GET requests remain read-only. Maintenance and merge transactions close obsolete
            # source-level decisions; until then they are simply omitted from review.
            continue
        key = tuple(sorted((left_root, right_root)))
        member_series_ids = sorted(set(left_cluster) | set(right_cluster))
        proposal = grouped.get(key)
        if proposal is None:
            grouped[key] = {
                "id": decision_id,
                "confidence": confidence,
                "decision_ids": [decision_id],
                "series_ids": member_series_ids,
            }
        else:
            proposal["decision_ids"].append(decision_id)
            proposal["series_ids"] = sorted(
                set(proposal["series_ids"]) | set(member_series_ids)
            )
            if (confidence, -decision_id) > (proposal["confidence"], -proposal["id"]):
                proposal.update(id=decision_id, confidence=confidence)
    for proposal in grouped.values():
        proposal["decision_ids"].sort()
    return sorted(grouped.values(), key=lambda row: (-row["confidence"], row["id"]))


def hydrate_match_proposals(
    session: Session,
    proposals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Load ORM details only for proposal representatives that will be serialized."""
    if not proposals:
        return []
    representative_ids = [row["id"] for row in proposals]
    left_alias = aliased(CatalogSourceSeries)
    right_alias = aliased(CatalogSourceSeries)
    details = {
        decision_id: (evidence_json, left, right)
        for decision_id, evidence_json, left, right in session.execute(
            select(
                CatalogMatchDecision.id,
                CatalogMatchDecision.evidence_json,
                left_alias,
                right_alias,
            )
            .join(left_alias, left_alias.id == CatalogMatchDecision.left_source_series_id)
            .join(right_alias, right_alias.id == CatalogMatchDecision.right_source_series_id)
            .where(CatalogMatchDecision.id.in_(representative_ids))
        )
    }
    result: list[dict[str, Any]] = []
    for proposal in proposals:
        detail = details.get(proposal["id"])
        if detail is None:
            continue
        evidence_json, left, right = detail
        result.append(
            {**proposal, "evidence_json": evidence_json, "left": left, "right": right}
        )
    return result


def pending_match_proposal(
    session: Session,
    representative_id: int,
) -> dict[str, Any] | None:
    """Resolve one review proposal without rebuilding/hydrating the complete queue."""
    left_alias = aliased(CatalogSourceSeries)
    right_alias = aliased(CatalogSourceSeries)
    row = session.execute(
        select(left_alias.series_id, right_alias.series_id)
        .select_from(CatalogMatchDecision)
        .join(left_alias, left_alias.id == CatalogMatchDecision.left_source_series_id)
        .join(right_alias, right_alias.id == CatalogMatchDecision.right_source_series_id)
        .where(
            CatalogMatchDecision.id == representative_id,
            CatalogMatchDecision.decision == "pending",
        )
    ).one_or_none()
    if row is None or row[0] == row[1]:
        return None
    return next(
        (
            proposal
            for proposal in pending_match_proposal_index(session)
            if proposal["id"] == representative_id
        ),
        None,
    )


def proposal_blockers(session: Session, series_ids: list[int]) -> list[str]:
    blockers = [
        f"provider conflict: {value}" for value in provider_merge_conflicts(session, series_ids)
    ]
    active = session.scalar(
        select(WorkJob.id)
        .where(
            WorkJob.series_key.in_([str(value) for value in series_ids]),
            merge_blocking_job_predicate(),
        )
        .limit(1)
    )
    if active is not None:
        blockers.append("active jobs")
    return blockers


def connected_proposal_components(proposals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    remaining = list(proposals)
    result = []
    while remaining:
        component = [remaining.pop(0)]
        series_ids = set(component[0]["series_ids"])
        changed = True
        while changed:
            changed = False
            for proposal in list(remaining):
                if series_ids.intersection(proposal["series_ids"]):
                    remaining.remove(proposal)
                    component.append(proposal)
                    series_ids.update(proposal["series_ids"])
                    changed = True
        result.append({"proposals": component, "series_ids": sorted(series_ids)})
    return result


def proposal_component_blockers(session: Session, series_ids: list[int]) -> list[str]:
    """Apply the same validation to batch previews and batch execution."""
    # Suggested components may contain more canonical records than providers when historical
    # pulls created duplicate identities. That is safe only when provider_merge_conflicts can
    # prove every repeated provider identity equivalent (or strongly chapter-overlapping).
    return proposal_blockers(session, sorted(set(series_ids)))


def create_api_router(
    factory_provider: Callable[[], sessionmaker[Session]],
) -> APIRouter:
    router = APIRouter(prefix="/api/v2")
    storage_root = V2Settings().storage_root

    def sessions():
        with factory_provider()() as session:
            yield session

    SessionDep = Annotated[Session, Depends(sessions)]

    def mark_series_read_and_queue_kavita(
        session: Session,
        series: CatalogSeries,
        changed_at: datetime,
    ) -> int:
        chapter_ids = session.scalars(
            select(CatalogChapter.id).where(CatalogChapter.series_id == series.id)
        ).all()
        readings = {
            row.chapter_id: row
            for row in session.scalars(
                select(CatalogChapterReadingState).where(
                    CatalogChapterReadingState.chapter_id.in_(chapter_ids or [-1])
                )
            )
        }
        for chapter_id in chapter_ids:
            reading = readings.get(chapter_id)
            if reading is None:
                reading = CatalogChapterReadingState(chapter_id=chapter_id)
                session.add(reading)
            reading.status = "read"
            reading.read_at = reading.read_at or changed_at
            reading.updated_at = changed_at
        if series.status not in {"paused", "untracked"}:
            series.status = "caught_up"
        series.updated_at = changed_at
        kavita_job, _created = JobQueue().enqueue(
            session,
            kind=JobKind.KAVITA_SYNC,
            dedupe_key=f"series:{series.id}",
            payload=KavitaSyncPayload(series_id=series.id, reading_status="read"),
            priority=20,
            series_key=str(series.id),
            coalesce=True,
        )
        if kavita_job.status in {"queued", "retry_wait"}:
            kavita_job.priority = min(kavita_job.priority, 20)
            kavita_job.available_at = changed_at
        return len(chapter_ids)

    @router.get("/providers")
    def providers():
        return {"items": list(provider_names())}

    @router.get("/covers/{series_id}/{checksum}.webp", include_in_schema=False)
    def cover_thumbnail(
        series_id: int,
        checksum: str,
        request: Request,
        session: SessionDep,
    ):
        if len(checksum) != 64 or any(character not in "0123456789abcdef" for character in checksum):
            raise HTTPException(status_code=404, detail="cover not found")
        asset = session.scalar(
            select(CatalogCoverAsset)
            .join(
                CatalogSourceSeries,
                CatalogSourceSeries.id == CatalogCoverAsset.source_series_id,
            )
            .where(
                CatalogSourceSeries.series_id == series_id,
                CatalogCoverAsset.content_checksum == checksum,
            )
            .limit(1)
        )
        if asset is None:
            raise HTTPException(status_code=404, detail="cover not found")
        etag = f'"{checksum}"'
        cache_headers = {
            "Cache-Control": "public, max-age=31536000, immutable",
            "ETag": etag,
        }
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers=cache_headers)
        path = storage_root / thumbnail_relative_path(checksum)
        if not path.is_file():
            original = storage_root / asset.relative_path
            if not original.is_file():
                raise HTTPException(status_code=404, detail="cover not found")
            try:
                ensure_cover_thumbnail(original, checksum, storage_root)
            except (OSError, ValueError):
                raise HTTPException(status_code=404, detail="cover not found") from None
        return FileResponse(
            path,
            media_type="image/webp",
            headers=cache_headers,
        )

    @router.get("/discovery")
    def discovery(
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
        valid_sources = sorted(set(source).intersection(provider_names()))
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
    def library(
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
                or_(
                    CatalogSeries.title.ilike(term),
                    CatalogSeries.description.ilike(term),
                    select(CatalogSeriesAlias.id)
                    .where(
                        CatalogSeriesAlias.series_id == CatalogSeries.id,
                        CatalogSeriesAlias.display_value.ilike(term),
                    )
                    .exists(),
                )
            )
        valid_sources = sorted(set(source).intersection(provider_names()))
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
    def change_series(series_id: int, change: SeriesStateChange, session: SessionDep):
        allowed = {"untracked", *TRACKED_STATES}
        if change.status not in allowed:
            raise HTTPException(422, "invalid series state")
        with session.begin():
            row = session.get(CatalogSeries, series_id)
            if row is None:
                raise HTTPException(404, "series not found")
            previous = row.status
            row.status = change.status
            changed_at = utcnow()
            row.updated_at = changed_at
            if change.status == "caught_up":
                mark_series_read_and_queue_kavita(session, row, changed_at)
            coordinator = DownloadPlanCoordinator()
            if change.status in TRACKED_STATES:
                coordinator.track(session, row.id)
            else:
                coordinator.untrack(session, row.id)
            enqueue_library_repair(
                session,
                series_id=row.id,
                reason="tracking",
                priority=75,
            )
        return {
            "item": serialize_series(session, [row], include_progress=True)[0],
            "previous": previous,
        }

    @router.patch("/chapters/{chapter_id}")
    def change_chapter(chapter_id: int, change: ChapterStateChange, session: SessionDep):
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
    def read_all(series_id: int, session: SessionDep):
        now = utcnow()
        with session.begin():
            series = session.get(CatalogSeries, series_id)
            if series is None:
                raise HTTPException(404, "series not found")
            updated = mark_series_read_and_queue_kavita(session, series, now)
        return {"series_id": series_id, "updated": updated}

    @router.get("/updates")
    def updates(
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
            .order_by(
                CatalogChapter.series_id,
                CatalogChapter.sort_number.desc().nullslast(),
                release_time.desc(),
                CatalogChapterRelease.id.desc(),
            )
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
    def matches(
        session: SessionDep,
        cursor: int = Query(default=0, ge=0),
        limit: int = Query(default=24, ge=1, le=100),
    ):
        proposals = pending_match_proposal_index(session)
        if cursor:
            current_index = next(
                (index for index, value in enumerate(proposals) if value["id"] == cursor),
                None,
            )
            if current_index is not None:
                start = current_index + 1
            else:
                # The preceding proposal may have just been reviewed. Its decision row remains,
                # so its stable confidence/id position can still advance this cursor correctly.
                cursor_confidence = session.scalar(
                    select(CatalogMatchDecision.confidence).where(
                        CatalogMatchDecision.id == cursor
                    )
                )
                cursor_key = (-cursor_confidence, cursor) if cursor_confidence is not None else None
                start = next(
                    (
                        index
                        for index, value in enumerate(proposals)
                        if cursor_key is not None
                        and (-value["confidence"], value["id"]) > cursor_key
                    ),
                    len(proposals),
                )
        else:
            start = 0
        rows = hydrate_match_proposals(session, proposals[start : start + limit])
        canonical_ids = {
            series_id
            for row in rows
            for series_id in row["series_ids"]
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
        matched_cover_identity_ids = {
            int(identity_id)
            for row in rows
            for key in (
                "cover_left_source_series_id",
                "cover_right_source_series_id",
            )
            for identity_id in [row["evidence_json"].get(key)]
            if identity_id
        }
        source_identity_ids = {
            source.id for row in rows for source in (row["left"], row["right"])
        } | matched_cover_identity_ids
        matched_identities = {
            identity.id: identity
            for identity in session.scalars(
                select(CatalogSourceSeries).where(
                    CatalogSourceSeries.id.in_(matched_cover_identity_ids or {-1})
                )
            )
        }
        cover_urls_by_identity = {
            source_series_id: f"/api/v2/covers/{series_id}/{checksum}.webp"
            for source_series_id, series_id, checksum in session.execute(
                select(
                    CatalogCoverAsset.source_series_id,
                    CatalogSourceSeries.series_id,
                    CatalogCoverAsset.content_checksum,
                )
                .join(
                    CatalogSourceSeries,
                    CatalogSourceSeries.id == CatalogCoverAsset.source_series_id,
                )
                .where(CatalogCoverAsset.source_series_id.in_(source_identity_ids or {-1}))
            )
        }

        def matched_identity(row: dict[str, Any], series_id: int, fallback: CatalogSourceSeries):
            for key in (
                "cover_left_source_series_id",
                "cover_right_source_series_id",
            ):
                identity = matched_identities.get(int(row["evidence_json"].get(key) or 0))
                if identity is not None and identity.series_id == series_id:
                    return identity
            return fallback

        def row_matched_cover_ids(row: dict[str, Any]) -> set[int]:
            return {
                int(row["evidence_json"].get(key) or 0)
                for key in (
                    "cover_left_source_series_id",
                    "cover_right_source_series_id",
                )
                if row["evidence_json"].get(key)
            }

        latest_by_identity: dict[int, str] = {}
        for source_series_id, display_number in session.execute(
            select(
                CatalogChapterRelease.source_series_id,
                CatalogChapter.display_number,
            )
            .join(CatalogChapter, CatalogChapter.id == CatalogChapterRelease.chapter_id)
            .where(CatalogChapterRelease.source_series_id.in_(source_identity_ids or {-1}))
            .order_by(
                CatalogChapterRelease.source_series_id,
                CatalogChapter.sort_number.desc().nullslast(),
                CatalogChapterRelease.published_at.desc().nullslast(),
                CatalogChapterRelease.id.desc(),
            )
        ):
            latest_by_identity.setdefault(source_series_id, str(display_number))
        sources_by_series: dict[int, set[str]] = defaultdict(set)
        for series_id, source_name in session.execute(
            select(CatalogSourceSeries.series_id, CatalogSourceSeries.source).where(
                CatalogSourceSeries.series_id.in_(canonical_ids or {-1})
            )
        ):
            sources_by_series[series_id].add(source_name)
        active_series_keys = set(
            session.scalars(
                select(WorkJob.series_key).where(
                    WorkJob.series_key.in_([str(value) for value in canonical_ids] or ["-"]),
                    merge_blocking_job_predicate(),
                )
            )
        )
        blockers_by_proposal: dict[int, list[str]] = {}
        for row in rows:
            blockers = []
            if any(str(value) in active_series_keys for value in row["series_ids"]):
                blockers.append("active jobs")
            source_counts: dict[str, int] = defaultdict(int)
            for series_id in row["series_ids"]:
                for source_name in sources_by_series[series_id]:
                    source_counts[source_name] += 1
            if any(count > 1 for count in source_counts.values()):
                blockers.extend(
                    f"provider conflict: {value}"
                    for value in provider_merge_conflicts(session, row["series_ids"])
                )
            blockers_by_proposal[row["id"]] = blockers
        display_identities = {
            row["id"]: (
                matched_identity(row, row["left"].series_id, row["left"]),
                matched_identity(row, row["right"].series_id, row["right"]),
            )
            for row in rows
        }
        return {
            "items": [
                {
                    "id": row["id"],
                    "decision_ids": row["decision_ids"],
                    "confidence": row["confidence"],
                    "evidence": human_evidence(row["evidence_json"]),
                    "blocked_reasons": blockers_by_proposal[row["id"]],
                    "left": {
                        **canonical[row["left"].series_id],
                        "source_title": display_identities[row["id"]][0].title,
                        "source": display_identities[row["id"]][0].source,
                        "url": display_identities[row["id"]][0].url,
                        "cover_url": cover_urls_by_identity.get(
                            display_identities[row["id"]][0].id,
                            display_identities[row["id"]][0].cover_url,
                        ),
                        "latest_chapter": latest_by_identity.get(
                            display_identities[row["id"]][0].id, ""
                        ),
                        "cover_evidence_used": (
                            display_identities[row["id"]][0].id
                            in row_matched_cover_ids(row)
                        ),
                    },
                    "right": {
                        **canonical[row["right"].series_id],
                        "source_title": display_identities[row["id"]][1].title,
                        "source": display_identities[row["id"]][1].source,
                        "url": display_identities[row["id"]][1].url,
                        "cover_url": cover_urls_by_identity.get(
                            display_identities[row["id"]][1].id,
                            display_identities[row["id"]][1].cover_url,
                        ),
                        "latest_chapter": latest_by_identity.get(
                            display_identities[row["id"]][1].id, ""
                        ),
                        "cover_evidence_used": (
                            display_identities[row["id"]][1].id
                            in row_matched_cover_ids(row)
                        ),
                    },
                }
                for row in rows
            ],
            "next_cursor": rows[-1]["id"] if len(rows) == limit else None,
            "total": len(proposals),
        }

    @router.get("/merge-candidates")
    def merge_candidates(
        session: SessionDep,
        selected_id: list[int] = Query(default=[]),
        anchor_id: int | None = Query(default=None, gt=0),
        q: str = Query(default="", max_length=200),
        source: list[str] = Query(default=[]),
        cursor: int = Query(default=0, ge=0),
        limit: int = Query(default=24, ge=1, le=100),
    ):
        selected_ids = sorted(set(selected_id or ([anchor_id] if anchor_id else [])))
        if not selected_ids:
            raise HTTPException(422, "select at least one manga")
        if len(selected_ids) > len(provider_names()):
            raise HTTPException(422, f"select at most {len(provider_names())} manga")
        selected_rows = session.scalars(
            select(CatalogSeries).where(CatalogSeries.id.in_(selected_ids))
        ).all()
        if len(selected_rows) != len(selected_ids) or any(
            row.status not in TRACKED_STATES for row in selected_rows
        ):
            raise HTTPException(404, "selected manga is not in the library")
        selected_identity_ids = session.scalars(
            select(CatalogSourceSeries.id).where(CatalogSourceSeries.series_id.in_(selected_ids))
        ).all()
        decision_candidate_ids: set[int] = set()
        if selected_identity_ids:
            candidate_identity = aliased(CatalogSourceSeries)
            for candidate_id in session.scalars(
                select(candidate_identity.series_id)
                .join(
                    CatalogMatchDecision,
                    or_(
                        and_(
                            CatalogMatchDecision.left_source_series_id.in_(selected_identity_ids),
                            CatalogMatchDecision.right_source_series_id == candidate_identity.id,
                        ),
                        and_(
                            CatalogMatchDecision.right_source_series_id.in_(selected_identity_ids),
                            CatalogMatchDecision.left_source_series_id == candidate_identity.id,
                        ),
                    ),
                )
                .where(CatalogMatchDecision.decision != "rejected")
            ):
                decision_candidate_ids.add(candidate_id)
        query = select(CatalogSeries).where(
            CatalogSeries.id.not_in(selected_ids), CatalogSeries.status.in_(TRACKED_STATES)
        )
        if q.strip():
            term = f"%{q.strip()}%"
            query = query.where(
                or_(
                    CatalogSeries.title.ilike(term),
                    CatalogSeries.description.ilike(term),
                    select(CatalogSeriesAlias.id)
                    .where(
                        CatalogSeriesAlias.series_id == CatalogSeries.id,
                        CatalogSeriesAlias.display_value.ilike(term),
                    )
                    .exists(),
                )
            )
        wanted_sources = set(source).intersection(provider_names())
        if wanted_sources:
            query = query.where(
                select(CatalogSourceSeries.id)
                .where(
                    CatalogSourceSeries.series_id == CatalogSeries.id,
                    CatalogSourceSeries.source.in_(wanted_sources),
                )
                .exists()
            )
        else:
            terms = {
                token
                for selected in selected_rows
                for token in selected.normalized_title.split()
                if len(token) >= 4
            }
            selected_bands = {
                value
                for row in session.execute(
                    select(
                        CatalogCoverSignature.hash_band_0,
                        CatalogCoverSignature.hash_band_1,
                        CatalogCoverSignature.hash_band_2,
                        CatalogCoverSignature.hash_band_3,
                    ).where(
                        CatalogCoverSignature.source_series_id.in_(selected_identity_ids or [-1])
                    )
                )
                for value in row
                if value
            }
            shortlist = [CatalogSeries.id.in_(decision_candidate_ids or {-1})]
            shortlist.extend(CatalogSeries.normalized_title.ilike(f"%{term}%") for term in terms)
            if selected_bands:
                cover_identity = aliased(CatalogSourceSeries)
                cover_signature = aliased(CatalogCoverSignature)
                shortlist.append(
                    select(cover_identity.id)
                    .join(
                        cover_signature,
                        cover_signature.source_series_id == cover_identity.id,
                    )
                    .where(
                        cover_identity.series_id == CatalogSeries.id,
                        or_(
                            cover_signature.hash_band_0.in_(selected_bands),
                            cover_signature.hash_band_1.in_(selected_bands),
                            cover_signature.hash_band_2.in_(selected_bands),
                            cover_signature.hash_band_3.in_(selected_bands),
                        ),
                    )
                    .exists()
                )
            query = query.where(or_(*shortlist))
        rows = session.scalars(
            query.order_by(CatalogSeries.normalized_title, CatalogSeries.id).limit(500)
        ).all()

        identity_rows = session.scalars(
            select(CatalogSourceSeries).where(
                CatalogSourceSeries.series_id.in_([*selected_ids, *[row.id for row in rows]])
            )
        ).all()
        identity_to_series = {identity.id: identity.series_id for identity in identity_rows}
        identities_by_series: dict[int, list[CatalogSourceSeries]] = defaultdict(list)
        for identity in identity_rows:
            identities_by_series[identity.series_id].append(identity)
        identity_ids = set(identity_to_series)

        occupied = {
            identity.source
            for selected in selected_ids
            for identity in identities_by_series[selected]
        }
        configured_providers = set(provider_names())

        def candidate_conflicts(candidate_id: int) -> list[str]:
            candidate_sources = {row.source for row in identities_by_series[candidate_id]}
            conflicts = occupied & candidate_sources
            if configured_providers and configured_providers.issubset(candidate_sources):
                conflicts.add("all configured providers already present")
            return sorted(conflicts)

        scores: dict[int, float] = {}
        if identity_ids:
            for decision in session.scalars(
                select(CatalogMatchDecision).where(
                    CatalogMatchDecision.left_source_series_id.in_(identity_ids),
                    CatalogMatchDecision.right_source_series_id.in_(identity_ids),
                    CatalogMatchDecision.decision != "rejected",
                )
            ):
                left_series = identity_to_series.get(decision.left_source_series_id)
                right_series = identity_to_series.get(decision.right_source_series_id)
                if not set(selected_ids).intersection({left_series, right_series}):
                    continue
                candidate_id = right_series if left_series in selected_ids else left_series
                if candidate_id:
                    scores[candidate_id] = max(scores.get(candidate_id, 0), decision.confidence)
        evidence = score_candidate_set(session, selected_ids, [row.id for row in rows])
        eligible = [row for row in rows if not candidate_conflicts(row.id)]
        ranked = sorted(
            eligible,
            key=lambda row: (
                -max(scores.get(row.id, 0), float(evidence[row.id]["score"])),
                row.normalized_title,
                row.id,
            ),
        )
        page = ranked[cursor : cursor + limit]
        summaries = serialize_series(session, page, include_progress=True)
        by_id = {item["id"]: item for item in summaries}
        items = []
        for row in page:
            score = max(scores.get(row.id, 0), float(evidence[row.id]["score"]))
            conflicts = candidate_conflicts(row.id)
            items.append(
                {
                    **by_id[row.id],
                    "similarity": score,
                    "compatible": not conflicts,
                    "conflicting_sources": conflicts,
                    "score_breakdown": evidence[row.id],
                }
            )
        next_cursor = cursor + len(page) if cursor + len(page) < len(ranked) else None
        return {"items": items, "next_cursor": next_cursor}

    @router.post("/series/merge-preview")
    def preview_series_merge(change: MergeSeriesChange, session: SessionDep):
        ids = sorted(set(change.series_ids))
        provider_count = len(provider_names())
        if not 2 <= len(ids) <= provider_count:
            raise HTTPException(422, f"select two to {provider_count} manga")
        rows = session.scalars(select(CatalogSeries).where(CatalogSeries.id.in_(ids))).all()
        if len(rows) != len(ids):
            raise HTTPException(404, "one or more manga no longer exist")
        if any(row.status not in TRACKED_STATES for row in rows):
            raise HTTPException(422, "manual merges are limited to manga in the library")
        source_rows = session.execute(
            select(CatalogSourceSeries.series_id, CatalogSourceSeries.source).where(
                CatalogSourceSeries.series_id.in_(ids)
            )
        ).all()
        providers: dict[int, set[str]] = defaultdict(set)
        for series_id, provider in source_rows:
            providers[series_id].add(provider)
        conflicts = provider_merge_conflicts(session, ids)
        priority = {
            name: len(SOURCE_PRIORITY) - index for index, name in enumerate(SOURCE_PRIORITY)
        }
        target = max(
            rows,
            key=lambda row: (
                max((priority.get(value, 0) for value in providers[row.id]), default=0),
                int(bool(row.cover_url)) + int(bool(row.description)),
                session.scalar(
                    select(func.count())
                    .select_from(CatalogChapter)
                    .where(CatalogChapter.series_id == row.id)
                )
                or 0,
                row.id * -1,
            ),
        )
        return {
            "target_id": target.id,
            "target_title": target.title,
            "items": serialize_series(session, rows, include_progress=True),
            "conflicting_sources": conflicts,
            "can_merge": not conflicts,
        }

    @router.post("/series/merge")
    def merge_series(change: MergeSeriesChange, session: SessionDep):
        if change.confirmation != "MERGE":
            raise HTTPException(422, "MERGE confirmation required")
        from manga_manager.web.app import merge_canonical_series

        with session.begin():
            ids = sorted(set(change.series_ids))
            provider_count = len(provider_names())
            if not 2 <= len(ids) <= provider_count:
                raise HTTPException(422, f"select two to {provider_count} manga")
            rows = session.scalars(select(CatalogSeries).where(CatalogSeries.id.in_(ids))).all()
            if len(rows) != len(ids):
                raise HTTPException(404, "one or more manga no longer exist")
            if any(row.status not in TRACKED_STATES for row in rows):
                raise HTTPException(422, "manual merges are limited to manga in the library")
            identities = session.scalars(
                select(CatalogSourceSeries).where(
                    CatalogSourceSeries.series_id.in_(set(change.series_ids))
                )
            ).all()
            for index, left in enumerate(identities):
                for right in identities[index + 1 :]:
                    if left.series_id == right.series_id or left.source == right.source:
                        continue
                    record_training_label(
                        session,
                        left_source_series_id=left.id,
                        right_source_series_id=right.id,
                        label=1,
                        origin="manual_merge",
                    )
            target_id = merge_canonical_series(session, change.series_ids)
        return {"target_id": target_id, "merged_ids": sorted(set(change.series_ids))}

    @router.post("/matches/{decision_id}")
    def decide_match(decision_id: int, change: MatchChange, session: SessionDep):
        if change.decision not in {"accepted", "rejected"}:
            raise HTTPException(422, "invalid match decision")
        if change.decision == "accepted" and change.confirmation != "MERGE":
            raise HTTPException(422, "MERGE confirmation required")
        from manga_manager.web.app import merge_canonical_series

        with session.begin():
            proposal = pending_match_proposal(session, decision_id)
            if proposal is None:
                raise HTTPException(404, "match not found")
            blockers = proposal_blockers(session, proposal["series_ids"])
            if change.decision == "accepted" and blockers:
                raise HTTPException(409, "; ".join(blockers))
            decisions = session.scalars(
                select(CatalogMatchDecision).where(
                    CatalogMatchDecision.id.in_(proposal["decision_ids"])
                )
            ).all()
            for decision in decisions:
                record_training_label(
                    session,
                    left_source_series_id=decision.left_source_series_id,
                    right_source_series_id=decision.right_source_series_id,
                    label=int(change.decision == "accepted"),
                    origin="suggested_review",
                    decision=decision,
                )
                decision.decision = change.decision
                decision.decided_by = "operator"
                decision.decided_at = utcnow()
            if change.decision == "accepted":
                merge_canonical_series(session, proposal["series_ids"])
        return {"id": decision_id, "decision": change.decision}

    @router.post("/match-batch/preview")
    def preview_match_batch(change: BatchMatchChange, session: SessionDep):
        proposals = pending_match_proposal_index(session)
        selected = (
            proposals
            if change.entire_queue
            else [row for row in proposals if row["id"] in set(change.ids)]
        )
        items = []
        for component in connected_proposal_components(selected):
            reasons = proposal_component_blockers(session, component["series_ids"])
            items.extend(
                {"id": row["id"], "blocked_reasons": reasons}
                for row in component["proposals"]
            )
        return {
            "selected": len(items),
            "eligible": sum(not row["blocked_reasons"] for row in items),
            "blocked": sum(bool(row["blocked_reasons"]) for row in items),
            "items": items,
        }

    @router.post("/match-batch")
    def decide_match_batch(change: BatchMatchChange, session: SessionDep):
        if (not change.ids and not change.entire_queue) or change.decision not in {
            "accepted",
            "rejected",
        }:
            raise HTTPException(422, "select matches and a valid decision")
        if change.decision == "accepted" and change.confirmation != "MERGE":
            raise HTTPException(422, "MERGE confirmation required")
        from manga_manager.web.app import merge_canonical_series

        with session.begin():
            proposals = pending_match_proposal_index(session)
            selected = (
                proposals
                if change.entire_queue
                else [row for row in proposals if row["id"] in set(change.ids)]
            )
            applied: list[int] = []
            blocked: list[dict[str, Any]] = []
            components = connected_proposal_components(selected)
            for component in components:
                reasons = (
                    proposal_component_blockers(session, component["series_ids"])
                    if change.decision == "accepted"
                    else []
                )
                if reasons:
                    blocked.extend(
                        {"id": proposal["id"], "reasons": reasons}
                        for proposal in component["proposals"]
                    )
                    continue
                try:
                    with session.begin_nested():
                        decision_ids = {
                            decision_id
                            for proposal in component["proposals"]
                            for decision_id in proposal["decision_ids"]
                        }
                        rows = session.scalars(
                            select(CatalogMatchDecision).where(
                                CatalogMatchDecision.id.in_(decision_ids)
                            )
                        ).all()
                        for row in rows:
                            record_training_label(
                                session,
                                left_source_series_id=row.left_source_series_id,
                                right_source_series_id=row.right_source_series_id,
                                label=int(change.decision == "accepted"),
                                origin="batch_review",
                                decision=row,
                            )
                            row.decision = change.decision
                            row.decided_by = "operator"
                            row.decided_at = utcnow()
                        if change.decision == "accepted":
                            merge_canonical_series(session, component["series_ids"])
                except HTTPException as error:
                    blocked.extend(
                        {"id": proposal["id"], "reasons": [str(error.detail)]}
                        for proposal in component["proposals"]
                    )
                    continue
                applied.extend(proposal["id"] for proposal in component["proposals"])
        return {"ids": applied, "blocked": blocked, "decision": change.decision}

    @router.get("/jobs")
    def jobs(
        session: SessionDep,
        state: list[str] = Query(default=[]),
        cursor: int = Query(default=0, ge=0),
        limit: int = Query(default=30, ge=1, le=100),
    ):
        query = select(WorkJob)
        if state:
            query = query.where(job_state_predicate(state))
        if cursor:
            query = query.where(WorkJob.id < cursor)
        rows = session.scalars(query.order_by(WorkJob.id.desc()).limit(limit)).all()
        return {
            "items": serialize_jobs(session, rows),
            "next_cursor": rows[-1].id if len(rows) == limit else None,
        }

    @router.get("/workload-cycle")
    def workload_cycle(session: SessionDep):
        cycle = session.scalar(
            select(WorkloadCycle)
            .order_by((WorkloadCycle.status == "active").desc(), WorkloadCycle.id.desc())
            .limit(1)
        )
        if cycle is None:
            return {
                "id": None,
                "status": "settled",
                "total": 0,
                "successful": 0,
                "failed": 0,
                "cancelled": 0,
                "superseded": 0,
                "remaining": 0,
                "added": 0,
            }
        return workload_cycle_summary(session, cycle)

    @router.get("/job-groups")
    def job_groups(
        session: SessionDep,
        state: list[str] = Query(default=[]),
        cursor: str = Query(default="", max_length=200),
        limit: int = Query(default=20, ge=1, le=50),
    ):
        key = func.coalesce(
            func.nullif(WorkJob.group_key, ""),
            WorkJob.kind + ":" + func.cast(WorkJob.id, String),
        ).label("group_key")
        running = func.sum(case((WorkJob.status == "leased", 1), else_=0)).label("running")
        succeeded = func.sum(case((WorkJob.status == "succeeded", 1), else_=0)).label("succeeded")
        failed = func.sum(case((WorkJob.status == "failed", 1), else_=0)).label("failed")
        cancelled = func.sum(case((WorkJob.status == "cancelled", 1), else_=0)).label("cancelled")
        queued = func.sum(case((WorkJob.status == "queued", 1), else_=0)).label("queued")
        retrying = func.sum(case((WorkJob.status == "retry_wait", 1), else_=0)).label("retry_wait")
        priority = func.min(WorkJob.priority)
        available_at = func.min(WorkJob.available_at)
        last_id = func.max(WorkJob.id)
        query = select(
            key,
            last_id.label("last_id"),
            func.count().label("task_count"),
            running,
            succeeded,
            failed,
            cancelled,
            queued,
            retrying,
            priority.label("priority"),
            available_at.label("available_at"),
        )
        if state:
            query = query.where(job_state_predicate(state))
        query = query.group_by(key)
        if cursor:
            cursor_running, cursor_priority, cursor_available, cursor_id = decode_job_cursor(cursor)
            query = query.having(
                or_(
                    running < cursor_running,
                    and_(running == cursor_running, priority > cursor_priority),
                    and_(
                        running == cursor_running,
                        priority == cursor_priority,
                        available_at > cursor_available,
                    ),
                    and_(
                        running == cursor_running,
                        priority == cursor_priority,
                        available_at == cursor_available,
                        last_id < cursor_id,
                    ),
                )
            )
        rows = session.execute(
            query.order_by(running.desc(), priority, available_at, last_id.desc()).limit(limit)
        ).all()
        group_keys = [row.group_key for row in rows]
        group_expression = func.coalesce(
            func.nullif(WorkJob.group_key, ""),
            WorkJob.kind + ":" + func.cast(WorkJob.id, String),
        )
        child_query = select(WorkJob).where(group_expression.in_(group_keys or ["-"]))
        if state:
            child_query = child_query.where(job_state_predicate(state))
        representatives: dict[str, WorkJob] = {}
        for child in session.scalars(
            child_query.order_by(
                (WorkJob.status == "leased").desc(),
                WorkJob.priority,
                WorkJob.available_at,
                WorkJob.id,
            )
        ):
            child_key = child.group_key or f"{child.kind}:{child.id}"
            representatives.setdefault(child_key, child)
        serialized_representatives = {
            item["id"]: item for item in serialize_jobs(session, list(representatives.values()))
        }
        all_status_counts: dict[str, dict[str, int]] = defaultdict(dict)
        for child_key, status, count in session.execute(
            select(group_expression, WorkJob.status, func.count())
            .where(group_expression.in_(group_keys or ["-"]))
            .group_by(group_expression, WorkJob.status)
        ):
            all_status_counts[str(child_key)][str(status)] = int(count)
        active_view = not state or bool(set(state).intersection(ACTIVE_JOB_STATES))
        plan_ids: set[int] = set()
        if active_view:
            for group_key in group_keys:
                if ":download:" not in group_key and not group_key.startswith("download:"):
                    continue
                try:
                    plan_ids.add(int(group_key.rsplit(":", 1)[1]))
                except ValueError:
                    continue
        plans = {
            plan.series_id: plan
            for plan in session.scalars(
                select(SeriesDownloadPlan).where(SeriesDownloadPlan.series_id.in_(plan_ids or {-1}))
            )
        }
        items = []
        for row in rows:
            representative = representatives.get(row.group_key)
            if representative is None:
                continue
            status_counts = {
                key: int(value or 0)
                for key, value in {
                    "leased": row.running,
                    "succeeded": row.succeeded,
                    "failed": row.failed,
                    "cancelled": row.cancelled,
                    "queued": row.queued,
                    "retry_wait": row.retry_wait,
                }.items()
                if value
            }
            serialized = serialized_representatives[representative.id]
            is_pull_group = representative.kind in {"source_pull", "source_refresh"}
            group_status_counts = all_status_counts[row.group_key]
            total = sum(group_status_counts.values())
            done = sum(
                group_status_counts.get(value, 0) for value in ("succeeded", "failed", "cancelled")
            )
            successful = group_status_counts.get("succeeded", 0)
            failed_count = group_status_counts.get("failed", 0)
            cancelled_count = group_status_counts.get("cancelled", 0)
            if active_view and (
                ":download:" in row.group_key or row.group_key.startswith("download:")
            ):
                try:
                    plan = plans.get(int(row.group_key.rsplit(":", 1)[1]))
                except ValueError:
                    plan = None
                if plan is not None:
                    total = plan.total_chapters
                    done = plan.satisfied_chapters + plan.attention_chapters
                    successful = plan.satisfied_chapters
                    failed_count = plan.attention_chapters
                    cancelled_count = 0
            items.append(
                {
                    "key": row.group_key,
                    "kind": "source_pull" if is_pull_group else representative.kind,
                    "source": representative.source,
                    "title": (
                        f"{representative.source.replace('_', ' ').title()} catalog pull"
                        if is_pull_group
                        else "Normalize library metadata"
                        if representative.kind == "library_repair"
                        else "Synchronize downloaded manga with Kavita"
                        if representative.kind == "kavita_sync"
                        else "Build cover matching evidence"
                        if representative.kind == "cover_backfill"
                        else "Maintenance and health checks"
                        if representative.kind == "maintenance"
                        else serialized["context"].get("title") or serialized["description"]
                    ),
                    "cover_url": serialized["context"].get("cover_url", ""),
                    "task_count": row.task_count,
                    "status_counts": status_counts,
                    "progress": {
                        "current": done,
                        "total": total,
                        "percent": round(done * 100 / total) if total else None,
                        "successful": successful,
                        "failed": failed_count,
                        "cancelled": cancelled_count,
                    },
                    "representative": serialized,
                    "single": row.task_count == 1,
                }
            )
        next_cursor = (
            encode_job_cursor(
                int(rows[-1].running or 0),
                rows[-1].priority,
                rows[-1].available_at,
                rows[-1].last_id,
            )
            if len(rows) == limit
            else None
        )
        return {"items": items, "next_cursor": next_cursor}

    @router.get("/job-groups/{group_key:path}/children")
    def job_group_children(
        group_key: str,
        session: SessionDep,
        state: list[str] = Query(default=[]),
        cursor: str = Query(default="", max_length=200),
        limit: int = Query(default=30, ge=1, le=100),
    ):
        query = select(WorkJob).where(WorkJob.group_key == group_key)
        if state:
            query = query.where(job_state_predicate(state))
        if cursor:
            running, priority, available, job_id = decode_job_cursor(cursor)
            is_running = case((WorkJob.status == "leased", 1), else_=0)
            query = query.where(
                or_(
                    is_running < running,
                    and_(is_running == running, WorkJob.priority > priority),
                    and_(
                        is_running == running,
                        WorkJob.priority == priority,
                        WorkJob.available_at > available,
                    ),
                    and_(
                        is_running == running,
                        WorkJob.priority == priority,
                        WorkJob.available_at == available,
                        WorkJob.id > job_id,
                    ),
                )
            )
        rows = session.scalars(
            query.order_by(
                (WorkJob.status == "leased").desc(),
                WorkJob.priority,
                WorkJob.available_at,
                WorkJob.id,
            ).limit(limit)
        ).all()
        next_cursor = (
            encode_job_cursor(
                int(rows[-1].status == "leased"),
                rows[-1].priority,
                rows[-1].available_at,
                rows[-1].id,
            )
            if len(rows) == limit
            else None
        )
        return {"items": serialize_jobs(session, rows), "next_cursor": next_cursor}

    @router.get("/activity")
    def activity(
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
                    "message": event.message or jobs_by_id[job.id]["error_message"],
                    "details": event.details or {},
                    "created_at": event.created_at.isoformat(),
                }
                for event, job in rows
            ],
            "next_cursor": rows[-1][0].id if len(rows) == limit else None,
        }

    @router.get("/operations")
    def operations(session: SessionDep):
        counts = dict(
            session.execute(
                select(WorkJob.status, func.count())
                .where(WorkJob.status.in_(ACTIVE_JOB_STATES))
                .group_by(WorkJob.status)
            ).all()
        )
        counts["failed"] = int(
            session.scalar(
                select(func.count()).select_from(WorkJob).where(job_state_predicate(["failed"]))
            )
            or 0
        )
        active_group_key = func.coalesce(
            func.nullif(WorkJob.group_key, ""),
            WorkJob.kind + ":" + func.cast(WorkJob.id, String),
        )
        active_groups = int(
            session.scalar(
                select(func.count(func.distinct(active_group_key))).where(
                    WorkJob.status.in_(ACTIVE_JOB_STATES)
                )
            )
            or 0
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
        # pg_database_size walks relation files and can stall this frequently refreshed endpoint
        # for seconds while a mechanical disk is flushing a large import.  Exact database size
        # remains available from `database-audit`; the interactive page does not consume it.
        database_bytes = 0
        policies = session.scalars(select(ProviderPolicy).order_by(ProviderPolicy.source)).all()
        endpoint_states = session.scalars(
            select(ProviderEndpointState).order_by(
                ProviderEndpointState.source, ProviderEndpointState.traffic_class
            )
        ).all()
        kavita = configured_kavita_client(
            local_library_root=settings.storage_root / "kavita-library"
        )
        return {
            "job_counts": counts,
            "active_groups": active_groups,
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
                **storage_health(session),
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
                    "frontier_metrics": (row.cursor_json or {}).get("last_pull", {}),
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
                    "request_interval_seconds": row.request_interval_seconds,
                    "cooldown_seconds": row.cooldown_seconds,
                    "poll_interval_seconds": int(
                        (row.metadata_json or {}).get("adaptive_poll_seconds") or 0
                    ),
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
            "provider_endpoints": [
                {
                    "source": row.source,
                    "traffic_class": row.traffic_class,
                    "request_interval_seconds": row.request_interval_seconds,
                    "failures": row.consecutive_failures,
                    "cooldown_until": row.cooldown_until.isoformat()
                    if row.cooldown_until
                    else None,
                    "last_error": row.last_error,
                }
                for row in endpoint_states
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
                    select(ProviderBenchmarkRun).order_by(ProviderBenchmarkRun.id.desc()).limit(10)
                )
            ],
        }

    @router.post("/sources/{source}/pull")
    def pull_source(source: str, session: SessionDep):
        if source not in provider_names():
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
    def probe(session: SessionDep):
        with session.begin():
            job, _created = JobQueue().enqueue(
                session,
                kind=JobKind.MAINTENANCE,
                dedupe_key="operations-probe",
                payload=MaintenancePayload(action="stage_probe"),
                priority=1,
                pool="health",
            )
        return {"job": serialize_jobs(session, [job])[0]}

    @router.post("/operations/kavita-sync")
    def sync_pending_kavita(session: SessionDep, limit: int = Query(100, ge=1, le=500)):
        with session.begin():
            pending, created = KavitaSyncPlanner().enqueue_pending(
                session,
                limit=limit,
                reading_refresh_after=timedelta(seconds=0),
                batch=True,
            )
        return {"pending": pending, "created": created}

    @router.post("/jobs/{job_id}/retry")
    def retry_job(job_id: int, session: SessionDep):
        with session.begin():
            job = session.get(WorkJob, job_id)
            if job is None or job.status not in {"failed", "cancelled"}:
                raise HTTPException(409, "job cannot be retried")
            job, _created = JobQueue().enqueue(
                session,
                kind=JobKind(job.kind),
                dedupe_key=job.dedupe_key,
                payload=job.payload,
                priority=job.priority,
                max_attempts=max(job.max_attempts, 3),
                source=job.source,
                series_key=job.series_key,
                pool=job.pool,
                workflow_key=job.workflow_key,
                group_key=job.group_key,
            )
        return {"job": serialize_jobs(session, [job])[0]}

    @router.post("/jobs/failures/dismiss")
    def dismiss_failed_jobs(session: SessionDep):
        with session.begin():
            jobs = session.scalars(
                select(WorkJob)
                .where(job_state_predicate(["failed"]))
                .order_by(WorkJob.id)
                .with_for_update(skip_locked=True)
            ).all()
            changed_at = utcnow()
            for job in jobs:
                job.status = "cancelled"
                job.updated_at = changed_at
                job.completed_at = job.completed_at or changed_at
                session.add(
                    JobEvent(
                        job_id=job.id,
                        event_type="cancelled",
                        status="cancelled",
                        message="failure dismissed from the live job center",
                        created_at=changed_at,
                    )
                )
        return {"dismissed": len(jobs)}

    @router.post("/jobs/{job_id}/dismiss")
    def dismiss_failed_job(job_id: int, session: SessionDep):
        with session.begin():
            job = session.scalar(
                select(WorkJob)
                .where(WorkJob.id == job_id, WorkJob.status == "failed")
                .with_for_update()
            )
            if job is None:
                raise HTTPException(409, "failed job cannot be dismissed")
            changed_at = utcnow()
            job.status = "cancelled"
            job.updated_at = changed_at
            job.completed_at = job.completed_at or changed_at
            session.add(
                JobEvent(
                    job_id=job.id,
                    event_type="cancelled",
                    status="cancelled",
                    message="failure dismissed from the live job center",
                    created_at=changed_at,
                )
            )
        return {"job": serialize_jobs(session, [job])[0]}

    @router.get("/events")
    async def events(request: Request, after: int = Query(default=0, ge=0)):
        def load_event_batch(cursor: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
            with factory_provider()() as session:
                rows = session.execute(
                    select(JobEvent, WorkJob.kind)
                    .outerjoin(WorkJob, WorkJob.id == JobEvent.job_id)
                    .where(JobEvent.id > cursor)
                    .order_by(JobEvent.id)
                    .limit(100)
                ).all()
                payloads = [
                    {
                        "event_id": event.id,
                        "job_id": event.job_id,
                        "kind": kind or "",
                        "state": event.status,
                        "type": event.event_type,
                        "message": event.message,
                        "timestamp": event.created_at.isoformat(),
                    }
                    for event, kind in rows
                ]
                counts = (
                    {
                        str(status): int(count)
                        for status, count in session.execute(
                            select(WorkJob.status, func.count()).group_by(WorkJob.status)
                        )
                    }
                    if rows
                    else {}
                )
                return payloads, counts

        async def stream():
            cursor = after
            while not await request.is_disconnected():
                rows, counts = load_event_batch(cursor)
                for payload in rows:
                    cursor = int(payload["event_id"])
                    yield f"id: {cursor}\nevent: job\ndata: {json.dumps(payload)}\n\n"
                if rows:
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
    cover_assets: dict[int, str] = {}
    if ids:
        priority = {source: index for index, source in enumerate(SOURCE_PRIORITY)}
        candidates = session.execute(
            select(
                CatalogSourceSeries.series_id,
                CatalogSourceSeries.source,
                CatalogCoverAsset.content_checksum,
            )
            .join(
                CatalogCoverAsset,
                CatalogCoverAsset.source_series_id == CatalogSourceSeries.id,
            )
            .where(CatalogSourceSeries.series_id.in_(ids))
        ).all()
        for series_id, source, checksum in sorted(
            candidates,
            key=lambda value: (value[0], priority.get(value[1], len(priority))),
        ):
            cover_assets.setdefault(
                series_id,
                f"/api/v2/covers/{series_id}/{checksum}.webp",
            )
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
            "cover_url": cover_assets.get(row.id, row.cover_url),
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
    queued_ids = [row.id for row in rows if row.status in {"queued", "retry_wait"}]
    queue_positions: dict[int, int] = {}
    if queued_ids:
        ranked = (
            select(
                WorkJob.id.label("job_id"),
                func.row_number()
                .over(
                    partition_by=WorkJob.pool,
                    order_by=(WorkJob.priority, WorkJob.available_at, WorkJob.id),
                )
                .label("queue_position"),
            )
            .where(WorkJob.status.in_(("queued", "retry_wait")))
            .subquery()
        )
        queue_positions = {
            int(job_id): int(position)
            for job_id, position in session.execute(
                select(ranked.c.job_id, ranked.c.queue_position).where(
                    ranked.c.job_id.in_(queued_ids)
                )
            )
        }
    release_ids = [
        int(row.payload.get("chapter_release_id"))
        for row in rows
        if row.kind == "chapter_download" and row.payload.get("chapter_release_id")
    ]
    chapter_context: dict[int, dict[str, Any]] = {}
    series_ids = {
        int(row.payload["series_id"])
        for row in rows
        if row.kind in {"library_repair", "kavita_sync"} and row.payload.get("series_id")
    }
    for row in rows:
        if row.kind == "kavita_sync":
            series_ids.update(int(value) for value in row.payload.get("series_ids", []) if value)
    series_context = {
        series.id: {
            "series_id": series.id,
            "title": series.title,
            "cover_url": series.cover_url,
        }
        for series in session.scalars(
            select(CatalogSeries).where(CatalogSeries.id.in_(series_ids or {-1}))
        )
    }
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
        queue_position = queue_positions.get(row.id)
        if row.kind == "chapter_download":
            context = chapter_context.get(int(row.payload.get("chapter_release_id", 0)), {})
            description = f"Download {context.get('title', 'chapter')} · Chapter {context.get('chapter', '?')}"
        elif row.kind == "source_pull":
            description = (
                f"Pull latest releases from {row.source or row.payload.get('source', 'source')}"
            )
        elif row.kind == "source_refresh":
            description = f"Refresh {row.payload.get('title', 'source series')}"
        elif row.kind == "kavita_sync":
            series_id = int(row.payload.get("series_id", 0))
            context = series_context.get(series_id, {})
            batch_ids = [int(value) for value in row.payload.get("series_ids", []) if value]
            if batch_ids:
                context = series_context.get(batch_ids[0], {})
                description = f"Synchronize {len(batch_ids)} manga with Kavita"
            else:
                description = (
                    f"Synchronize {context['title']} with Kavita"
                    if context
                    else "Synchronize downloaded manga with Kavita"
                )
        elif row.kind == "library_repair":
            series_id = int(row.payload.get("series_id", 0))
            context = series_context.get(series_id, {})
            description = (
                f"Normalize library metadata · {context['title']}"
                if context
                else f"Normalize library metadata · series #{series_id or '?'}"
            )
        elif row.kind == "cover_backfill":
            description = (
                f"Build cover evidence · source series #{row.payload.get('source_series_id', '?')}"
            )
        elif row.kind == "maintenance":
            action = str(row.payload.get("action", ""))
            if action.startswith("provider_probe_"):
                source = action.removeprefix("provider_probe_").replace("_", " ").title()
                description = f"Probe {source} provider recovery"
            elif action == "rescore_matches":
                description = "Refresh manga matching evidence"
            else:
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
                "cycle_id": row.cycle_id,
                "workflow_key": row.workflow_key,
                "group_key": row.group_key,
                "status": row.status,
                "queue_position": queue_position,
                "attempt": row.attempts,
                "max_attempts": row.max_attempts,
                "error_code": row.error_code,
                "error_message": operational_error_message(row.error_code, row.error_message),
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


def operational_error_message(code: str, message: str) -> str:
    if message.strip():
        return message
    defaults = {
        "source_network_error": "Provider network request failed; retry is scheduled.",
        "rate_limited": "Provider rate limit reached; waiting for the shared cooldown.",
        "kavita_unconfigured": "Kavita is not configured for this worker.",
    }
    return defaults.get(code, code.replace("_", " ").capitalize() if code else "")


def human_evidence(raw: dict[str, Any] | None) -> list[dict[str, str]]:
    data = raw or {}
    labels: list[dict[str, str]] = []
    if data.get("shared_external_id") or data.get("external_id"):
        labels.append({"tone": "positive", "label": "Shared external ID"})
    cover_state = str(data.get("cover_evidence_state") or "")
    if data.get("cover_match") is True or cover_state == "match":
        raw_inliers = data.get("cover_inliers")
        inliers = int(raw_inliers) if isinstance(raw_inliers, (int, float)) else 0
        label = f"Visual cover match · {inliers} aligned features" if inliers else "Cover match"
        labels.append({"tone": "positive", "label": label})
    elif cover_state == "likely":
        labels.append({"tone": "positive", "label": "Covers are likely similar"})
    elif cover_state == "different":
        labels.append({"tone": "warning", "label": "Cover artwork differs"})
    elif data.get("cover_compared"):
        labels.append({"tone": "warning", "label": "Cover evidence inconclusive"})
    elif cover_state == "invalid" or data.get("cover_signature_invalid"):
        labels.append({"tone": "warning", "label": "Cover signature needs refresh"})
    else:
        labels.append({"tone": "neutral", "label": "Cover comparison pending"})
    if data.get("title_or_alias") or data.get("title_match"):
        labels.append({"tone": "positive", "label": "Strong title or alias match"})
    if data.get("chapter_fingerprint"):
        labels.append({"tone": "positive", "label": "Chapter fingerprint match"})
    if data.get("latest_chapter_compared"):
        delta = str(data.get("latest_chapter_delta") or "0")
        if data.get("latest_chapter_match"):
            labels.append({"tone": "positive", "label": f"Latest chapters align · Δ{delta}"})
        else:
            labels.append({"tone": "warning", "label": f"Latest chapters differ · Δ{delta}"})
    if "manual_review" in str(data.get("policy", "")):
        labels.append({"tone": "neutral", "label": "Manual review required"})
    return labels or [{"tone": "neutral", "label": "Similarity candidate"}]


def encode_cursor(timestamp: datetime | None, row_id: int) -> str:
    return f"{timestamp.isoformat() if timestamp else 'none'}|{row_id}"


def encode_job_cursor(running: int, priority: int, available_at: datetime, row_id: int) -> str:
    return f"{running}|{priority}|{available_at.isoformat()}|{row_id}"


def decode_job_cursor(value: str) -> tuple[int, int, datetime, int]:
    try:
        running, priority, available_at, row_id = value.split("|", 3)
        return int(running), int(priority), datetime.fromisoformat(available_at), int(row_id)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, "invalid job cursor") from exc


def decode_cursor(value: str) -> tuple[datetime | None, int]:
    try:
        raw_time, raw_id = value.rsplit("|", 1)
        return (None if raw_time == "none" else datetime.fromisoformat(raw_time), int(raw_id))
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, "invalid cursor") from exc
