from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from manga_manager.domain.jobs import ChapterDownloadPayload, JobKind
from manga_manager.domain.providers import SOURCE_PRIORITY as PROVIDER_PRIORITY
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogSeries,
    ChapterArtifact,
    ChapterReleaseAttempt,
    ChapterDownloadIntent,
    SeriesDownloadPlan,
    WorkJob,
)
from manga_manager.infrastructure.job_queue import JobQueue


TRACKED_STATES = {"interested", "reading", "caught_up", "paused"}
TERMINAL_JOB_STATES = {"succeeded", "failed", "cancelled"}
SOURCE_PRIORITY = {
    source: len(PROVIDER_PRIORITY) - index for index, source in enumerate(PROVIDER_PRIORITY)
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


class DownloadPlanCoordinator:
    def __init__(self, queue: JobQueue | None = None, *, rolling_window: int = 20) -> None:
        self.queue = queue or JobQueue()
        self.rolling_window = rolling_window

    def track(self, session: Session, series_id: int) -> SeriesDownloadPlan:
        plan = session.get(SeriesDownloadPlan, series_id)
        if plan is None:
            plan = SeriesDownloadPlan(series_id=series_id)
            session.add(plan)
            session.flush()
        plan.status = "active"
        if plan.phase in {"complete", "cancelled"}:
            plan.phase = "priority"
        self._sync_chapters(session, plan)
        self.reconcile(session, series_id)
        return plan

    def untrack(self, session: Session, series_id: int) -> None:
        plan = session.get(SeriesDownloadPlan, series_id)
        if plan is not None:
            plan.status = "cancelled"
            plan.phase = "cancelled"
            plan.updated_at = utcnow()
        intents = session.scalars(
            select(ChapterDownloadIntent).where(
                ChapterDownloadIntent.series_id == series_id,
                ChapterDownloadIntent.state.in_(["blocked", "pending", "queued"]),
            )
        ).all()
        for intent in intents:
            if intent.job_id:
                job = session.get(WorkJob, intent.job_id)
                if job is not None and job.status in {"queued", "retry_wait"}:
                    self.queue.cancel(session, job_id=job.id, reason="series was untracked")
            intent.state = "cancelled"
            intent.updated_at = utcnow()

    def bootstrap(self, session: Session) -> int:
        series_ids = session.scalars(
            select(CatalogSeries.id).where(CatalogSeries.status.in_(TRACKED_STATES))
        ).all()
        for series_id in series_ids:
            self.track(session, series_id)
        self.enqueue_preferred_upgrades(session, series_ids)
        return len(series_ids)

    def fallback_release(
        self,
        session: Session,
        chapter_id: int,
        failed_release_id: int,
        *,
        excluded_sources: set[str] | None = None,
    ) -> CatalogChapterRelease | None:
        now = utcnow()
        rows = session.scalars(
            select(CatalogChapterRelease).where(
                CatalogChapterRelease.chapter_id == chapter_id,
                CatalogChapterRelease.id != failed_release_id,
                CatalogChapterRelease.source.not_in(excluded_sources or set()),
                (CatalogChapterRelease.downloadable_after.is_(None))
                | (CatalogChapterRelease.downloadable_after <= now),
            )
        ).all()
        return max(
            rows,
            key=lambda row: (SOURCE_PRIORITY.get(row.source, 0), row.id),
            default=None,
        )

    def reroute(
        self,
        session: Session,
        *,
        job: WorkJob,
        failed_release: CatalogChapterRelease,
        alternative: CatalogChapterRelease,
        error_code: str,
        error_message: str,
        retry_after: datetime | None,
    ) -> WorkJob:
        failed_release.downloadable_after = retry_after
        session.add(
            ChapterReleaseAttempt(
                chapter_id=failed_release.chapter_id,
                chapter_release_id=failed_release.id,
                job_id=job.id,
                source=failed_release.source,
                outcome="failed",
                error_code=error_code,
                error_message=error_message[:4000],
                retry_after=retry_after,
                details_json={"fallback_release_id": alternative.id},
            )
        )
        current_payload = ChapterDownloadPayload.model_validate(job.payload)
        attempted = tuple(dict.fromkeys((*current_payload.attempted_sources, failed_release.source)))
        replacement = ChapterDownloadPayload(
            chapter_release_id=alternative.id,
            attempted_sources=attempted,
            preferred_only=current_payload.preferred_only,
        )
        moved = self.queue.reroute(
            session,
            job_id=job.id,
            owner=job.lease_owner,
            payload=replacement,
            source=alternative.source,
            available_at=utcnow(),
            message=f"rerouted to {alternative.source}: {error_code}",
            details={"failed_source": failed_release.source, "attempted_sources": attempted},
        )
        if not moved:
            raise RuntimeError("chapter lease was lost while rerouting")
        intent = session.scalar(
            select(ChapterDownloadIntent).where(
                ChapterDownloadIntent.chapter_id == failed_release.chapter_id
            )
        )
        if intent is not None:
            intent.job_id = job.id
            intent.state = "queued"
            intent.updated_at = utcnow()
        session.add(
            ChapterReleaseAttempt(
                chapter_id=alternative.chapter_id,
                chapter_release_id=alternative.id,
                job_id=job.id,
                source=alternative.source,
                outcome="fallback_queued",
                details_json={
                    "failed_release_id": failed_release.id,
                    "failed_source": failed_release.source,
                },
            )
        )
        return job

    def bypass_cooling_source(self, session: Session, *, source: str, retry_after: datetime) -> int:
        """Move waiting chapters to another provider while one provider is cooling down."""
        jobs = session.scalars(
            select(WorkJob)
            .where(
                WorkJob.kind == JobKind.CHAPTER_DOWNLOAD.value,
                WorkJob.source == source,
                WorkJob.status.in_(["queued", "retry_wait"]),
            )
            .with_for_update(skip_locked=True)
        ).all()
        rerouted = 0
        for job in jobs:
            release_id = int((job.payload or {}).get("chapter_release_id") or 0)
            failed_release = session.get(CatalogChapterRelease, release_id)
            if failed_release is None:
                continue
            payload = ChapterDownloadPayload.model_validate(job.payload)
            if payload.preferred_only:
                job.status = "retry_wait"
                job.available_at = retry_after
                failed_release.downloadable_after = retry_after
                continue
            attempted = set(payload.attempted_sources) | {source}
            alternative = self.fallback_release(
                session,
                failed_release.chapter_id,
                failed_release.id,
                excluded_sources=attempted,
            )
            if alternative is None:
                failed_release.downloadable_after = retry_after
                job.status = "retry_wait"
                job.available_at = retry_after
                continue
            replacement = ChapterDownloadPayload(
                chapter_release_id=alternative.id,
                attempted_sources=tuple(sorted(attempted)),
            )
            self.queue.reroute_waiting(
                session,
                job_id=job.id,
                payload=replacement,
                source=alternative.source,
                available_at=utcnow(),
                message=f"bypassed while {source} is cooling down",
                details={"attempted_sources": sorted(attempted)},
            )
            intent = session.scalar(
                select(ChapterDownloadIntent).where(
                    ChapterDownloadIntent.chapter_id == failed_release.chapter_id
                )
            )
            if intent is not None:
                intent.job_id = job.id
                intent.state = "queued"
                intent.updated_at = utcnow()
            session.add(
                ChapterReleaseAttempt(
                    chapter_id=alternative.chapter_id,
                    chapter_release_id=alternative.id,
                    job_id=job.id,
                    source=alternative.source,
                    outcome="fallback_queued",
                    retry_after=retry_after,
                    details_json={
                        "bypassed_source": source,
                        "bypassed_release_id": failed_release.id,
                    },
                )
            )
            rerouted += 1
        return rerouted

    def enqueue_preferred_upgrades(
        self, session: Session, series_ids: list[int] | tuple[int, ...]
    ) -> int:
        if not series_ids:
            return 0
        artifacts = session.scalars(
            select(ChapterArtifact)
            .join(CatalogChapter, CatalogChapter.id == ChapterArtifact.chapter_id)
            .where(
                CatalogChapter.series_id.in_(series_ids),
                ChapterArtifact.state == "active",
                ChapterArtifact.provenance == "fallback",
            )
        ).all()
        created = 0
        for artifact in artifacts:
            release = self._best_release(session, artifact.chapter_id)
            if release is None or SOURCE_PRIORITY.get(release.source, 0) <= SOURCE_PRIORITY.get(
                artifact.source, 0
            ):
                continue
            chapter = session.get(CatalogChapter, artifact.chapter_id)
            if chapter is None:
                continue
            _job, was_created = self.queue.enqueue(
                session,
                kind=JobKind.CHAPTER_DOWNLOAD,
                dedupe_key=f"upgrade:{artifact.chapter_id}:{release.id}",
                payload=ChapterDownloadPayload(
                    chapter_release_id=release.id,
                    preferred_only=True,
                ),
                priority=250,
                source=release.source,
                series_key=str(chapter.series_id),
                max_attempts=3,
            )
            created += int(was_created)
        return created

    def reconcile(self, session: Session, series_id: int) -> SeriesDownloadPlan | None:
        plan = session.get(SeriesDownloadPlan, series_id)
        if plan is None or plan.status != "active":
            return plan
        self._sync_chapters(session, plan)
        intents = session.scalars(
            select(ChapterDownloadIntent)
            .where(ChapterDownloadIntent.series_id == series_id)
            .order_by(ChapterDownloadIntent.id)
        ).all()
        active_chapters = set(
            session.scalars(
                select(ChapterArtifact.chapter_id).where(
                    ChapterArtifact.chapter_id.in_([row.chapter_id for row in intents] or [-1]),
                    ChapterArtifact.state == "active",
                )
            ).all()
        )
        for intent in intents:
            if intent.chapter_id in active_chapters:
                intent.state = "satisfied"
                intent.updated_at = utcnow()
                continue
            if not intent.job_id:
                continue
            job = session.get(WorkJob, intent.job_id)
            if job is None:
                intent.job_id = None
                if intent.state == "queued":
                    intent.state = "pending" if intent.tier != "backfill" else "blocked"
                continue
            if job.status == "succeeded":
                intent.state = "satisfied"
            elif job.status in {"failed", "cancelled"}:
                intent.state = "attention"
            elif job.status in {"queued", "leased", "retry_wait"}:
                intent.state = "queued"

        priority = [row for row in intents if row.tier == "priority"]
        if plan.phase == "priority" and all(
            row.state in {"satisfied", "attention", "cancelled"} for row in priority
        ):
            plan.phase = "backfill"
            for intent in intents:
                if intent.tier == "backfill" and intent.state == "blocked":
                    intent.state = "pending"

        self._enqueue_ready(session, plan, intents)
        plan.total_chapters = len(intents)
        plan.satisfied_chapters = sum(row.state == "satisfied" for row in intents)
        plan.attention_chapters = sum(row.state == "attention" for row in intents)
        unfinished = [
            row for row in intents if row.state not in {"satisfied", "attention", "cancelled"}
        ]
        if not unfinished and intents:
            plan.status = "complete"
            plan.phase = "complete"
        plan.updated_at = utcnow()
        session.flush()
        return plan

    def _sync_chapters(self, session: Session, plan: SeriesDownloadPlan) -> None:
        chapters = session.scalars(
            select(CatalogChapter)
            .where(CatalogChapter.series_id == plan.series_id)
            .order_by(CatalogChapter.sort_number.asc().nullslast(), CatalogChapter.id)
        ).all()
        existing = {
            row.chapter_id: row
            for row in session.scalars(
                select(ChapterDownloadIntent).where(
                    ChapterDownloadIntent.series_id == plan.series_id
                )
            ).all()
        }
        priority_ids = {row.id for row in chapters[:2] + chapters[-2:]}
        for chapter in chapters:
            intent = existing.get(chapter.id)
            if intent is None:
                if plan.created_at and aware(chapter.created_at) > aware(plan.created_at):
                    tier, state = "current", "pending"
                elif chapter.id in priority_ids and plan.phase == "priority":
                    tier, state = "priority", "pending"
                else:
                    tier = "backfill"
                    state = "pending" if plan.phase == "backfill" else "blocked"
                intent = ChapterDownloadIntent(
                    series_id=plan.series_id,
                    chapter_id=chapter.id,
                    tier=tier,
                    state="satisfied" if self._has_active_artifact(session, chapter.id) else state,
                )
                session.add(intent)
        session.flush()

    def _enqueue_ready(
        self,
        session: Session,
        plan: SeriesDownloadPlan,
        intents: list[ChapterDownloadIntent],
    ) -> None:
        active_backfill = sum(row.tier == "backfill" and row.state == "queued" for row in intents)
        for intent in sorted(intents, key=self._intent_order):
            if intent.state != "pending":
                continue
            if intent.tier == "backfill" and active_backfill >= self.rolling_window:
                continue
            release = self._best_release(session, intent.chapter_id)
            if release is None:
                intent.state = "attention"
                continue
            priority = {"current": 5, "priority": 20, "backfill": 100}[intent.tier]
            job, _created = self.queue.enqueue(
                session,
                kind=JobKind.CHAPTER_DOWNLOAD,
                dedupe_key=f"chapter:{intent.chapter_id}",
                payload=ChapterDownloadPayload(chapter_release_id=release.id),
                priority=priority,
                source=release.source,
                series_key=str(plan.series_id),
                max_attempts=3,
            )
            intent.job_id = job.id
            intent.state = "queued"
            intent.updated_at = utcnow()
            if intent.tier == "backfill":
                active_backfill += 1

    @staticmethod
    def _intent_order(intent: ChapterDownloadIntent) -> tuple[int, int]:
        return ({"current": 0, "priority": 1, "backfill": 2}[intent.tier], intent.id)

    @staticmethod
    def _best_release(session: Session, chapter_id: int) -> CatalogChapterRelease | None:
        now = utcnow()
        rows = session.scalars(
            select(CatalogChapterRelease).where(
                CatalogChapterRelease.chapter_id == chapter_id,
                (CatalogChapterRelease.downloadable_after.is_(None))
                | (CatalogChapterRelease.downloadable_after <= now),
            )
        ).all()
        return max(rows, key=lambda row: (SOURCE_PRIORITY.get(row.source, 0), row.id), default=None)

    @staticmethod
    def _has_active_artifact(session: Session, chapter_id: int) -> bool:
        return bool(
            session.scalar(
                select(func.count())
                .select_from(ChapterArtifact)
                .where(ChapterArtifact.chapter_id == chapter_id, ChapterArtifact.state == "active")
            )
        )
