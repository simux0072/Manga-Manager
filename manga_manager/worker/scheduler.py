from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import Engine, select, text
from sqlalchemy.orm import Session, aliased

from app.settings import settings as adapter_settings
from app.kavita import configured_kavita_client
from manga_manager.application.download_plans import DownloadPlanCoordinator
from manga_manager.application.cover_backfill import CoverBackfillPlanner
from manga_manager.application.kavita_sync import KavitaSyncPlanner
from manga_manager.application.library_repair import LibraryRepairPlanner
from manga_manager.application.match_rescore import MatchRescorePlanner
from manga_manager.application.provider_health import contains_cloudflare_origin_error
from manga_manager.application.job_retention import JobRetention
from manga_manager.domain.jobs import JobKind, MaintenancePayload, SourcePullPayload
from manga_manager.infrastructure.db_models import CatalogSourceState, ProviderPolicy, WorkJob
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.provider_telemetry import (
    ProviderTelemetry,
    effective_poll_interval,
)
from manga_manager.infrastructure.storage_capacity import StorageCapacityCoordinator
from manga_manager.infrastructure.scheduler_leadership import (
    SchedulerLeadership,
    try_acquire_scheduler_leadership,
)
from manga_manager.settings import V2Settings
from manga_manager.worker.runtime import SessionFactory


logger = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SourcePollScheduler:
    def __init__(
        self,
        *,
        engine: Engine,
        session_factory: SessionFactory,
        settings: V2Settings,
        queue: JobQueue | None = None,
    ) -> None:
        self.engine = engine
        self.session_factory = session_factory
        self.settings = settings
        self.queue = queue or JobQueue()
        self.telemetry = ProviderTelemetry(session_factory)
        self.storage_capacity = StorageCapacityCoordinator(
            settings.storage_root, settings.min_free_bytes
        )
        self._last_run: dict[str, datetime] = {}

    def _due(self, name: str, now: datetime, interval: timedelta) -> bool:
        previous = self._last_run.get(name)
        if previous is not None and previous + interval > now:
            return False
        self._last_run[name] = now
        return True

    async def run(self, stop: asyncio.Event) -> None:
        leadership: SchedulerLeadership | None = None
        try:
            while not stop.is_set():
                if leadership is None:
                    leadership = try_acquire_scheduler_leadership(self.engine)
                    if leadership is None:
                        await self._wait(stop)
                        continue
                    logger.info("scheduler leadership acquired")
                try:
                    leadership.connection.execute(text("SELECT 1"))
                    self.enqueue_due()
                except Exception:
                    logger.exception("scheduler leadership connection failed")
                    leadership.release()
                    leadership = None
                await self._wait(stop)
        finally:
            if leadership is not None:
                leadership.release()

    def enqueue_due(self, *, now: datetime | None = None) -> int:
        current = now or utcnow()
        count = 0
        defaults = {
            "asura": (
                self.settings.asura_download_concurrency,
                1,
                2,
                max(2.0, adapter_settings.asura_request_interval_seconds),
            ),
            "mangafire": (
                self.settings.mangafire_download_concurrency,
                4,
                4,
                adapter_settings.mangafire_request_interval_seconds,
            ),
            "kingofshojo": (
                self.settings.kingofshojo_download_concurrency,
                4,
                4,
                adapter_settings.kingofshojo_request_interval_seconds,
            ),
        }
        for source, (jobs, pages, ceiling, interval) in defaults.items():
            poll_interval = self.settings.source_intervals().get(source)
            self.telemetry.ensure_policy(
                source,
                job_limit=jobs,
                page_limit=pages,
                cooldown_seconds=int(self.settings.source_cooldown(source).total_seconds()),
                request_interval_seconds=interval,
                poll_interval_seconds=(
                    int(poll_interval.total_seconds()) if poll_interval is not None else 0
                ),
            )
            self.telemetry.start_due_exploration(source, ceiling=ceiling, now=current)
        self.telemetry.finalize_due(now=current)
        if self._due("telemetry_cleanup", current, timedelta(hours=1)):
            self.telemetry.cleanup_samples(now=current)
        with self.session_factory() as policy_session:
            for policy in policy_session.scalars(select(ProviderPolicy)).all():
                field = f"{policy.source}_page_concurrency"
                if hasattr(adapter_settings, field):
                    setattr(adapter_settings, field, policy.learned_page_limit)
        with self.session_factory() as session, session.begin():
            self.storage_capacity.refresh(session)
            count += self._recover_misclassified_provider_outages(session, current)
            download_plans = DownloadPlanCoordinator(self.queue)
            if self._due("download_bootstrap", current, timedelta(hours=6)):
                download_plans.bootstrap(session)
            # Successful downloads advance their plans transactionally.  This bounded sweep is
            # the safety net for a worker crash between persistence and completion, and for a
            # priority job whose final retry became terminal without a later successful sibling.
            if self._due("download_reconcile", current, timedelta(minutes=1)):
                download_plans.reconcile_active(session, limit=50)
            repair_planner = LibraryRepairPlanner(self.queue)
            if self._due("library_repair", current, timedelta(minutes=10)):
                canonicalized, cancelled = repair_planner.reconcile_active_jobs(session)
                if canonicalized or cancelled:
                    logger.info(
                        "coalesced library repair backlog: canonicalized=%s cancelled=%s",
                        canonicalized,
                        cancelled,
                    )
                repair_planner.enqueue_pending(session, limit=25)
            if self._due("cover_backfill", current, timedelta(minutes=5)):
                count += CoverBackfillPlanner(self.queue).enqueue_pending(session, limit=10)
            if self._due("match_rescore", current, timedelta(minutes=5)):
                count += MatchRescorePlanner(self.queue).enqueue_pending(session, limit=10)
            if self._due("job_retention", current, timedelta(minutes=10)):
                JobRetention().prune(session, now=current, batch=250)
            if self._due("lease_cleanup", current, timedelta(minutes=10)):
                count += self.queue.fail_exhausted_leases(session, now=current)
                self.queue.cleanup_expired_permits(session, now=current)
            kavita = configured_kavita_client(
                local_library_root=self.settings.storage_root / "kavita-library"
            )
            if kavita.configured and self._due("kavita_refresh", current, timedelta(minutes=10)):
                KavitaSyncPlanner(self.queue).enqueue_pending(
                    session,
                    limit=25,
                    reading_refresh_after=timedelta(hours=6),
                    batch=True,
                )
            for policy in session.scalars(select(ProviderPolicy)).all():
                next_probe = (policy.metadata_json or {}).get("next_recovery_probe")
                if next_probe:
                    try:
                        due_at = aware_datetime(datetime.fromisoformat(str(next_probe)))
                    except ValueError:
                        due_at = None
                    if due_at is not None and due_at <= current:
                        _probe, created = self.queue.enqueue(
                            session,
                            kind=JobKind.MAINTENANCE,
                            dedupe_key=f"provider-probe:{policy.source}",
                            payload=MaintenancePayload(action=f"provider_probe_{policy.source}"),
                            priority=2,
                            source=policy.source,
                            pool=f"pull:{policy.source}",
                        )
                        count += int(created)
            for source, interval in self.settings.source_intervals().items():
                state = session.get(CatalogSourceState, source)
                interval = effective_poll_interval(session.get(ProviderPolicy, source), interval)
                if state is not None and not state.manual_enabled:
                    continue
                cooldown_until = aware_datetime(state.cooldown_until) if state is not None else None
                if cooldown_until is not None and cooldown_until > current:
                    continue
                last_poll = aware_datetime(state.last_poll_at) if state is not None else None
                if last_poll is not None and last_poll + interval > current:
                    continue
                _job, created = self.queue.enqueue(
                    session,
                    kind=JobKind.SOURCE_PULL,
                    dedupe_key=f"source:{source}",
                    payload=SourcePullPayload(source=source),
                    priority=50,
                    max_attempts=3,
                    available_at=current,
                )
                count += int(created)
        return count

    def _recover_misclassified_provider_outages(
        self,
        session: Session,
        current: datetime,
        *,
        limit: int = 25,
    ) -> int:
        """Requeue refreshes that older releases incorrectly made terminal on HTTP 52x."""
        later = aliased(WorkJob)
        rows = session.scalars(
            select(WorkJob)
            .where(
                WorkJob.kind == JobKind.SOURCE_REFRESH.value,
                WorkJob.status == "failed",
                WorkJob.error_code == "source_item_invalid",
                ~select(later.id)
                .where(
                    later.kind == WorkJob.kind,
                    later.dedupe_key == WorkJob.dedupe_key,
                    later.id > WorkJob.id,
                    later.status == "succeeded",
                )
                .exists(),
            )
            .order_by(WorkJob.id)
            .limit(limit)
        ).all()
        recovered = 0
        for failed in rows:
            if not contains_cloudflare_origin_error(failed.error_message):
                continue
            state = session.get(CatalogSourceState, failed.source)
            available_at = aware_datetime(state.cooldown_until) if state is not None else None
            replacement, created = self.queue.enqueue(
                session,
                kind=JobKind.SOURCE_REFRESH,
                dedupe_key=failed.dedupe_key,
                payload=failed.payload,
                priority=failed.priority,
                max_attempts=max(failed.max_attempts, 4),
                available_at=max(available_at or current, current),
                source=failed.source,
                series_key=failed.series_key,
                pool=failed.pool,
                workflow_key=failed.workflow_key,
                group_key=failed.group_key,
            )
            if not created:
                continue
            failed.error_code = "provider_outage_requeued"
            failed.error_message = (
                f"Automatically requeued as job #{replacement.id} after a temporary "
                "provider-origin outage."
            )
            failed.updated_at = current
            recovered += 1
        return recovered

    async def _wait(self, stop: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=self.settings.scheduler_check_seconds)
        except TimeoutError:
            return


def aware_datetime(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)
