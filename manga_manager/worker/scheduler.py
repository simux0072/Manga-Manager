from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import Engine, select, text

from app.settings import settings as adapter_settings
from app.kavita import configured_kavita_client
from manga_manager.application.download_plans import DownloadPlanCoordinator
from manga_manager.application.cover_backfill import CoverBackfillPlanner
from manga_manager.application.kavita_sync import KavitaSyncPlanner
from manga_manager.application.library_repair import LibraryRepairPlanner
from manga_manager.application.job_retention import JobRetention
from manga_manager.domain.jobs import JobKind, MaintenancePayload, SourcePullPayload
from manga_manager.infrastructure.db_models import CatalogSourceState, ProviderPolicy
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.infrastructure.provider_telemetry import ProviderTelemetry
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
            self.telemetry.ensure_policy(
                source,
                job_limit=jobs,
                page_limit=pages,
                cooldown_seconds=int(self.settings.source_cooldown(source).total_seconds()),
                request_interval_seconds=interval,
            )
            self.telemetry.start_due_exploration(source, ceiling=ceiling, now=current)
        self.telemetry.finalize_due(now=current)
        self.telemetry.cleanup_samples(now=current)
        with self.session_factory() as policy_session:
            for policy in policy_session.scalars(select(ProviderPolicy)).all():
                field = f"{policy.source}_page_concurrency"
                if hasattr(adapter_settings, field):
                    setattr(adapter_settings, field, policy.learned_page_limit)
        with self.session_factory() as session, session.begin():
            self.storage_capacity.refresh(session)
            DownloadPlanCoordinator(self.queue).bootstrap(session)
            repair_planner = LibraryRepairPlanner(self.queue)
            canonicalized, cancelled = repair_planner.reconcile_active_jobs(session)
            if canonicalized or cancelled:
                logger.info(
                    "coalesced library repair backlog: canonicalized=%s cancelled=%s",
                    canonicalized,
                    cancelled,
                )
            repair_planner.enqueue_pending(session, limit=25)
            count += CoverBackfillPlanner(self.queue).enqueue_pending(session, limit=10)
            JobRetention().prune(session, now=current, batch=250)
            kavita = configured_kavita_client(
                local_library_root=self.settings.storage_root / "kavita-library"
            )
            if kavita.configured:
                KavitaSyncPlanner(self.queue).enqueue_pending(
                    session,
                    limit=25,
                    reading_refresh_after=timedelta(minutes=10),
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

    async def _wait(self, stop: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=self.settings.scheduler_check_seconds)
        except TimeoutError:
            return


def aware_datetime(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=timezone.utc)
