from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import Engine, text

from manga_manager.domain.jobs import JobKind, SourcePullPayload
from manga_manager.infrastructure.db_models import CatalogSourceState
from manga_manager.infrastructure.job_queue import JobQueue
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
        with self.session_factory() as session, session.begin():
            for source, interval in self.settings.source_intervals().items():
                state = session.get(CatalogSourceState, source)
                if state is not None and not state.manual_enabled:
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
