from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from contextlib import suppress
from datetime import timedelta

from manga_manager.application.job_handlers import JobHandler
from manga_manager.domain.jobs import JobKind
from manga_manager.infrastructure.worker_registry import WorkerRegistry
from manga_manager.settings import V2Settings
from manga_manager.worker.runtime import JobWorker, SessionFactory, WorkerSettings


logger = logging.getLogger(__name__)


class WorkerService:
    def __init__(
        self,
        *,
        session_factory: SessionFactory,
        handlers: Mapping[JobKind, JobHandler],
        settings: V2Settings,
        registry: WorkerRegistry | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.handlers = dict(handlers)
        self.settings = settings
        self.registry = registry or WorkerRegistry()

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(self._run_slot(slot, stop))
            for slot in range(1, self.settings.worker_concurrency + 1)
        ]
        await stop.wait()
        grace = self.settings.worker_shutdown_grace_seconds
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=grace)
        except TimeoutError:
            logger.warning("worker shutdown grace expired; releasing active leases")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_slot(self, slot: int, stop: asyncio.Event) -> None:
        worker_id = f"{self.settings.worker_id}-{slot}"
        with self.session_factory() as session, session.begin():
            self.registry.register(
                session,
                worker_id=worker_id,
                metadata={"slot": slot, "concurrency": self.settings.worker_concurrency},
            )
        heartbeat = asyncio.create_task(self._heartbeat(worker_id, stop))
        runtime = JobWorker(
            owner=worker_id,
            session_factory=self.session_factory,
            handlers=self.handlers,
            claim_kinds=set(self.handlers),
            settings=WorkerSettings(
                lease_for=self.settings.lease_for,
                heartbeat_interval=self.settings.job_heartbeat_interval,
                poll_interval=timedelta(seconds=self.settings.worker_poll_seconds),
                retry_base=timedelta(seconds=self.settings.retry_base_seconds),
                retry_cap=timedelta(seconds=self.settings.retry_cap_seconds),
            ),
        )
        try:
            await runtime.run_forever(stop)
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            with self.session_factory() as session, session.begin():
                self.registry.heartbeat(session, worker_id=worker_id, status="stopped")

    async def _heartbeat(self, worker_id: str, stop: asyncio.Event) -> None:
        interval = max(5.0, self.settings.worker_heartbeat_seconds / 2)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                with self.session_factory() as session, session.begin():
                    if not self.registry.heartbeat(session, worker_id=worker_id):
                        logger.error("worker heartbeat row disappeared worker_id=%s", worker_id)
                        return

