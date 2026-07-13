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
        pools: set[str] | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.handlers = dict(handlers)
        self.settings = settings
        self.registry = registry or WorkerRegistry()
        self.pools = pools

    async def run(self, stop: asyncio.Event) -> None:
        specs = self._pool_specs()
        tasks = [
            asyncio.create_task(self._run_slot(slot, pool, kinds, stop))
            for slot, pool, kinds in specs
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

    def _pool_specs(self) -> list[tuple[int, str, set[JobKind]]]:
        requested = [
            (
                "pull:asura",
                1,
                {JobKind.SOURCE_PULL, JobKind.SOURCE_REFRESH, JobKind.MAINTENANCE},
            ),
            (
                "pull:mangafire",
                1,
                {JobKind.SOURCE_PULL, JobKind.SOURCE_REFRESH, JobKind.MAINTENANCE},
            ),
            (
                "pull:kingofshojo",
                1,
                {JobKind.SOURCE_PULL, JobKind.SOURCE_REFRESH, JobKind.MAINTENANCE},
            ),
            (
                "download:asura",
                self.settings.asura_download_concurrency,
                {JobKind.CHAPTER_DOWNLOAD},
            ),
            (
                "download:kingofshojo",
                self.settings.kingofshojo_download_concurrency,
                {JobKind.CHAPTER_DOWNLOAD},
            ),
            (
                "download:mangafire",
                self.settings.mangafire_download_concurrency,
                {JobKind.CHAPTER_DOWNLOAD},
            ),
            ("kavita", 1, {JobKind.KAVITA_SYNC}),
            ("maintenance", 1, {JobKind.LIBRARY_REPAIR}),
            ("health", 1, {JobKind.MAINTENANCE}),
            ("cover_backfill", 1, {JobKind.COVER_BACKFILL}),
        ]
        specs: list[tuple[int, str, set[JobKind]]] = []
        for pool, count, kinds in requested:
            if self.pools is not None and pool not in self.pools:
                continue
            if not kinds.intersection(self.handlers):
                continue
            for _ in range(count):
                specs.append((len(specs) + 1, pool, kinds))
        return specs

    async def _run_slot(
        self, slot: int, pool: str, kinds: set[JobKind], stop: asyncio.Event
    ) -> None:
        worker_id = f"{self.settings.worker_id}-{pool.replace(':', '-')}-{slot}"
        with self.session_factory() as session, session.begin():
            self.registry.register(
                session,
                worker_id=worker_id,
                metadata={"slot": slot, "pool": pool, "pool_limits": self.settings.pool_limits()},
            )
        heartbeat = asyncio.create_task(self._heartbeat(worker_id, stop))
        runtime = JobWorker(
            owner=worker_id,
            session_factory=self.session_factory,
            handlers=self.handlers,
            claim_kinds=kinds.intersection(self.handlers),
            claim_pools={pool},
            settings=WorkerSettings(
                lease_for=self.settings.lease_for,
                heartbeat_interval=self.settings.job_heartbeat_interval,
                poll_interval=timedelta(seconds=self.settings.worker_poll_seconds),
                retry_base=timedelta(seconds=self.settings.retry_base_seconds),
                retry_cap=timedelta(seconds=self.settings.retry_cap_seconds),
                pool_limits=self.settings.pool_limits(),
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
