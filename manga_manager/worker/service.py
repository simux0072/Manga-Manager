from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta

import psycopg

from manga_manager.application.job_handlers import JobHandler
from manga_manager.domain.jobs import JobKind
from manga_manager.domain.providers import provider_names
from manga_manager.infrastructure.worker_registry import WorkerRegistry
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.settings import V2Settings
from manga_manager.worker.runtime import JobWorker, SessionFactory, WorkerSettings


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WorkerSlotSpec:
    slot: int
    name: str
    claim_pools: frozenset[str]
    kinds: frozenset[JobKind]


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
        self.queue = JobQueue()
        self.pools = pools

    async def run(self, stop: asyncio.Event) -> None:
        specs = self._pool_specs()
        slot_wakeups = {spec.slot: asyncio.Event() for spec in specs}
        pool_wakeups: dict[str, list[asyncio.Event]] = {}
        for spec in specs:
            for pool in spec.claim_pools:
                pool_wakeups.setdefault(pool, []).append(slot_wakeups[spec.slot])
        tasks = [
            asyncio.create_task(self._run_slot(spec, stop, slot_wakeups[spec.slot]))
            for spec in specs
        ]
        listener = asyncio.create_task(self._listen_for_jobs(stop, pool_wakeups))
        await stop.wait()
        grace = self.settings.worker_shutdown_grace_seconds
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=grace)
        except TimeoutError:
            logger.warning("worker shutdown grace expired; releasing active leases")
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)

    def _pool_specs(self) -> list[WorkerSlotSpec]:
        network_kinds = frozenset(
            {
                JobKind.SOURCE_PULL,
                JobKind.SOURCE_REFRESH,
                JobKind.CHAPTER_DOWNLOAD,
                # Provider recovery probes are maintenance jobs deliberately
                # routed into a provider pull pool.
                JobKind.MAINTENANCE,
            }
        ).intersection(self.handlers)
        enabled_sources = set(self.settings.source_intervals())
        network_pools = frozenset(
            f"{traffic}:{source}"
            for source in provider_names()
            if source in enabled_sources
            for traffic in ("download", "pull", "refresh")
        )
        if self.pools is not None:
            network_pools = network_pools.intersection(self.pools)

        requested = [
            ("kavita", {JobKind.KAVITA_SYNC}),
            # Catalog rescoring and repair share a permit-limited pool even though
            # every worker slot is eligible to claim either kind.
            ("maintenance", {JobKind.LIBRARY_REPAIR, JobKind.MAINTENANCE}),
            ("health", {JobKind.MAINTENANCE}),
            ("cover_backfill", {JobKind.COVER_BACKFILL}),
        ]
        shared_pools = set(network_pools)
        shared_kinds = set(network_kinds)
        for pool, kinds in requested:
            if self.pools is not None and pool not in self.pools:
                continue
            if not kinds.intersection(self.handlers):
                continue
            shared_pools.add(pool)
            shared_kinds.update(kinds.intersection(self.handlers))
        if not shared_pools or not shared_kinds:
            return []

        if self.pools is None:
            count = self.settings.worker_concurrency
        else:
            limits = self.settings.pool_limits()
            count = min(
                self.settings.worker_concurrency,
                max((limits.get(pool, 1) for pool in shared_pools), default=1),
            )
        claim_pools = frozenset(shared_pools)
        kinds = frozenset(shared_kinds)
        return [
            WorkerSlotSpec(
                slot=slot,
                name="shared",
                claim_pools=claim_pools,
                kinds=kinds,
            )
            for slot in range(1, count + 1)
        ]

    async def _run_slot(
        self,
        spec: WorkerSlotSpec,
        stop: asyncio.Event,
        wakeup: asyncio.Event,
    ) -> None:
        worker_id = (
            f"{self.settings.worker_id}-{spec.name.replace(':', '-')}-{spec.slot}"
        )
        with self.session_factory() as session, session.begin():
            self.registry.register(
                session,
                worker_id=worker_id,
                metadata={
                    "slot": spec.slot,
                    "pool": spec.name,
                    "claim_pools": sorted(spec.claim_pools),
                    "pool_limits": self.settings.pool_limits(),
                },
            )
        heartbeat = asyncio.create_task(self._heartbeat(worker_id, stop))
        runtime = JobWorker(
            owner=worker_id,
            session_factory=self.session_factory,
            handlers=self.handlers,
            queue=self.queue,
            claim_kinds=spec.kinds.intersection(self.handlers),
            claim_pools=spec.claim_pools,
            settings=WorkerSettings(
                lease_for=self.settings.lease_for,
                heartbeat_interval=self.settings.job_heartbeat_interval,
                poll_interval=timedelta(seconds=self.settings.worker_poll_seconds),
                retry_base=timedelta(seconds=self.settings.retry_base_seconds),
                retry_cap=timedelta(seconds=self.settings.retry_cap_seconds),
                pool_limits=self.settings.pool_limits(),
                provider_pacing_window_seconds=(
                    self.settings.provider_pacing_window_seconds
                ),
            ),
        )
        try:
            await runtime.run_forever(stop, wakeup)
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
            with self.session_factory() as session, session.begin():
                self.registry.heartbeat(session, worker_id=worker_id, status="stopped")

    async def _listen_for_jobs(
        self,
        stop: asyncio.Event,
        wakeups: Mapping[str, list[asyncio.Event]],
    ) -> None:
        """Use one async LISTEN socket per worker process with polling as recovery."""
        with self.session_factory() as session:
            bind = session.get_bind()
            url = getattr(bind, "url", None)
        if url is None or bind.dialect.name != "postgresql":
            return
        connect = {
            "host": url.host or "localhost",
            "port": url.port or 5432,
            "dbname": url.database or "",
            "user": url.username or "",
            "password": url.password or "",
            "connect_timeout": 10,
            "application_name": f"{self.settings.worker_id}-listener",
        }
        while not stop.is_set():
            try:
                connection = await psycopg.AsyncConnection.connect(**connect, autocommit=True)
                async with connection:
                    await connection.execute("LISTEN manga_manager_jobs")
                    async for notification in connection.notifies():
                        events = wakeups.get(notification.payload)
                        if events is not None:
                            for event in events:
                                event.set()
                        else:
                            # Unknown/legacy pool names are rare and should not
                            # wait for the recovery poll after a rolling upgrade.
                            for pool_events in wakeups.values():
                                for event in pool_events:
                                    event.set()
                        if stop.is_set():
                            return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("job notification listener failed; polling remains active")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=10)
                except TimeoutError:
                    continue

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
