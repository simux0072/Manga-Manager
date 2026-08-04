from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    ProviderEndpointState,
    ProviderPolicy,
)
from manga_manager.domain.providers import KNOWN_SOURCES
from manga_manager.infrastructure.bounded_executor import AsyncBoundedExecutor


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


_recovery_probe_active: ContextVar[bool] = ContextVar(
    "provider_recovery_probe_active", default=False
)


@contextmanager
def provider_recovery_probe() -> Iterator[None]:
    """Let only the current recovery task test a provider during cooldown."""

    token = _recovery_probe_active.set(True)
    try:
        yield
    finally:
        _recovery_probe_active.reset(token)


def recovery_probe_active() -> bool:
    return _recovery_probe_active.get()


@dataclass(frozen=True, slots=True)
class RequestReservation:
    delay_seconds: float
    recheck_cooldown: bool = False


class ProviderRequestScheduler:
    """Atomically reserves provider request start times across worker processes."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        cooldown_recheck_seconds: float = 5.0,
    ) -> None:
        self.session_factory = session_factory
        self.cooldown_recheck_seconds = max(cooldown_recheck_seconds, 0.05)
        self._executor = AsyncBoundedExecutor(
            workers=1,
            thread_name_prefix="manga-provider-schedule",
        )

    async def wait(self, source: str, traffic_class: str, interval_seconds: float) -> None:
        bypass_cooldown = recovery_probe_active()
        while True:
            reservation = await self._executor.run(
                self._reserve,
                source,
                interval_seconds,
                traffic_class=traffic_class,
                bypass_cooldown=bypass_cooldown,
            )
            if reservation.delay_seconds <= 0:
                return
            if not reservation.recheck_cooldown:
                await asyncio.sleep(reservation.delay_seconds)
                return
            await asyncio.sleep(
                min(reservation.delay_seconds, self.cooldown_recheck_seconds)
            )

    def close(self) -> None:
        self._executor.close()

    def reserve(
        self,
        source: str,
        interval_seconds: float,
        *,
        traffic_class: str = "origin",
        now: datetime | None = None,
        bypass_cooldown: bool = False,
    ) -> float:
        return self._reserve(
            source,
            interval_seconds,
            traffic_class=traffic_class,
            now=now,
            bypass_cooldown=bypass_cooldown,
        ).delay_seconds

    def _reserve(
        self,
        source: str,
        interval_seconds: float,
        *,
        traffic_class: str = "origin",
        now: datetime | None = None,
        bypass_cooldown: bool = False,
    ) -> RequestReservation:
        if source not in KNOWN_SOURCES:
            raise ValueError(f"unknown provider source: {source}")
        current = now or utcnow()
        with self.session_factory() as session, session.begin():
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                session.execute(
                    select(
                        func.pg_advisory_xact_lock(
                            func.hashtext(f"request:{source}:{traffic_class}")
                        )
                    )
                )
            state = session.get(CatalogSourceState, source)
            if state is None:
                state = CatalogSourceState(source=source)
                session.add(state)
                session.flush()
            policy = session.get(ProviderPolicy, source)
            if policy is not None and policy.request_interval_seconds > 0:
                interval_seconds = policy.request_interval_seconds
            endpoint = session.scalar(
                select(ProviderEndpointState).where(
                    ProviderEndpointState.source == source,
                    ProviderEndpointState.traffic_class == traffic_class,
                )
            )
            if endpoint is None:
                endpoint = ProviderEndpointState(
                    source=source,
                    traffic_class=traffic_class,
                    request_interval_seconds=0.0,
                )
                session.add(endpoint)
                session.flush()
            elif endpoint.request_interval_seconds > 0:
                interval_seconds = endpoint.request_interval_seconds
            cooldown_available = current
            for candidate in (state.cooldown_until, endpoint.cooldown_until):
                if candidate is not None:
                    if candidate.tzinfo is None:
                        candidate = candidate.replace(tzinfo=timezone.utc)
                    cooldown_available = max(cooldown_available, candidate)
            if not bypass_cooldown and cooldown_available > current:
                # Do not reserve a paced request slot beyond a cooldown. Waiting
                # tasks periodically recheck so a successful probe can wake them
                # without holding the original, possibly hours-long delay.
                return RequestReservation(
                    (cooldown_available - current).total_seconds(),
                    recheck_cooldown=True,
                )

            available = current
            if endpoint.next_request_at is not None:
                next_request_at = endpoint.next_request_at
                if next_request_at.tzinfo is None:
                    next_request_at = next_request_at.replace(tzinfo=timezone.utc)
                if bypass_cooldown:
                    # A legacy reservation may have been pushed to the end of a
                    # cooldown. Preserve at most one normal pacing interval.
                    next_request_at = min(
                        next_request_at,
                        current + timedelta(seconds=max(interval_seconds, 0.0)),
                    )
                available = max(available, next_request_at)
            endpoint.next_request_at = available + timedelta(seconds=max(interval_seconds, 0.0))
            endpoint.updated_at = current
            state.next_request_at = endpoint.next_request_at
            state.updated_at = current
            return RequestReservation(max(0.0, (available - current).total_seconds()))
