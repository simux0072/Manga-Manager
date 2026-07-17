from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

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


class ProviderRequestScheduler:
    """Atomically reserves provider request start times across worker processes."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory
        self._executor = AsyncBoundedExecutor(
            workers=1,
            thread_name_prefix="manga-provider-schedule",
        )

    async def wait(self, source: str, traffic_class: str, interval_seconds: float) -> None:
        delay = await self._executor.run(
            self.reserve, source, interval_seconds, traffic_class=traffic_class
        )
        if delay > 0:
            await asyncio.sleep(delay)

    def close(self) -> None:
        self._executor.close()

    def reserve(
        self,
        source: str,
        interval_seconds: float,
        *,
        traffic_class: str = "origin",
        now: datetime | None = None,
    ) -> float:
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
            available = current
            for candidate in (
                state.cooldown_until,
                endpoint.cooldown_until,
                endpoint.next_request_at,
            ):
                if candidate is not None:
                    if candidate.tzinfo is None:
                        candidate = candidate.replace(tzinfo=timezone.utc)
                    available = max(available, candidate)
            endpoint.next_request_at = available + timedelta(seconds=max(interval_seconds, 0.0))
            endpoint.updated_at = current
            state.next_request_at = endpoint.next_request_at
            state.updated_at = current
            return max(0.0, (available - current).total_seconds())
