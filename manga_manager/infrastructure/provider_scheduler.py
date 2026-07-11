from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from manga_manager.infrastructure.db_models import CatalogSourceState


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ProviderRequestScheduler:
    """Atomically reserves provider request start times across worker processes."""

    def __init__(self, session_factory: Callable[[], Session]) -> None:
        self.session_factory = session_factory

    async def wait(self, source: str, interval_seconds: float) -> None:
        delay = await asyncio.to_thread(self.reserve, source, interval_seconds)
        if delay > 0:
            await asyncio.sleep(delay)

    def reserve(
        self, source: str, interval_seconds: float, *, now: datetime | None = None
    ) -> float:
        current = now or utcnow()
        with self.session_factory() as session, session.begin():
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                session.execute(select(func.pg_advisory_xact_lock(func.hashtext(f"request:{source}"))))
            state = session.get(CatalogSourceState, source)
            if state is None:
                state = CatalogSourceState(source=source)
                session.add(state)
                session.flush()
            available = current
            for candidate in (state.cooldown_until, state.next_request_at):
                if candidate is not None:
                    if candidate.tzinfo is None:
                        candidate = candidate.replace(tzinfo=timezone.utc)
                    available = max(available, candidate)
            state.next_request_at = available + timedelta(seconds=max(interval_seconds, 0.0))
            state.updated_at = current
            return max(0.0, (available - current).total_seconds())
