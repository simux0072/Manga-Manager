from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select, update

from app.adapters import adapter_for_source
from app.adapters.base import SourceRateLimited
from manga_manager.application.job_handlers import (
    DeferredJobError,
    JobContext,
    PermanentJobError,
    exception_message,
)
from manga_manager.application.provider_health import (
    is_cloudflare_origin_error,
    provider_cooldown_until,
    record_provider_failure,
)
from manga_manager.domain.jobs import MaintenancePayload
from manga_manager.infrastructure.db_models import (
    CatalogSourceState,
    ProviderEndpointState,
    ProviderPolicy,
)
from manga_manager.worker.runtime import SessionFactory


class MaintenanceHandler:
    def __init__(self, *, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, MaintenancePayload):
            raise RuntimeError("maintenance handler received the wrong payload")
        if payload.action.startswith("provider_probe_"):
            await self._provider_probe(payload.action.removeprefix("provider_probe_"), context)
            return
        if payload.action != "stage_probe":
            raise PermanentJobError("unknown_maintenance_action", payload.action)
        context.ensure_lease()
        with self.session_factory() as session:
            session.execute(select(1))
        context.ensure_lease()

    async def _provider_probe(self, source: str, context: JobContext) -> None:
        adapter = adapter_for_source(source)
        if adapter is None:
            raise DeferredJobError("provider_unavailable", source, retry_after=timedelta(minutes=5))
        try:
            await adapter.list_recent_frontier([])
        except SourceRateLimited as exc:
            delay = timedelta(minutes=self._record_probe_failure(source))
            raise DeferredJobError(
                "rate_limited", exception_message(exc), retry_after=delay
            ) from exc
        except httpx.HTTPStatusError as exc:
            if not is_cloudflare_origin_error(exc.response.status_code):
                raise
            cooldown = provider_cooldown_until(self.session_factory, source)
            effective = record_provider_failure(
                self.session_factory,
                source=source,
                error=exception_message(exc),
                cooldown_until=cooldown,
            )
            retry_at = effective or cooldown
            raise DeferredJobError(
                "provider_origin_unavailable",
                exception_message(exc),
                retry_after=max(retry_at - datetime.now(timezone.utc), timedelta(seconds=1)),
            ) from exc
        finally:
            await adapter.aclose()
        context.ensure_lease()
        now = datetime.now(timezone.utc)
        with self.session_factory() as session, session.begin():
            policy = session.get(ProviderPolicy, source)
            state = session.get(CatalogSourceState, source)
            if policy is None:
                return
            metadata = dict(policy.metadata_json or {})
            successes = int(metadata.get("recovery_probe_successes") or 0) + 1
            metadata["recovery_probe_successes"] = successes
            if successes >= 2:
                started = metadata.get("recovery_started_at")
                if started:
                    try:
                        elapsed = int((now - datetime.fromisoformat(str(started))).total_seconds())
                        policy.cooldown_seconds = max(60, min(21600, int(elapsed * 1.2)))
                    except (TypeError, ValueError):
                        pass
                for key in (
                    "next_recovery_probe",
                    "recovery_probe_step",
                    "recovery_probe_successes",
                    "recovery_started_at",
                ):
                    metadata.pop(key, None)
                if state is not None:
                    state.cooldown_until = None
                    state.health_status = "healthy"
                session.execute(
                    update(ProviderEndpointState)
                    .where(ProviderEndpointState.source == source)
                    .values(cooldown_until=None, consecutive_failures=0, last_error="")
                )
            else:
                metadata["next_recovery_probe"] = (now + timedelta(seconds=30)).isoformat()
            policy.metadata_json = metadata
            policy.updated_at = now

    def _record_probe_failure(self, source: str) -> int:
        intervals = [1, 2, 5, 10, 20, 40, 60]
        now = datetime.now(timezone.utc)
        with self.session_factory() as session, session.begin():
            policy = session.get(ProviderPolicy, source)
            if policy is None:
                return 5
            metadata = dict(policy.metadata_json or {})
            step = min(int(metadata.get("recovery_probe_step") or 0) + 1, len(intervals) - 1)
            delay = intervals[step]
            metadata["recovery_probe_step"] = step
            metadata["recovery_probe_successes"] = 0
            metadata.setdefault("recovery_started_at", now.isoformat())
            metadata["next_recovery_probe"] = (now + timedelta(minutes=delay)).isoformat()
            policy.metadata_json = metadata
            policy.updated_at = now
            return delay
