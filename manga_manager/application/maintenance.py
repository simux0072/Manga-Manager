from __future__ import annotations

from sqlalchemy import select

from manga_manager.application.job_handlers import JobContext, PermanentJobError
from manga_manager.domain.jobs import MaintenancePayload
from manga_manager.worker.runtime import SessionFactory


class MaintenanceHandler:
    def __init__(self, *, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory

    async def __call__(self, context: JobContext) -> None:
        payload = context.lease.payload
        if not isinstance(payload, MaintenancePayload):
            raise RuntimeError("maintenance handler received the wrong payload")
        if payload.action != "stage_probe":
            raise PermanentJobError("unknown_maintenance_action", payload.action)
        context.ensure_lease()
        with self.session_factory() as session:
            session.execute(select(1))
        context.ensure_lease()
