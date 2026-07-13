from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, func, select, text
from sqlalchemy.orm import Session

from manga_manager.infrastructure.db_models import StorageReservation, StorageState


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class StorageCapacityCoordinator:
    """Coordinates disk reservations across all PostgreSQL workers."""

    def __init__(self, root: Path, min_free_bytes: int) -> None:
        self.root = root
        self.min_free_bytes = min_free_bytes

    def reserve(
        self,
        session: Session,
        *,
        job_id: int,
        owner: str,
        requested_bytes: int,
        lease_expires_at: datetime,
    ) -> bool:
        now = utcnow()
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            session.execute(text("SELECT pg_advisory_xact_lock(hashtext('storage-capacity'))"))
        session.execute(
            delete(StorageReservation).where(StorageReservation.lease_expires_at <= now)
        )
        existing = session.scalar(
            select(StorageReservation).where(StorageReservation.job_id == job_id)
        )
        if existing is not None:
            existing.lease_expires_at = lease_expires_at
            return True
        location = self.root if self.root.exists() else self.root.parent
        free = shutil.disk_usage(location).free
        reserved = int(
            session.scalar(select(func.coalesce(func.sum(StorageReservation.reserved_bytes), 0)))
            or 0
        )
        state = session.get(StorageState, 1)
        if state is None:
            state = StorageState(id=1)
            session.add(state)
        state.free_bytes = free
        state.min_free_bytes = self.min_free_bytes
        state.reserved_bytes = reserved
        state.updated_at = now
        if free - reserved - requested_bytes < self.min_free_bytes:
            state.paused = True
            state.reason = "free-space reserve would be crossed"
            return False
        state.paused = False
        state.reason = ""
        session.add(
            StorageReservation(
                job_id=job_id,
                owner=owner,
                reserved_bytes=requested_bytes,
                lease_expires_at=lease_expires_at,
            )
        )
        state.reserved_bytes = reserved + requested_bytes
        return True

    def refresh(self, session: Session) -> StorageState:
        now = utcnow()
        session.execute(
            delete(StorageReservation).where(StorageReservation.lease_expires_at <= now)
        )
        location = self.root if self.root.exists() else self.root.parent
        free = shutil.disk_usage(location).free
        reserved = int(
            session.scalar(select(func.coalesce(func.sum(StorageReservation.reserved_bytes), 0)))
            or 0
        )
        state = session.get(StorageState, 1) or StorageState(id=1)
        state.free_bytes = free
        state.min_free_bytes = self.min_free_bytes
        state.reserved_bytes = reserved
        state.paused = free - reserved < self.min_free_bytes
        state.reason = "free-space reserve would be crossed" if state.paused else ""
        state.updated_at = now
        session.add(state)
        return state
