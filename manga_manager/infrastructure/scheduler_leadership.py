from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import Connection, Engine, text


DEFAULT_SCHEDULER_LOCK_ID = 6_672_821_481


@dataclass(slots=True)
class SchedulerLeadership:
    connection: Connection
    lock_id: int
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        try:
            self.connection.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"),
                {"lock_id": self.lock_id},
            )
        finally:
            self.connection.close()
            self.released = True

    def __enter__(self) -> SchedulerLeadership:
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


def try_acquire_scheduler_leadership(
    engine: Engine,
    *,
    lock_id: int = DEFAULT_SCHEDULER_LOCK_ID,
) -> SchedulerLeadership | None:
    if engine.dialect.name != "postgresql":
        raise RuntimeError("scheduler leadership requires PostgreSQL advisory locks")
    connection = engine.connect()
    try:
        acquired = connection.scalar(
            text("SELECT pg_try_advisory_lock(:lock_id)"),
            {"lock_id": lock_id},
        )
        if not acquired:
            connection.close()
            return None
        return SchedulerLeadership(connection=connection, lock_id=lock_id)
    except Exception:
        connection.close()
        raise
