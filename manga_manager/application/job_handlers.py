from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta

from manga_manager.domain.jobs import JobLease


@dataclass(frozen=True, slots=True)
class JobContext:
    lease: JobLease
    lease_lost: asyncio.Event

    def ensure_lease(self) -> None:
        if self.lease_lost.is_set():
            raise LeaseLostError("job lease was lost")


JobHandler = Callable[[JobContext], Awaitable[None]]


class JobExecutionError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class RetryableJobError(JobExecutionError):
    def __init__(self, code: str, message: str, *, retry_after: timedelta | None = None) -> None:
        super().__init__(code, message)
        self.retry_after = retry_after


class PermanentJobError(JobExecutionError):
    pass


class LeaseLostError(JobExecutionError):
    def __init__(self, message: str = "job lease was lost") -> None:
        super().__init__("lease_lost", message)
