"""Application use-case contracts."""

from manga_manager.application.job_handlers import (
    JobContext,
    JobHandler,
    LeaseLostError,
    PermanentJobError,
    RetryableJobError,
)

__all__ = [
    "JobContext",
    "JobHandler",
    "LeaseLostError",
    "PermanentJobError",
    "RetryableJobError",
]
