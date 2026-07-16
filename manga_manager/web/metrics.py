from __future__ import annotations

import contextvars
import threading
import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Engine, event


@dataclass
class RequestMeasurement:
    sql_queries: int = 0
    sql_seconds: float = 0.0


@dataclass
class RouteMeasurement:
    requests: int = 0
    duration_seconds: float = 0.0
    sql_queries: int = 0
    sql_seconds: float = 0.0
    response_bytes: int = 0


_current_request: contextvars.ContextVar[RequestMeasurement | None] = contextvars.ContextVar(
    "manga_manager_request_measurement", default=None
)


def begin_request_measurement() -> tuple[RequestMeasurement, contextvars.Token]:
    measurement = RequestMeasurement()
    return measurement, _current_request.set(measurement)


def end_request_measurement(token: contextvars.Token) -> None:
    _current_request.reset(token)


def install_sql_timing(engine: Engine) -> None:
    """Install one process-local SQL timer on an engine.

    The mutable request measurement is propagated into FastAPI's worker thread, so synchronous
    SQLAlchemy calls remain attributed to the HTTP request that initiated them.
    """

    if getattr(engine, "_manga_manager_sql_timing", False):
        return
    setattr(engine, "_manga_manager_sql_timing", True)

    @event.listens_for(engine, "before_cursor_execute")
    def before_cursor_execute(
        _connection: Any,
        _cursor: Any,
        _statement: str,
        _parameters: Any,
        context: Any,
        _executemany: bool,
    ) -> None:
        measurement = _current_request.get()
        if measurement is None:
            return
        measurement.sql_queries += 1
        context._manga_manager_query_started_at = time.perf_counter()

    @event.listens_for(engine, "after_cursor_execute")
    def after_cursor_execute(
        _connection: Any,
        _cursor: Any,
        _statement: str,
        _parameters: Any,
        context: Any,
        _executemany: bool,
    ) -> None:
        measurement = _current_request.get()
        started = getattr(context, "_manga_manager_query_started_at", None)
        if measurement is not None and started is not None:
            measurement.sql_seconds += max(time.perf_counter() - started, 0.0)


class RequestMetrics:
    """Small bounded in-process route counters suitable for a single Pi web process."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._routes: dict[tuple[str, str, int], RouteMeasurement] = {}

    def observe(
        self,
        *,
        method: str,
        route: str,
        status: int,
        duration_seconds: float,
        sql_queries: int,
        sql_seconds: float,
        response_bytes: int,
    ) -> None:
        # Route is a FastAPI template (or the fixed "unmatched" label), so this dictionary cannot
        # grow with user-supplied paths or identifiers.
        key = (method, route, status)
        with self._lock:
            value = self._routes.setdefault(key, RouteMeasurement())
            value.requests += 1
            value.duration_seconds += max(duration_seconds, 0.0)
            value.sql_queries += max(sql_queries, 0)
            value.sql_seconds += max(sql_seconds, 0.0)
            value.response_bytes += max(response_bytes, 0)

    def render_prometheus(self) -> str:
        with self._lock:
            rows = [(key, RouteMeasurement(**vars(value))) for key, value in self._routes.items()]
        lines = [
            "# HELP manga_manager_http_requests_total Completed HTTP requests.",
            "# TYPE manga_manager_http_requests_total counter",
            "# HELP manga_manager_http_duration_seconds_total Cumulative request handling time.",
            "# TYPE manga_manager_http_duration_seconds_total counter",
            "# HELP manga_manager_http_sql_queries_total SQL statements executed by HTTP requests.",
            "# TYPE manga_manager_http_sql_queries_total counter",
            "# HELP manga_manager_http_sql_duration_seconds_total Cumulative SQL execution time.",
            "# TYPE manga_manager_http_sql_duration_seconds_total counter",
            "# HELP manga_manager_http_response_bytes_total Response bytes with a known length.",
            "# TYPE manga_manager_http_response_bytes_total counter",
        ]
        for (method, route, status), value in sorted(rows):
            labels = (
                f'method="{_escape(method)}",route="{_escape(route)}",status="{status}"'
            )
            lines.extend(
                (
                    f"manga_manager_http_requests_total{{{labels}}} {value.requests}",
                    f"manga_manager_http_duration_seconds_total{{{labels}}} "
                    f"{value.duration_seconds:.9f}",
                    f"manga_manager_http_sql_queries_total{{{labels}}} {value.sql_queries}",
                    f"manga_manager_http_sql_duration_seconds_total{{{labels}}} "
                    f"{value.sql_seconds:.9f}",
                    f"manga_manager_http_response_bytes_total{{{labels}}} "
                    f"{value.response_bytes}",
                )
            )
        return "\n".join(lines) + "\n"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
