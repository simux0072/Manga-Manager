from __future__ import annotations

import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, text


_URL_QUERY = re.compile(r"(https?://[^\s?'\"<>]+)\?[^\s'\"<>]*", re.IGNORECASE)
_URL_PASSWORD = re.compile(r"(://[^:/\s]+:)[^@\s]+(@)")
_AUTHORIZATION_VALUE = re.compile(
    r"(?i)(authorization\s*[:=]\s*)(?:(?:bearer|basic)\s+)?[^\s,;]+"
)
_SECRET_VALUE = re.compile(
    r"(?i)(api[_-]?key|authorization|password|secret|token)(\s*[:=]\s*)([^\s,;]+)"
)


def redact_text(value: object, *, limit: int = 2_000) -> str:
    text_value = str(value or "")
    text_value = _URL_QUERY.sub(r"\1?[redacted]", text_value)
    text_value = _URL_PASSWORD.sub(r"\1[redacted]\2", text_value)
    text_value = _AUTHORIZATION_VALUE.sub(r"\1[redacted]", text_value)
    text_value = _SECRET_VALUE.sub(r"\1\2[redacted]", text_value)
    if len(text_value) > limit:
        return text_value[:limit] + "…"
    return text_value


def _iso(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def build_diagnostic_bundle(
    engine: Engine,
    *,
    storage_root: Path,
    recent_failure_limit: int = 200,
) -> dict[str, Any]:
    if recent_failure_limit < 0 or recent_failure_limit > 1_000:
        raise ValueError("recent_failure_limit must be between 0 and 1000")

    with engine.connect() as connection:
        migration = connection.scalar(text("SELECT version_num FROM alembic_version"))
        database_bytes = int(
            connection.scalar(text("SELECT pg_database_size(current_database())")) or 0
        )
        catalog = {
            str(name): int(count)
            for name, count in connection.execute(
                text(
                    "SELECT name,value FROM ("
                    "SELECT 'series' name,count(*) value FROM series_v2 UNION ALL "
                    "SELECT 'source_series',count(*) FROM source_series_v2 UNION ALL "
                    "SELECT 'chapters',count(*) FROM chapter_v2 UNION ALL "
                    "SELECT 'releases',count(*) FROM chapter_release_v2 UNION ALL "
                    "SELECT 'active_artifacts',count(*) FROM chapter_artifact "
                    "WHERE state='active' UNION ALL "
                    "SELECT 'blobs',count(*) FROM artifact_blob UNION ALL "
                    "SELECT 'library_projections',count(*) FROM library_projection UNION ALL "
                    "SELECT 'kavita_projections',count(*) FROM kavita_projection"
                    ") counts ORDER BY name"
                )
            )
        }
        job_counts = [
            {
                "kind": str(kind),
                "source": str(source),
                "status": str(status),
                "error_code": str(error_code),
                "count": int(count),
            }
            for kind, source, status, error_code, count in connection.execute(
                text(
                    "SELECT kind,source,status,error_code,count(*) FROM job "
                    "GROUP BY kind,source,status,error_code "
                    "ORDER BY kind,source,status,error_code"
                )
            )
        ]
        recent_failures = [
            {
                "id": int(job_id),
                "kind": str(kind),
                "source": str(source),
                "status": str(status),
                "attempts": int(attempts),
                "max_attempts": int(max_attempts),
                "error_code": str(error_code),
                "error_message": redact_text(error_message),
                "updated_at": _iso(updated_at),
            }
            for (
                job_id,
                kind,
                source,
                status,
                attempts,
                max_attempts,
                error_code,
                error_message,
                updated_at,
            ) in connection.execute(
                text(
                    "SELECT id,kind,source,status,attempts,max_attempts,error_code,"
                    "error_message,updated_at FROM job "
                    "WHERE status IN ('failed','retry_wait') OR error_code<>'' "
                    "ORDER BY updated_at DESC,id DESC LIMIT :limit"
                ),
                {"limit": recent_failure_limit},
            )
        ]
        source_states = [
            {
                "source": str(source),
                "enabled": bool(enabled),
                "health": str(health),
                "consecutive_failures": int(failures),
                "last_error": redact_text(last_error),
                "cooldown_until": _iso(cooldown_until),
                "next_request_at": _iso(next_request_at),
                "last_poll_at": _iso(last_poll_at),
                "updated_at": _iso(updated_at),
            }
            for (
                source,
                enabled,
                health,
                failures,
                last_error,
                cooldown_until,
                next_request_at,
                last_poll_at,
                updated_at,
            ) in connection.execute(
                text(
                    "SELECT source,manual_enabled,health_status,consecutive_failures,last_error,"
                    "cooldown_until,next_request_at,last_poll_at,updated_at "
                    "FROM source_state_v2 ORDER BY source"
                )
            )
        ]
        provider_policies = [
            {
                "source": str(source),
                "job_limit": int(job_limit),
                "page_limit": int(page_limit),
                "request_interval_seconds": float(interval),
                "cooldown_seconds": int(cooldown),
                "last_limited_at": _iso(last_limited),
                "updated_at": _iso(updated_at),
            }
            for source, job_limit, page_limit, interval, cooldown, last_limited, updated_at in (
                connection.execute(
                    text(
                        "SELECT source,learned_job_limit,learned_page_limit,"
                        "request_interval_seconds,cooldown_seconds,last_limited_at,updated_at "
                        "FROM provider_policy ORDER BY source"
                    )
                )
            )
        ]
        workers = [
            {
                "worker_id": redact_text(worker_id, limit=200),
                "status": str(status),
                "active_job_id": int(active_job_id) if active_job_id is not None else None,
                "started_at": _iso(started_at),
                "heartbeat_at": _iso(heartbeat_at),
                "stopped_at": _iso(stopped_at),
            }
            for worker_id, status, active_job_id, started_at, heartbeat_at, stopped_at in (
                connection.execute(
                    text(
                        "SELECT worker_id,status,active_job_id,started_at,heartbeat_at,stopped_at "
                        "FROM worker_heartbeat ORDER BY heartbeat_at DESC"
                    )
                )
            )
        ]
        storage_state_row = connection.execute(
            text(
                "SELECT paused,free_bytes,min_free_bytes,reserved_bytes,reason,updated_at "
                "FROM storage_state WHERE id=1"
            )
        ).first()
        blob_bytes = int(
            connection.scalar(text("SELECT coalesce(sum(byte_count),0) FROM artifact_blob")) or 0
        )

    disk_probe = storage_root
    while not disk_probe.exists() and disk_probe != disk_probe.parent:
        disk_probe = disk_probe.parent
    disk = shutil.disk_usage(disk_probe)
    storage_state = None
    if storage_state_row:
        storage_state = {
            "paused": bool(storage_state_row.paused),
            "free_bytes": int(storage_state_row.free_bytes),
            "min_free_bytes": int(storage_state_row.min_free_bytes),
            "reserved_bytes": int(storage_state_row.reserved_bytes),
            "reason": redact_text(storage_state_row.reason),
            "updated_at": _iso(storage_state_row.updated_at),
        }
    return {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "migration": str(migration or ""),
        "database_bytes": database_bytes,
        "catalog": catalog,
        "jobs": job_counts,
        "recent_failures": recent_failures,
        "sources": source_states,
        "provider_policies": provider_policies,
        "workers": workers,
        "storage": {
            "blob_bytes": blob_bytes,
            "filesystem_total_bytes": disk.total,
            "filesystem_free_bytes": disk.free,
            "state": storage_state,
        },
    }
