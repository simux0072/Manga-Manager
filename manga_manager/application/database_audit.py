from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection


COUNT_TABLES = (
    "series_v2",
    "source_series_v2",
    "chapter_v2",
    "chapter_release_v2",
    "chapter_artifact",
    "artifact_blob",
    "library_projection",
    "kavita_projection",
    "job",
    "job_event",
    "job_permit",
    "storage_reservation",
    "provider_request_sample",
    "workload_cycle",
)


LATEST_MISMATCH_SQL = """
WITH ranked AS (
  SELECT c.series_id, c.display_number, r.source,
         COALESCE(r.published_at, r.first_seen_at) AS release_at,
         ROW_NUMBER() OVER (
           PARTITION BY c.series_id
           ORDER BY (c.sort_number IS NULL), c.sort_number DESC,
                    CASE WHEN c.sort_number IS NULL THEN r.published_at END DESC NULLS LAST,
                    r.id DESC
         ) AS position
  FROM chapter_v2 c JOIN chapter_release_v2 r ON r.chapter_id=c.id
)
SELECT count(*) FROM series_v2 s LEFT JOIN ranked r ON r.series_id=s.id AND r.position=1
WHERE (r.series_id IS NULL AND (
         COALESCE(s.latest_release_number, '') <> ''
      OR COALESCE(s.latest_release_source, '') <> ''
      OR s.latest_release_at IS NOT NULL
      ))
   OR (r.series_id IS NOT NULL AND (
         COALESCE(s.latest_release_number, '') <> COALESCE(r.display_number, '')
      OR COALESCE(s.latest_release_source, '') <> COALESCE(r.source, '')
      OR s.latest_release_at IS DISTINCT FROM r.release_at
      ))
"""


def _scalar(connection: Connection, sql: str) -> int:
    return int(connection.scalar(text(sql)) or 0)


def audit_database(connection: Connection, *, statement_timeout_ms: int = 5_000) -> dict[str, Any]:
    if connection.dialect.name == "postgresql":
        # PostgreSQL's SET grammar does not accept bind parameters. set_config
        # keeps the audit bounded without interpolating user input into SQL.
        connection.execute(
            text("SELECT set_config('statement_timeout', :timeout, true)"),
            {"timeout": f"{max(1, statement_timeout_ms)}ms"},
        )
        connection.execute(text("SELECT set_config('lock_timeout', '1s', true)"))
    counts = {table: _scalar(connection, f"SELECT count(*) FROM {table}") for table in COUNT_TABLES}
    checks = {
        "latest_release_mismatches": _scalar(connection, LATEST_MISMATCH_SQL),
        "duplicate_provider_identities": _scalar(
            connection,
            "SELECT count(*) FROM (SELECT series_id,source FROM source_series_v2 "
            "GROUP BY series_id,source HAVING count(*)>1) rows",
        ),
        "active_artifacts_without_blob": _scalar(
            connection,
            "SELECT count(*) FROM chapter_artifact a LEFT JOIN artifact_blob b "
            "ON b.checksum=a.blob_checksum WHERE a.state='active' AND b.checksum IS NULL",
        ),
        "active_artifacts_without_library_projection": _scalar(
            connection,
            "SELECT count(*) FROM chapter_artifact a LEFT JOIN library_projection p "
            "ON p.artifact_id=a.id WHERE a.state='active' AND p.artifact_id IS NULL",
        ),
        "expired_job_leases": _scalar(
            connection,
            "SELECT count(*) FROM job WHERE status='leased' AND lease_expires_at < CURRENT_TIMESTAMP",
        ),
        "expired_permits": _scalar(
            connection, "SELECT count(*) FROM job_permit WHERE lease_expires_at < CURRENT_TIMESTAMP"
        ),
        "expired_storage_reservations": _scalar(
            connection,
            "SELECT count(*) FROM storage_reservation "
            "WHERE lease_expires_at < CURRENT_TIMESTAMP",
        ),
        "orphan_reading_states": _scalar(
            connection,
            "SELECT count(*) FROM chapter_reading_state_v2 r LEFT JOIN chapter_v2 c "
            "ON c.id=r.chapter_id WHERE c.id IS NULL",
        ),
        "multiple_active_artifacts_per_chapter": _scalar(
            connection,
            "SELECT count(*) FROM (SELECT chapter_id FROM chapter_artifact "
            "WHERE state='active' GROUP BY chapter_id HAVING count(*)>1) rows",
        ),
        "unknown_provider_identities": _scalar(
            connection,
            "SELECT count(*) FROM source_series_v2 "
            "WHERE source NOT IN ('asura','mangadex','mangafire','kingofshojo')",
        ),
        "multiple_active_workload_cycles": max(
            _scalar(connection, "SELECT count(*) FROM workload_cycle WHERE status='active'") - 1,
            0,
        ),
        "obsolete_pending_matches": _scalar(
            connection,
            "SELECT count(*) FROM match_decision_v2 d "
            "JOIN source_series_v2 l ON l.id=d.left_source_series_id "
            "JOIN source_series_v2 r ON r.id=d.right_source_series_id "
            "WHERE d.decision='pending' AND l.series_id=r.series_id",
        ),
    }
    jobs = {
        str(status): int(count)
        for status, count in connection.execute(
            text("SELECT status,count(*) FROM job GROUP BY status ORDER BY status")
        )
    }
    database: dict[str, Any] = {"dialect": connection.dialect.name}
    table_stats: list[dict[str, Any]] = []
    if connection.dialect.name == "postgresql":
        database["size_bytes"] = int(connection.scalar(text("SELECT pg_database_size(current_database())")) or 0)
        table_stats = [
            dict(row._mapping)
            for row in connection.execute(
                text(
                    "SELECT relname AS table_name,n_live_tup,n_dead_tup,last_analyze,last_autoanalyze,"
                    "pg_total_relation_size(relid) AS total_bytes,"
                    "pg_indexes_size(relid) AS index_bytes "
                    "FROM pg_stat_user_tables ORDER BY n_dead_tup DESC,relname LIMIT 30"
                )
            )
        ]
    critical = sum(checks.values())
    return {
        "ok": critical == 0,
        "migration": str(connection.scalar(text("SELECT version_num FROM alembic_version")) or ""),
        "database": database,
        "counts": counts,
        "jobs": jobs,
        "checks": checks,
        "table_stats": table_stats,
    }


def write_database_audit(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".json":
        path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")
        return
    lines = [
        "# Database Audit",
        "",
        f"- Healthy: **{payload['ok']}**",
        f"- Migration: `{payload['migration']}`",
        f"- Dialect: `{payload['database']['dialect']}`",
        "",
        "## Integrity checks",
        "",
    ]
    lines.extend(f"- {name}: {value}" for name, value in payload["checks"].items())
    lines.extend(["", "## Row counts", ""])
    lines.extend(f"- {name}: {value}" for name, value in payload["counts"].items())
    path.write_text("\n".join(lines) + "\n")
