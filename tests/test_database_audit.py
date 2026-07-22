from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from manga_manager.application.database_audit import audit_database, write_database_audit
from manga_manager.cli import print_stage_check
from manga_manager.infrastructure.database import create_database_engine, run_migrations


def test_database_audit_is_read_only_and_reports_latest_mismatch(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'audit.db'}"
    run_migrations(url)
    engine = create_database_engine(url, allow_sqlite_for_tests=True)
    with engine.begin() as connection:
        payload = audit_database(connection)

    assert payload["ok"] is True
    assert payload["checks"]["latest_release_mismatches"] == 0
    assert payload["counts"]["series_v2"] == 0

    report = tmp_path / "audit.json"
    write_database_audit(report, payload)
    assert json.loads(report.read_text())["migration"] == "0022_chapter_release_quality"


def test_database_audit_writes_markdown(tmp_path: Path) -> None:
    report = tmp_path / "audit.md"
    write_database_audit(
        report,
        {
            "ok": False,
            "migration": "test",
            "database": {"dialect": "postgresql"},
            "checks": {"latest_release_mismatches": 1},
            "counts": {"series_v2": 2},
        },
    )
    assert "# Database Audit" in report.read_text()
    assert "latest_release_mismatches: 1" in report.read_text()


def test_database_audit_stdout_serializes_postgres_stat_timestamps(capsys) -> None:
    timestamp = datetime(2026, 7, 17, 1, 2, 3, tzinfo=timezone.utc)

    print_stage_check(
        {"ok": True, "table_stats": [{"last_autoanalyze": timestamp}]},
        json_output=True,
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["table_stats"][0]["last_autoanalyze"] == str(timestamp)
