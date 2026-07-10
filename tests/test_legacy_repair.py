from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from manga_manager.application.legacy_repair import LegacyRepair, write_legacy_report
from manga_manager.cli import main


def legacy_database(path: Path) -> Path:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE series (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
        CREATE TABLE source_series (
          id INTEGER PRIMARY KEY, series_id INTEGER NOT NULL REFERENCES series(id),
          source TEXT NOT NULL, metadata_json TEXT
        );
        CREATE TABLE chapter_release (
          id INTEGER PRIMARY KEY, number TEXT, title TEXT, url TEXT
        );
        INSERT INTO series VALUES (1, 'Collision'), (2, 'No source');
        INSERT INTO source_series VALUES (1, 1, 'asura', ''), (2, 1, 'asura', '{}');
        INSERT INTO chapter_release VALUES
          (1, '1', 'Latest: Chapter 1', 'https://example/chapter/1'),
          (2, 'first chapter', 'First', 'https://example/first'),
          (3, '80', 'Chapter {{number}} {{date}}', 'https://example/chapter/80');
        """
    )
    connection.commit()
    connection.close()
    return path


def test_audit_is_read_only_and_reports_evidence(tmp_path: Path) -> None:
    database = legacy_database(tmp_path / "legacy.db")
    before = database.read_bytes()
    actions = LegacyRepair(database).audit()
    assert database.read_bytes() == before
    assert {item.category for item in actions} >= {"identity", "metadata", "release"}
    assert any(item.action.startswith("manual split") for item in actions)
    assert any(item.action.startswith("quarantine") for item in actions)


def test_repair_defaults_to_dry_run_and_apply_is_idempotent(tmp_path: Path) -> None:
    database = legacy_database(tmp_path / "legacy.db")
    repair = LegacyRepair(database)
    predicted, backup = repair.repair()
    assert backup is None
    assert not any(item.applied for item in predicted)

    actions, backup = repair.repair(apply=True, backup_dir=tmp_path / "backups")
    assert backup and backup.is_file()
    assert sum(item.applied for item in actions) == 2
    with sqlite3.connect(database) as connection:
        assert (
            connection.execute("SELECT metadata_json FROM source_series WHERE id=1").fetchone()[0]
            == "{}"
        )
        assert (
            connection.execute("SELECT title FROM chapter_release WHERE id=1").fetchone()[0]
            == "Chapter 1"
        )
    second, second_backup = repair.repair(apply=True, backup_dir=tmp_path / "backups")
    assert second_backup and second_backup.is_file()
    assert not any(item.applied for item in second)


def test_cli_does_not_require_postgres_for_legacy_audit(tmp_path: Path, monkeypatch) -> None:
    database = legacy_database(tmp_path / "legacy.db")
    report = tmp_path / "report.json"
    monkeypatch.delenv("V2_DATABASE_URL", raising=False)
    assert main(["audit-legacy", str(database), "--report", str(report)]) == 0
    payload = json.loads(report.read_text())
    assert payload["dry_run"] is True
    assert payload["summary"]["observations"] >= 5


def test_markdown_report(tmp_path: Path) -> None:
    database = legacy_database(tmp_path / "legacy.db")
    repair = LegacyRepair(database)
    report = tmp_path / "report.md"
    write_legacy_report(
        report, database=database, dry_run=True, actions=repair.audit(), manifest=[]
    )
    assert report.read_text().startswith("# Legacy catalog repair report")
