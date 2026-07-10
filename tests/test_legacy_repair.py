from __future__ import annotations

import json
import os
import sqlite3
import time
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
    assert sum(item.applied for item in actions) == 4
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


def full_legacy_database(path: Path) -> Path:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE series (
          id INTEGER PRIMARY KEY, title TEXT NOT NULL, normalized_title TEXT NOT NULL,
          aliases TEXT NOT NULL DEFAULT '', description TEXT NOT NULL DEFAULT '',
          cover_url TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'new'
        );
        CREATE TABLE source_series (
          id INTEGER PRIMARY KEY, series_id INTEGER NOT NULL REFERENCES series(id),
          source TEXT NOT NULL, source_id TEXT NOT NULL, title TEXT NOT NULL,
          normalized_title TEXT NOT NULL, url TEXT NOT NULL, aliases TEXT NOT NULL DEFAULT '',
          description TEXT NOT NULL DEFAULT '', cover_url TEXT NOT NULL DEFAULT '',
          metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE chapter (
          id INTEGER PRIMARY KEY, series_id INTEGER NOT NULL REFERENCES series(id),
          number TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', best_source TEXT NOT NULL DEFAULT '',
          downloaded_source TEXT NOT NULL DEFAULT '', cbz_path TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE chapter_release (
          id INTEGER PRIMARY KEY, chapter_id INTEGER REFERENCES chapter(id),
          source_series_id INTEGER NOT NULL REFERENCES source_series(id), source TEXT NOT NULL,
          number TEXT NOT NULL, title TEXT NOT NULL DEFAULT '', url TEXT NOT NULL
        );
        CREATE TABLE downloaded_file (
          id INTEGER PRIMARY KEY, chapter_id INTEGER NOT NULL REFERENCES chapter(id),
          chapter_release_id INTEGER NOT NULL REFERENCES chapter_release(id), source TEXT NOT NULL,
          path TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, replaced_at TEXT
        );
        CREATE TABLE download_job (
          id INTEGER PRIMARY KEY, chapter_release_id INTEGER NOT NULL UNIQUE REFERENCES chapter_release(id),
          status TEXT NOT NULL DEFAULT 'queued'
        );
        INSERT INTO series VALUES (1, 'Merged', 'merged', '', '', '', 'new');
        INSERT INTO source_series VALUES
          (1, 1, 'asura', 'first', 'First', 'first', 'https://asura.test/comics/first', '', '', '', '{}'),
          (2, 1, 'asura', 'second', 'Second', 'second', 'https://asura.test/comics/second', '', '', '', '{}');
        INSERT INTO chapter VALUES (1, 1, '1', 'Chapter 1', 'asura', '', '');
        INSERT INTO chapter_release VALUES
          (1, 1, 1, 'asura', '1', 'Chapter 1', 'https://asura.test/comics/first/chapter/1'),
          (2, 1, 2, 'asura', '1', 'Chapter 1', 'https://asura.test/comics/second/chapter/1');
        INSERT INTO downloaded_file VALUES (1, 1, 2, 'asura', 'second.cbz', 1, NULL);
        """
    )
    connection.commit()
    connection.close()
    return path


def test_repair_splits_conflicting_provider_identity_and_reassigns_artifact(
    tmp_path: Path,
) -> None:
    database = full_legacy_database(tmp_path / "legacy.db")
    actions, _ = LegacyRepair(database).repair(apply=True, backup_dir=tmp_path / "backups")
    assert any(
        item.action == "split conflicting provider identity" and item.applied for item in actions
    )
    with sqlite3.connect(database) as connection:
        first_series = connection.execute(
            "SELECT series_id FROM source_series WHERE id=1"
        ).fetchone()[0]
        second_series = connection.execute(
            "SELECT series_id FROM source_series WHERE id=2"
        ).fetchone()[0]
        assert first_series != second_series
        chapter_series, file_chapter = connection.execute(
            "SELECT c.series_id, d.chapter_id FROM downloaded_file d JOIN chapter c ON c.id=d.chapter_id WHERE d.id=1"
        ).fetchone()
        assert chapter_series == second_series
        assert file_chapter != 1


def test_repair_consolidates_evidenced_helper_release(tmp_path: Path) -> None:
    database = full_legacy_database(tmp_path / "legacy.db")
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO chapter VALUES (2, 1, 'first chapter', 'First', 'asura', 'asura', 'helper.cbz')"
        )
        connection.execute(
            "INSERT INTO chapter_release VALUES (3, 2, 1, 'asura', 'first chapter', 'Chapter 1', 'https://asura.test/comics/first/chapter/1')"
        )
        connection.execute(
            "INSERT INTO downloaded_file VALUES (2, 2, 3, 'asura', 'helper.cbz', 1, NULL)"
        )
        connection.commit()
    actions, _ = LegacyRepair(database).repair(apply=True, backup_dir=tmp_path / "backups")
    assert any(
        item.action == "consolidate helper release into numeric chapter" and item.applied
        for item in actions
    )
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT 1 FROM chapter_release WHERE id=3").fetchone() is None
        chapter_id, release_id, active = connection.execute(
            "SELECT chapter_id, chapter_release_id, active FROM downloaded_file WHERE id=2"
        ).fetchone()
        assert (chapter_id, release_id, active) == (1, 1, 1)


def test_template_release_is_moved_to_repair_archive(tmp_path: Path) -> None:
    database = full_legacy_database(tmp_path / "legacy.db")
    storage = tmp_path / "storage"
    artifact = storage / "Manga" / "bad.cbz"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"unverifiable")
    with sqlite3.connect(database) as connection:
        connection.execute(
            "INSERT INTO chapter VALUES (2, 1, '80', 'Bad', 'asura', 'asura', 'Manga/bad.cbz')"
        )
        connection.execute(
            "INSERT INTO chapter_release VALUES (3, 2, 1, 'asura', '80', 'Chapter {{number}} {{date}}', 'https://asura.test/bad')"
        )
        connection.execute(
            "INSERT INTO downloaded_file VALUES (2, 2, 3, 'asura', 'Manga/bad.cbz', 1, NULL)"
        )
        connection.commit()
    actions, _ = LegacyRepair(database, storage_root=storage).repair(
        apply=True, backup_dir=tmp_path / "backups"
    )
    assert any(
        item.action == "quarantine template-placeholder release" and item.applied
        for item in actions
    )
    assert not artifact.exists()
    archived = list((storage / "repair-archive").rglob("bad.cbz"))
    assert len(archived) == 1
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT 1 FROM chapter_release WHERE id=3").fetchone() is None
        assert connection.execute("SELECT 1 FROM downloaded_file WHERE id=2").fetchone() is None


def test_repair_archive_cleanup_preserves_thirty_day_window(tmp_path: Path) -> None:
    archive_root = tmp_path / "storage" / "repair-archive"
    old = archive_root / "old"
    recent = archive_root / "recent"
    old.mkdir(parents=True)
    recent.mkdir()
    old_time = time.time() - 31 * 86_400
    os.utime(old, (old_time, old_time))
    removed = LegacyRepair.cleanup_archives(tmp_path / "storage", retain_days=30)
    assert removed == [old]
    assert not old.exists()
    assert recent.exists()
