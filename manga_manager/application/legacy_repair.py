from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LATEST_PREFIX = re.compile(r"^\s*Latest:\s*(Chapter\s+.+)$", re.IGNORECASE)
TEMPLATE_CHAPTER = re.compile(r"\{\{\s*(?:number|date)\s*\}\}", re.IGNORECASE)
HELPER_NUMBER = re.compile(r"^(?:first|latest)\s+chapter$", re.IGNORECASE)
URL_CHAPTER_NUMBER = re.compile(r"(?:chapter|ch)[-_/ ]+(\d+)(?:[.-](\d+))?", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class RepairAction:
    key: str
    category: str
    table: str
    row_id: int | None
    evidence: dict[str, Any]
    action: str
    before: Any = None
    after: Any = None
    applied: bool = False
    rollback: str = "restore the pre-repair database backup"


def sqlite_path(database: str | Path) -> Path:
    value = str(database)
    if value.startswith("sqlite:///"):
        value = value.removeprefix("sqlite:///")
    path = Path(value).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    return path


class LegacyRepair:
    """Evidence-first audit and narrowly scoped repair for the legacy SQLite catalog."""

    def __init__(self, database: str | Path, *, storage_root: Path | None = None) -> None:
        self.database = sqlite_path(database)
        self.storage_root = storage_root.resolve() if storage_root else None

    def audit(self) -> list[RepairAction]:
        connection = sqlite3.connect(f"file:{self.database}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            return self._collect(connection)
        finally:
            connection.close()

    def repair(
        self, *, apply: bool = False, backup_dir: Path | None = None
    ) -> tuple[list[RepairAction], Path | None]:
        actions = self.audit()
        if not apply:
            return actions, None
        backup = self._backup(backup_dir or self.database.parent)
        archive_dir = None
        if self.storage_root is not None:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            archive_dir = self.storage_root / "repair-archive" / stamp
        connection = sqlite3.connect(self.database)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        applied: set[str] = set()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for item in actions:
                if item.action == "normalize metadata_json to {}":
                    connection.execute(
                        "UPDATE source_series SET metadata_json='{}' WHERE id=? AND "
                        "(metadata_json IS NULL OR trim(metadata_json)='')",
                        (item.row_id,),
                    )
                    applied.add(item.key)
                elif item.action == "remove Latest: title prefix":
                    connection.execute(
                        "UPDATE chapter_release SET title=? WHERE id=? AND title=?",
                        (item.after, item.row_id, item.before),
                    )
                    applied.add(item.key)
                elif item.action == "split conflicting provider identity":
                    self._split_provider_identity(
                        connection, int(item.evidence["source_series_id"])
                    )
                    applied.add(item.key)
                elif item.action == "consolidate helper release into numeric chapter":
                    self._consolidate_helper_release(
                        connection,
                        int(item.row_id),
                        int(item.evidence["target_release_id"]),
                        int(item.evidence["target_chapter_id"]),
                    )
                    applied.add(item.key)
                elif item.action == "consolidate URL-disagreed release into numeric chapter":
                    self._consolidate_helper_release(
                        connection,
                        int(item.row_id),
                        int(item.evidence["target_release_id"]),
                        int(item.evidence["target_chapter_id"]),
                    )
                    applied.add(item.key)
                elif item.action == "quarantine template-placeholder release":
                    self._quarantine_release(connection, int(item.row_id), archive_dir)
                    applied.add(item.key)
                elif item.action == "delete unverifiable incomplete series":
                    connection.execute("DELETE FROM series WHERE id=?", (item.row_id,))
                    applied.add(item.key)
                elif item.action == "null orphaned activity reference":
                    column = str(item.evidence["column"])
                    if column not in {
                        "series_id",
                        "chapter_id",
                        "download_job_id",
                        "kavita_sync_job_id",
                    }:
                        raise RuntimeError(f"unsupported activity reference {column}")
                    connection.execute(
                        f"UPDATE activity_event SET {column}=NULL WHERE id=?", (item.row_id,)
                    )
                    applied.add(item.key)
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise RuntimeError(f"foreign-key violations remain after repair: {len(violations)}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return [
            RepairAction(**{**asdict(item), "applied": item.key in applied}) for item in actions
        ], backup

    def manifest(self, cache_path: Path | None = None) -> list[dict[str, Any]]:
        if self.storage_root is None or not self.storage_root.exists():
            return []
        cached: dict[str, dict[str, Any]] = {}
        if cache_path is not None and cache_path.is_file():
            try:
                cached = {
                    record["path"]: record
                    for record in json.loads(cache_path.read_text(encoding="utf-8"))
                }
            except (KeyError, TypeError, json.JSONDecodeError):
                cached = {}
        records: list[dict[str, Any]] = []
        for index, path in enumerate(sorted(self.storage_root.rglob("*.cbz")), start=1):
            stat = path.stat()
            relative = path.relative_to(self.storage_root).as_posix()
            record = cached.get(relative)
            if (
                not record
                or record.get("bytes") != stat.st_size
                or record.get("mtime_ns") != stat.st_mtime_ns
            ):
                digest = hashlib.sha256()
                with path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                record = {
                    "path": relative,
                    "bytes": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "sha256": digest.hexdigest(),
                }
            records.append(record)
            if cache_path is not None and index % 50 == 0:
                write_manifest_cache(cache_path, records)
        if cache_path is not None:
            write_manifest_cache(cache_path, records)
        return records

    @staticmethod
    def cleanup_archives(storage_root: Path, *, retain_days: int = 30) -> list[Path]:
        if retain_days < 1:
            raise ValueError("retain_days must be at least 1")
        archive_root = storage_root.resolve() / "repair-archive"
        if not archive_root.is_dir():
            return []
        cutoff = datetime.now(timezone.utc).timestamp() - retain_days * 86_400
        removed: list[Path] = []
        for directory in sorted(archive_root.iterdir()):
            if directory.is_dir() and directory.stat().st_mtime < cutoff:
                shutil.rmtree(directory)
                removed.append(directory)
        return removed

    def _backup(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        destination = directory / f"{self.database.name}.pre-repair-{stamp}"
        source = sqlite3.connect(f"file:{self.database}?mode=ro", uri=True)
        target = sqlite3.connect(destination)
        try:
            source.backup(target)
        finally:
            target.close()
            source.close()
        return destination

    def _collect(self, connection: sqlite3.Connection) -> list[RepairAction]:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        actions: list[RepairAction] = []
        if "source_series" in tables:
            for row in connection.execute(
                "SELECT id, metadata_json FROM source_series "
                "WHERE metadata_json IS NULL OR trim(metadata_json)=''"
            ):
                actions.append(
                    self._action(
                        "empty-metadata",
                        "metadata",
                        "source_series",
                        row["id"],
                        {},
                        "normalize metadata_json to {}",
                        row["metadata_json"],
                        {},
                    )
                )
            duplicates = connection.execute(
                "SELECT series_id, source, count(*) AS total, group_concat(id) AS ids "
                "FROM source_series GROUP BY series_id, source HAVING count(*) > 1"
            )
            for row in duplicates:
                ids = [int(value) for value in row["ids"].split(",")]
                source_columns = {
                    column["name"]
                    for column in connection.execute("PRAGMA table_info(source_series)")
                }
                if not {"source_id", "url"}.issubset(source_columns):
                    actions.append(
                        self._action(
                            "provider-collision",
                            "identity",
                            "series",
                            row["series_id"],
                            {
                                "source": row["source"],
                                "source_series_ids": ids,
                            },
                            "manual split required; canonical group has duplicate provider identities",
                        )
                    )
                    continue
                identities = connection.execute(
                    f"SELECT id, source_id, url FROM source_series WHERE id IN ({','.join('?' for _ in ids)}) ORDER BY id",
                    ids,
                ).fetchall()
                keep = identities[0]
                for identity in identities[1:]:
                    same_identity = normalized_identity(
                        keep["source_id"], keep["url"]
                    ) == normalized_identity(identity["source_id"], identity["url"])
                    action = (
                        "consolidate duplicate provider identity"
                        if same_identity
                        else "split conflicting provider identity"
                    )
                    actions.append(
                        self._action(
                            "provider-collision",
                            "identity",
                            "source_series",
                            identity["id"],
                            {
                                "series_id": row["series_id"],
                                "source": row["source"],
                                "source_series_id": identity["id"],
                                "kept_source_series_id": keep["id"],
                                "source_id": identity["source_id"],
                                "url": identity["url"],
                            },
                            action,
                        )
                    )
        if "chapter_release" in tables:
            release_columns = {
                column["name"]
                for column in connection.execute("PRAGMA table_info(chapter_release)")
            }
            chapter_id_sql = (
                "chapter_id" if "chapter_id" in release_columns else "NULL AS chapter_id"
            )
            source_series_sql = (
                "source_series_id"
                if "source_series_id" in release_columns
                else "NULL AS source_series_id"
            )
            for row in connection.execute(
                f"SELECT id, {chapter_id_sql}, {source_series_sql}, number, title, url FROM chapter_release"
            ):
                title = row["title"] or ""
                match = LATEST_PREFIX.match(title)
                if match:
                    actions.append(
                        self._action(
                            "latest-prefix",
                            "release",
                            "chapter_release",
                            row["id"],
                            {"number": row["number"], "url": row["url"]},
                            "remove Latest: title prefix",
                            title,
                            match.group(1),
                        )
                    )
                if TEMPLATE_CHAPTER.search(title) or TEMPLATE_CHAPTER.search(row["url"] or ""):
                    actions.append(
                        self._action(
                            "template-release",
                            "release",
                            "chapter_release",
                            row["id"],
                            {"title": title, "url": row["url"]},
                            "quarantine template-placeholder release",
                        )
                    )
                if HELPER_NUMBER.match((row["number"] or "").strip()):
                    numeric = evidenced_chapter_number(row["url"] or "", title)
                    target = None
                    if numeric and row["source_series_id"] is not None:
                        target = connection.execute(
                            "SELECT id, chapter_id FROM chapter_release "
                            "WHERE source_series_id=? AND number=? AND id<>? ORDER BY id LIMIT 1",
                            (row["source_series_id"], numeric, row["id"]),
                        ).fetchone()
                    actions.append(
                        self._action(
                            "helper-release",
                            "release",
                            "chapter_release",
                            row["id"],
                            {
                                "number": row["number"],
                                "url": row["url"],
                                "evidenced_number": numeric,
                                "target_release_id": target["id"] if target else None,
                                "target_chapter_id": target["chapter_id"] if target else None,
                            },
                            "consolidate helper release into numeric chapter"
                            if target and target["chapter_id"]
                            else "associate helper release with evidenced numeric chapter",
                        )
                    )
                else:
                    numeric = evidenced_chapter_number(row["url"] or "", "")
                    release_number = normalized_chapter_number(row["number"] or "")
                    if (
                        numeric
                        and release_number
                        and numeric != release_number
                        and row["source_series_id"] is not None
                    ):
                        target = connection.execute(
                            "SELECT id, chapter_id FROM chapter_release "
                            "WHERE source_series_id=? AND number=? AND id<>? ORDER BY id LIMIT 1",
                            (row["source_series_id"], numeric, row["id"]),
                        ).fetchone()
                        if target and target["chapter_id"]:
                            actions.append(
                                self._action(
                                    "url-number-disagreement",
                                    "release",
                                    "chapter_release",
                                    row["id"],
                                    {
                                        "number": row["number"],
                                        "url": row["url"],
                                        "evidenced_number": numeric,
                                        "target_release_id": target["id"],
                                        "target_chapter_id": target["chapter_id"],
                                    },
                                    "consolidate URL-disagreed release into numeric chapter",
                                )
                            )
        if "series" in tables:
            series_columns = {
                column["name"] for column in connection.execute("PRAGMA table_info(series)")
            }
            status_sql = "s.status" if "status" in series_columns else "'new' AS status"
            for row in connection.execute(
                f"SELECT s.id, s.title, {status_sql} FROM series s "
                "LEFT JOIN source_series ss ON ss.series_id=s.id "
                "GROUP BY s.id HAVING count(ss.id)=0"
            ):
                has_chapters = (
                    connection.execute(
                        "SELECT 1 FROM chapter WHERE series_id=? LIMIT 1", (row["id"],)
                    ).fetchone()
                    if "chapter" in tables
                    else None
                )
                deletable = row["status"] in {"new", "untracked"} and not has_chapters
                actions.append(
                    self._action(
                        "sourceless-series",
                        "integrity",
                        "series",
                        row["id"],
                        {"title": row["title"]},
                        "delete unverifiable incomplete series"
                        if deletable
                        else "verify or delete incomplete untracked series",
                    )
                )
        for row in connection.execute("PRAGMA foreign_key_check"):
            foreign_key = connection.execute(f"PRAGMA foreign_key_list({str(row[0])})").fetchall()
            fk = next((item for item in foreign_key if item[0] == row[3]), None)
            column = fk[3] if fk else ""
            nullable_activity = str(row[0]) == "activity_event" and column in {
                "series_id",
                "chapter_id",
                "download_job_id",
                "kavita_sync_job_id",
            }
            actions.append(
                self._action(
                    "foreign-key",
                    "integrity",
                    str(row[0]),
                    int(row[1]) if row[1] is not None else None,
                    {
                        "parent": row[2],
                        "foreign_key_index": row[3],
                        "column": column,
                    },
                    "null orphaned activity reference"
                    if nullable_activity
                    else "repair orphaned reference before enabling foreign keys",
                )
            )
        return sorted(actions, key=lambda item: item.key)

    def _split_provider_identity(
        self, connection: sqlite3.Connection, source_series_id: int
    ) -> None:
        source = connection.execute(
            "SELECT * FROM source_series WHERE id=?", (source_series_id,)
        ).fetchone()
        if source is None:
            return
        original = connection.execute(
            "SELECT * FROM series WHERE id=?", (source["series_id"],)
        ).fetchone()
        if original is None:
            raise RuntimeError(f"series {source['series_id']} is missing")
        series_columns = [
            row["name"]
            for row in connection.execute("PRAGMA table_info(series)")
            if row["name"] != "id"
        ]
        values: dict[str, Any] = {column: original[column] for column in series_columns}
        for column in (
            "title",
            "normalized_title",
            "aliases",
            "description",
            "cover_url",
            "genres",
            "popularity",
            "external_ids",
            "cover_path",
        ):
            if (
                column in series_columns
                and column in source.keys()
                and source[column] not in (None, "")
            ):
                values[column] = source[column]
        columns_sql = ", ".join(series_columns)
        placeholders = ", ".join("?" for _ in series_columns)
        cursor = connection.execute(
            f"INSERT INTO series ({columns_sql}) VALUES ({placeholders})",
            [values[column] for column in series_columns],
        )
        new_series_id = int(cursor.lastrowid)
        connection.execute(
            "UPDATE source_series SET series_id=? WHERE id=?",
            (new_series_id, source_series_id),
        )
        if table_exists(connection, "series_progress"):
            progress = connection.execute(
                "SELECT * FROM series_progress WHERE series_id=?", (source["series_id"],)
            ).fetchone()
            if progress is not None:
                progress_columns = [
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(series_progress)")
                    if row["name"] != "id"
                ]
                progress_values = [
                    new_series_id if column == "series_id" else progress[column]
                    for column in progress_columns
                ]
                connection.execute(
                    f"INSERT OR IGNORE INTO series_progress ({', '.join(progress_columns)}) "
                    f"VALUES ({', '.join('?' for _ in progress_columns)})",
                    progress_values,
                )
        releases = connection.execute(
            "SELECT r.id AS release_id, r.chapter_id, r.number, c.* "
            "FROM chapter_release r LEFT JOIN chapter c ON c.id=r.chapter_id "
            "WHERE r.source_series_id=? ORDER BY r.id",
            (source_series_id,),
        ).fetchall()
        chapter_columns = {row["name"] for row in connection.execute("PRAGMA table_info(chapter)")}
        chapter_map: dict[int, int] = {}
        for release in releases:
            existing = connection.execute(
                "SELECT id FROM chapter WHERE series_id=? AND number=?",
                (new_series_id, release["number"]),
            ).fetchone()
            if existing:
                new_chapter_id = int(existing["id"])
            else:
                copy_columns = [
                    column
                    for column in (
                        "number",
                        "title",
                        "best_source",
                        "downloaded_source",
                        "cbz_path",
                        "created_at",
                        "updated_at",
                    )
                    if column in chapter_columns
                ]
                insert_columns = ["series_id", *copy_columns]
                cursor = connection.execute(
                    f"INSERT INTO chapter ({', '.join(insert_columns)}) VALUES ({', '.join('?' for _ in insert_columns)})",
                    [new_series_id, *[release[column] for column in copy_columns]],
                )
                new_chapter_id = int(cursor.lastrowid)
            if release["chapter_id"] is not None:
                chapter_map[int(release["chapter_id"])] = new_chapter_id
            connection.execute(
                "UPDATE chapter_release SET chapter_id=? WHERE id=?",
                (new_chapter_id, release["release_id"]),
            )
            if table_exists(connection, "downloaded_file"):
                connection.execute(
                    "UPDATE downloaded_file SET chapter_id=? WHERE chapter_release_id=?",
                    (new_chapter_id, release["release_id"]),
                )
        for old_chapter_id, new_chapter_id in chapter_map.items():
            if table_exists(connection, "chapter_progress"):
                progress = connection.execute(
                    "SELECT id FROM chapter_progress WHERE chapter_id=?", (old_chapter_id,)
                ).fetchone()
                target_progress = connection.execute(
                    "SELECT id FROM chapter_progress WHERE chapter_id=?", (new_chapter_id,)
                ).fetchone()
                if progress and not target_progress:
                    connection.execute(
                        "UPDATE chapter_progress SET chapter_id=? WHERE id=?",
                        (new_chapter_id, progress["id"]),
                    )
            remaining = connection.execute(
                "SELECT 1 FROM chapter_release WHERE chapter_id=? LIMIT 1", (old_chapter_id,)
            ).fetchone()
            files = (
                connection.execute(
                    "SELECT 1 FROM downloaded_file WHERE chapter_id=? LIMIT 1", (old_chapter_id,)
                ).fetchone()
                if table_exists(connection, "downloaded_file")
                else None
            )
            if not remaining and not files:
                if table_exists(connection, "activity_event"):
                    connection.execute(
                        "UPDATE activity_event SET chapter_id=NULL WHERE chapter_id=?",
                        (old_chapter_id,),
                    )
                connection.execute("DELETE FROM chapter WHERE id=?", (old_chapter_id,))

    def _consolidate_helper_release(
        self,
        connection: sqlite3.Connection,
        helper_release_id: int,
        target_release_id: int,
        target_chapter_id: int,
    ) -> None:
        helper = connection.execute(
            "SELECT chapter_id FROM chapter_release WHERE id=?", (helper_release_id,)
        ).fetchone()
        if helper is None:
            return
        if table_exists(connection, "downloaded_file"):
            connection.execute(
                "UPDATE downloaded_file SET active=0, replaced_at=CURRENT_TIMESTAMP "
                "WHERE chapter_release_id=? AND active=1",
                (target_release_id,),
            )
            connection.execute(
                "UPDATE downloaded_file SET chapter_id=?, chapter_release_id=? "
                "WHERE chapter_release_id=?",
                (target_chapter_id, target_release_id, helper_release_id),
            )
        if table_exists(connection, "download_job"):
            helper_jobs = connection.execute(
                "SELECT id FROM download_job WHERE chapter_release_id=?",
                (helper_release_id,),
            ).fetchall()
            if table_exists(connection, "activity_event"):
                for helper_job in helper_jobs:
                    connection.execute(
                        "UPDATE activity_event SET download_job_id=NULL WHERE download_job_id=?",
                        (helper_job["id"],),
                    )
            target_job = connection.execute(
                "SELECT id FROM download_job WHERE chapter_release_id=?", (target_release_id,)
            ).fetchone()
            if target_job:
                connection.execute(
                    "DELETE FROM download_job WHERE chapter_release_id=?", (helper_release_id,)
                )
            else:
                connection.execute(
                    "UPDATE download_job SET chapter_release_id=? WHERE chapter_release_id=?",
                    (target_release_id, helper_release_id),
                )
        if table_exists(connection, "chapter_fingerprint"):
            connection.execute(
                "DELETE FROM chapter_fingerprint WHERE chapter_release_id=?", (helper_release_id,)
            )
        connection.execute("DELETE FROM chapter_release WHERE id=?", (helper_release_id,))
        old_chapter_id = helper["chapter_id"]
        if old_chapter_id and old_chapter_id != target_chapter_id:
            if table_exists(connection, "activity_event"):
                connection.execute(
                    "UPDATE activity_event SET chapter_id=? WHERE chapter_id=?",
                    (target_chapter_id, old_chapter_id),
                )
            remaining = connection.execute(
                "SELECT 1 FROM chapter_release WHERE chapter_id=? LIMIT 1", (old_chapter_id,)
            ).fetchone()
            if not remaining:
                if table_exists(connection, "chapter_progress"):
                    connection.execute(
                        "DELETE FROM chapter_progress WHERE chapter_id=?", (old_chapter_id,)
                    )
                connection.execute("DELETE FROM chapter WHERE id=?", (old_chapter_id,))

    def _quarantine_release(
        self,
        connection: sqlite3.Connection,
        release_id: int,
        archive_dir: Path | None,
    ) -> None:
        release_columns = {
            column["name"] for column in connection.execute("PRAGMA table_info(chapter_release)")
        }
        chapter_sql = "chapter_id" if "chapter_id" in release_columns else "NULL AS chapter_id"
        release = connection.execute(
            f"SELECT {chapter_sql} FROM chapter_release WHERE id=?", (release_id,)
        ).fetchone()
        if release is None:
            return
        if table_exists(connection, "downloaded_file"):
            files = connection.execute(
                "SELECT id, path FROM downloaded_file WHERE chapter_release_id=?", (release_id,)
            ).fetchall()
            for record in files:
                source = self._storage_path(record["path"])
                if source is not None and source.is_file() and archive_dir is not None:
                    relative = source.relative_to(self.storage_root)
                    destination = archive_dir / "rejected" / relative
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(source, destination)
            connection.execute(
                "DELETE FROM downloaded_file WHERE chapter_release_id=?", (release_id,)
            )
        if table_exists(connection, "activity_event") and table_exists(connection, "download_job"):
            job_ids = connection.execute(
                "SELECT id FROM download_job WHERE chapter_release_id=?", (release_id,)
            ).fetchall()
            for job in job_ids:
                connection.execute(
                    "UPDATE activity_event SET download_job_id=NULL WHERE download_job_id=?",
                    (job["id"],),
                )
        for table in ("chapter_fingerprint", "download_job"):
            if table_exists(connection, table):
                connection.execute(f"DELETE FROM {table} WHERE chapter_release_id=?", (release_id,))
        connection.execute("DELETE FROM chapter_release WHERE id=?", (release_id,))
        chapter_id = release["chapter_id"]
        if chapter_id:
            remaining = connection.execute(
                "SELECT 1 FROM chapter_release WHERE chapter_id=? LIMIT 1", (chapter_id,)
            ).fetchone()
            if not remaining:
                if table_exists(connection, "activity_event"):
                    connection.execute(
                        "UPDATE activity_event SET chapter_id=NULL WHERE chapter_id=?",
                        (chapter_id,),
                    )
                if table_exists(connection, "chapter_progress"):
                    connection.execute(
                        "DELETE FROM chapter_progress WHERE chapter_id=?", (chapter_id,)
                    )
                connection.execute("DELETE FROM chapter WHERE id=?", (chapter_id,))

    def _storage_path(self, value: str) -> Path | None:
        if self.storage_root is None or not value:
            return None
        candidate = Path(value)
        candidate = (
            candidate.resolve()
            if candidate.is_absolute()
            else (self.storage_root / candidate).resolve()
        )
        try:
            candidate.relative_to(self.storage_root)
        except ValueError:
            return None
        return candidate

    @staticmethod
    def _action(
        prefix: str,
        category: str,
        table: str,
        row_id: int | None,
        evidence: dict[str, Any],
        action: str,
        before: Any = None,
        after: Any = None,
    ) -> RepairAction:
        raw = json.dumps([prefix, table, row_id, evidence], sort_keys=True, default=str)
        key = f"{prefix}:{hashlib.sha256(raw.encode()).hexdigest()[:16]}"
        return RepairAction(key, category, table, row_id, evidence, action, before, after)


def write_legacy_report(
    path: Path,
    *,
    database: Path,
    dry_run: bool,
    actions: list[RepairAction],
    manifest: list[dict[str, Any]],
    backup: Path | None = None,
) -> None:
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database": str(database),
        "dry_run": dry_run,
        "backup": str(backup) if backup else None,
        "summary": {
            "observations": len(actions),
            "applied": sum(item.applied for item in actions),
            "storage_files": len(manifest),
        },
        "actions": [asdict(item) for item in actions],
        "storage_manifest": manifest,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() in {".md", ".markdown"}:
        lines = [
            "# Legacy catalog repair report",
            "",
            f"- Database: `{database}`",
            f"- Dry run: `{str(dry_run).lower()}`",
            f"- Observations: {len(actions)}",
            f"- Applied: {payload['summary']['applied']}",
            "",
            "## Actions",
            "",
        ]
        lines.extend(
            f"- `{item.key}` — {item.table}#{item.row_id}: {item.action}" for item in actions
        )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    else:
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
        )


def table_exists(connection: sqlite3.Connection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def normalized_identity(source_id: str | None, url: str | None) -> str:
    value = (source_id or "").strip().lower().rstrip("/")
    if value:
        return value
    cleaned_url = re.sub(r"^https?://(?:www\.)?", "", (url or "").strip().lower())
    return cleaned_url.split("?", 1)[0].rstrip("/")


def evidenced_chapter_number(url: str, title: str) -> str | None:
    match = URL_CHAPTER_NUMBER.search(url)
    if match:
        return f"{match.group(1)}.{match.group(2)}" if match.group(2) else match.group(1)
    title_match = re.search(r"\bchapter\s+(\d+(?:\.\d+)?)\b", title, re.IGNORECASE)
    if title_match:
        return (
            title_match.group(1).rstrip("0").rstrip(".")
            if "." in title_match.group(1)
            else title_match.group(1)
        )
    return None


def normalized_chapter_number(value: str) -> str | None:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*", value)
    if not match:
        return None
    number = match.group(1)
    return number.rstrip("0").rstrip(".") if "." in number else number


def write_manifest_cache(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)
