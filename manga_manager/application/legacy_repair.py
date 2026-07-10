from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LATEST_PREFIX = re.compile(r"^\s*Latest:\s*(Chapter\s+.+)$", re.IGNORECASE)
TEMPLATE_CHAPTER = re.compile(r"\{\{\s*(?:number|date)\s*\}\}", re.IGNORECASE)
HELPER_NUMBER = re.compile(r"^(?:first|latest)\s+chapter$", re.IGNORECASE)


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

    def manifest(self) -> list[dict[str, Any]]:
        if self.storage_root is None or not self.storage_root.exists():
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(self.storage_root.rglob("*.cbz")):
            digest = hashlib.sha256()
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            records.append(
                {
                    "path": path.relative_to(self.storage_root).as_posix(),
                    "bytes": path.stat().st_size,
                    "sha256": digest.hexdigest(),
                }
            )
        return records

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
                actions.append(
                    self._action(
                        "provider-collision",
                        "identity",
                        "series",
                        row["series_id"],
                        {"source": row["source"], "source_series_ids": row["ids"].split(",")},
                        "manual split required; canonical group has duplicate provider identities",
                    )
                )
        if "chapter_release" in tables:
            for row in connection.execute("SELECT id, number, title, url FROM chapter_release"):
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
                    actions.append(
                        self._action(
                            "helper-release",
                            "release",
                            "chapter_release",
                            row["id"],
                            {"number": row["number"], "url": row["url"]},
                            "associate helper release with evidenced numeric chapter",
                        )
                    )
        if "series" in tables:
            for row in connection.execute(
                "SELECT s.id, s.title FROM series s LEFT JOIN source_series ss ON ss.series_id=s.id "
                "GROUP BY s.id HAVING count(ss.id)=0"
            ):
                actions.append(
                    self._action(
                        "sourceless-series",
                        "integrity",
                        "series",
                        row["id"],
                        {"title": row["title"]},
                        "verify or delete incomplete untracked series",
                    )
                )
        for row in connection.execute("PRAGMA foreign_key_check"):
            actions.append(
                self._action(
                    "foreign-key",
                    "integrity",
                    str(row[0]),
                    int(row[1]) if row[1] is not None else None,
                    {"parent": row[2], "foreign_key_index": row[3]},
                    "repair orphaned reference before enabling foreign keys",
                )
            )
        return sorted(actions, key=lambda item: item.key)

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
