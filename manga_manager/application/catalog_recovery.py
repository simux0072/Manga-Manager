from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

from sqlalchemy import func, select

from manga_manager.application.download_plans import DownloadPlanCoordinator
from manga_manager.application.library_repair import enqueue_library_repair
from manga_manager.domain.catalog import normalize_title
from manga_manager.domain.jobs import (
    JobKind,
    SourcePullPayload,
    SourceRefreshPayload,
)
from manga_manager.domain.providers import provider_names
from manga_manager.infrastructure.db_models import (
    CatalogMatchDecision,
    CatalogObservation,
    CatalogChapter,
    CatalogSeries,
    CatalogSourceSeries,
    ChapterArtifact,
    KavitaProjection,
)
from manga_manager.infrastructure.job_queue import JobQueue
from manga_manager.worker.runtime import SessionFactory


@dataclass(frozen=True, slots=True)
class RecoveryRecord:
    legacy_id: int
    title: str
    legacy_status: str
    action: str
    series_ids: tuple[int, ...] = ()
    evidence: str = ""


class CatalogRecovery:
    def __init__(self, session_factory: SessionFactory, queue: JobQueue | None = None) -> None:
        self.session_factory = session_factory
        self.queue = queue or JobQueue()

    def run(self, legacy_database: Path, *, apply: bool) -> list[RecoveryRecord]:
        legacy = self._legacy_rows(legacy_database)
        records: list[RecoveryRecord] = []
        with self.session_factory() as session:
            for row in legacy:
                matches, evidence = self._matches(session, row)
                if len(matches) == 1:
                    action = (
                        "track"
                        if self._needs_tracking_or_repair(session, matches[0])
                        else "verified"
                    )
                elif len(matches) > 1:
                    action = "review_merge"
                else:
                    action = "create_track"
                records.append(
                    RecoveryRecord(
                        legacy_id=row["id"],
                        title=row["title"],
                        legacy_status=row["status"],
                        action=action,
                        series_ids=tuple(series.id for series in matches),
                        evidence=evidence,
                    )
                )
        if apply:
            self._apply(records, legacy)
        return records

    @staticmethod
    def _legacy_rows(path: Path) -> list[dict]:
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        try:
            series_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(series)").fetchall()
            }
            status_expression = "s.status" if "status" in series_columns else "'interested'"
            rows = connection.execute(
                f"""
                SELECT DISTINCT s.id, s.title, {status_expression} AS status,
                                ss.source, ss.source_id
                FROM series s
                JOIN chapter c ON c.series_id=s.id
                JOIN downloaded_file df ON df.chapter_id=c.id AND df.active=1
                JOIN chapter_release cr ON cr.id=df.chapter_release_id
                JOIN source_series ss ON ss.id=cr.source_series_id
                ORDER BY s.id, ss.source
                """
            ).fetchall()
        finally:
            connection.close()
        grouped: dict[int, dict] = {}
        for row in rows:
            item = grouped.setdefault(
                int(row["id"]),
                {
                    "id": int(row["id"]),
                    "title": str(row["title"]),
                    "status": str(row["status"]),
                    "identities": [],
                },
            )
            item["identities"].append((str(row["source"]), str(row["source_id"])))
        return list(grouped.values())

    @staticmethod
    def _matches(session, row: dict) -> tuple[list[CatalogSeries], str]:
        ids = set()
        for source, source_id in row["identities"]:
            identity = session.scalar(
                select(CatalogSourceSeries).where(
                    CatalogSourceSeries.source == source,
                    CatalogSourceSeries.source_id == source_id,
                )
            )
            if identity is not None:
                ids.add(identity.series_id)
        if ids:
            return list(session.scalars(select(CatalogSeries).where(CatalogSeries.id.in_(ids)))), "provider_identity"
        normalized = normalize_title(row["title"])
        matches = session.scalars(
            select(CatalogSeries).where(CatalogSeries.normalized_title == normalized)
        ).all()
        return matches, "normalized_title" if matches else "no_current_identity"

    @staticmethod
    def _needs_tracking_or_repair(session, series: CatalogSeries) -> bool:
        if series.status not in {"interested", "reading", "caught_up", "paused"}:
            return True
        active = int(
            session.scalar(
                select(func.count())
                .select_from(ChapterArtifact)
                .join(CatalogChapter, CatalogChapter.id == ChapterArtifact.chapter_id)
                .where(
                    CatalogChapter.series_id == series.id,
                    ChapterArtifact.state == "active",
                )
            )
            or 0
        )
        projected = int(
            session.scalar(
                select(func.count())
                .select_from(KavitaProjection)
                .join(CatalogChapter, CatalogChapter.id == KavitaProjection.chapter_id)
                .where(CatalogChapter.series_id == series.id)
            )
            or 0
        )
        return active != projected

    def _apply(self, records: list[RecoveryRecord], legacy: list[dict]) -> None:
        by_id = {row["id"]: row for row in legacy}
        with self.session_factory() as session, session.begin():
            coordinator = DownloadPlanCoordinator(self.queue)
            refresh_sources: set[str] = set()
            recovered_series_ids: set[int] = set()
            for record in records:
                if record.action == "track":
                    series = session.get(CatalogSeries, record.series_ids[0])
                    if series is None:
                        continue
                    series.status = "reading" if record.legacy_status == "reading" else "interested"
                    coordinator.track(session, series.id)
                    recovered_series_ids.add(series.id)
                    enqueue_library_repair(
                        session,
                        series_id=series.id,
                        reason="legacy_recovery",
                        priority=85,
                        queue=self.queue,
                    )
                elif record.action == "review_merge":
                    for series_id in record.series_ids:
                        series = session.get(CatalogSeries, series_id)
                        if series is None:
                            continue
                        if not self._needs_tracking_or_repair(session, series):
                            continue
                        series.status = (
                            "reading" if record.legacy_status == "reading" else "interested"
                        )
                        coordinator.track(session, series.id)
                        recovered_series_ids.add(series.id)
                        enqueue_library_repair(
                            session,
                            series_id=series.id,
                            reason="legacy_recovery",
                            priority=85,
                            queue=self.queue,
                        )
                    self._create_review_pairs(session, record)
                elif record.action == "create_track":
                    legacy_row = by_id[record.legacy_id]
                    series = CatalogSeries(
                        title=record.title,
                        normalized_title=normalize_title(record.title),
                        status=(
                            "reading" if record.legacy_status == "reading" else "interested"
                        ),
                        integrity_state="attention",
                    )
                    session.add(series)
                    session.flush()
                    for source, source_id in legacy_row["identities"]:
                        if source not in set(provider_names()):
                            continue
                        session.add(
                            CatalogSourceSeries(
                                series_id=series.id,
                                source=source,
                                source_id=source_id,
                                title=record.title,
                                normalized_title=normalize_title(record.title),
                                url="",
                            )
                        )
                        refresh_sources.add(source)
                    coordinator.track(session, series.id)
                    recovered_series_ids.add(series.id)
                    enqueue_library_repair(
                        session,
                        series_id=series.id,
                        reason="legacy_recovery",
                        priority=85,
                        queue=self.queue,
                    )
                    session.add(
                        CatalogObservation(
                            source="legacy",
                            observation_type="legacy_tracking_recreated",
                            source_key=str(record.legacy_id),
                            state="accepted",
                            reason="downloaded legacy series recreated for provider refresh",
                            payload_json={**asdict(record), "resulting_series_id": series.id},
                        )
                    )
            for source in sorted(refresh_sources & set(provider_names())):
                self.queue.enqueue(
                    session,
                    kind=JobKind.SOURCE_PULL,
                    dedupe_key=f"source:{source}",
                    payload=SourcePullPayload(source=source),
                    priority=70,
                    source=source,
                )
            for identity in session.scalars(
                select(CatalogSourceSeries).where(
                    CatalogSourceSeries.series_id.in_(recovered_series_ids)
                )
            ):
                if (
                    identity.source not in set(provider_names())
                    or not identity.url
                ):
                    continue
                self.queue.enqueue(
                    session,
                    kind=JobKind.SOURCE_REFRESH,
                    dedupe_key=f"refresh:{identity.source}:{identity.source_id}",
                    payload=SourceRefreshPayload(
                        source=identity.source,
                        source_id=identity.source_id,
                        title=identity.title or "Unknown manga",
                        url=identity.url,
                        description=identity.description or "",
                        cover_url=identity.cover_url or "",
                        popularity=identity.popularity or 0,
                        metadata=dict(identity.metadata_json or {}),
                    ),
                    priority=60,
                    source=identity.source,
                    max_attempts=4,
                )

    @staticmethod
    def _create_review_pairs(session, record: RecoveryRecord) -> None:
        identities = {
            series_id: session.scalars(
                select(CatalogSourceSeries).where(CatalogSourceSeries.series_id == series_id)
            ).all()
            for series_id in record.series_ids
        }
        ids = list(record.series_ids)
        for index, left_series_id in enumerate(ids):
            for right_series_id in ids[index + 1 :]:
                pair = next(
                    (
                        (left, right)
                        for left in identities[left_series_id]
                        for right in identities[right_series_id]
                        if left.source != right.source
                    ),
                    None,
                )
                if pair is None:
                    continue
                left_id, right_id = sorted((pair[0].id, pair[1].id))
                existing = session.scalar(
                    select(CatalogMatchDecision).where(
                        CatalogMatchDecision.left_source_series_id == left_id,
                        CatalogMatchDecision.right_source_series_id == right_id,
                    )
                )
                if existing is None:
                    session.add(
                        CatalogMatchDecision(
                            left_source_series_id=left_id,
                            right_source_series_id=right_id,
                            confidence=0.99,
                            evidence_json={
                                "legacy_download_group": record.legacy_id,
                                "policy": "manual_review_required",
                            },
                            scorer_version="legacy-recovery-v1",
                            feature_vector_json={"legacy_download_group": 1},
                        )
                    )


def write_recovery_report(path: Path, records: list[RecoveryRecord], *, applied: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"applied": applied, "records": [asdict(record) for record in records]}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    counts: dict[str, int] = {}
    for record in records:
        counts[record.action] = counts.get(record.action, 0) + 1
    markdown = ["# Catalog Recovery Report", "", f"Applied: `{str(applied).lower()}`", ""]
    markdown.extend(f"- {key}: {value}" for key, value in sorted(counts.items()))
    markdown.extend(["", "| Legacy title | Action | Resulting series |", "|---|---|---|"])
    for record in records:
        title = record.title.replace("|", "\\|")
        resulting = ", ".join(map(str, record.series_ids)) or "unresolved"
        markdown.append(f"| {title} | {record.action} | {resulting} |")
    path.with_suffix(".md").write_text("\n".join(markdown) + "\n", encoding="utf-8")
