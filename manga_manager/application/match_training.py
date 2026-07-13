from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

from sqlalchemy import select

from manga_manager.infrastructure.db_models import (
    CatalogCoverAsset,
    CatalogMatchDecision,
    CatalogSourceSeries,
    MatchTrainingLabel,
)
from manga_manager.worker.runtime import SessionFactory


def record_training_label(
    session,
    *,
    left_source_series_id: int,
    right_source_series_id: int,
    label: int,
    origin: str,
    decision: CatalogMatchDecision | None = None,
) -> MatchTrainingLabel | None:
    if decision is not None:
        existing = session.scalar(
            select(MatchTrainingLabel).where(
                MatchTrainingLabel.original_decision_id == decision.id,
                MatchTrainingLabel.label == label,
            )
        )
        if existing is not None:
            return existing
    left = session.get(CatalogSourceSeries, left_source_series_id)
    right = session.get(CatalogSourceSeries, right_source_series_id)
    if left is None or right is None:
        return None

    def snapshot(identity: CatalogSourceSeries) -> dict:
        asset = session.get(CatalogCoverAsset, identity.id)
        return {
            "id": identity.id,
            "series_id": identity.series_id,
            "source": identity.source,
            "source_id": identity.source_id,
            "title": identity.title,
            "url": identity.url,
            "cover_url": identity.cover_url,
            "cover_checksum": asset.content_checksum if asset else "",
            "cover_relative_path": asset.relative_path if asset else "",
        }

    row = MatchTrainingLabel(
        original_decision_id=decision.id if decision is not None else None,
        label=label,
        origin=origin,
        scorer_version=decision.scorer_version if decision is not None else "manual-v1",
        feature_vector_json=decision.feature_vector_json if decision is not None else {},
        evidence_json=decision.evidence_json if decision is not None else {},
        left_identity_json=snapshot(left),
        right_identity_json=snapshot(right),
    )
    session.add(row)
    return row


def export_training_data(
    session_factory: SessionFactory, storage_root: Path, output: Path
) -> int:
    output.mkdir(parents=True, exist_ok=True)
    covers = output / "covers"
    covers.mkdir(exist_ok=True)
    with session_factory() as session:
        labels = session.scalars(
            select(MatchTrainingLabel).order_by(MatchTrainingLabel.id)
        ).all()
        records = []
        for label in labels:
            image_paths = {}
            for side, identity in (
                ("left", label.left_identity_json),
                ("right", label.right_identity_json),
            ):
                relative = str(identity.get("cover_relative_path") or "")
                source = storage_root / relative
                if not relative or not source.is_file():
                    continue
                checksum = str(identity.get("cover_checksum") or source.stem)
                destination = covers / f"{checksum}{source.suffix}"
                if not destination.exists():
                    try:
                        os.link(source, destination)
                    except OSError:
                        shutil.copy2(source, destination)
                image_paths[side] = destination.relative_to(output).as_posix()
            records.append(
                {
                    "label_id": label.id,
                    "original_decision_id": label.original_decision_id,
                    "label": label.label,
                    "origin": label.origin,
                    "scorer_version": label.scorer_version,
                    "features": label.feature_vector_json,
                    "evidence": label.evidence_json,
                    "left": label.left_identity_json,
                    "right": label.right_identity_json,
                    "images": image_paths,
                }
            )
    manifest = output / "matches.jsonl"
    manifest.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    return len(records)
