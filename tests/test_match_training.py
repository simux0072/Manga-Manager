from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from manga_manager.application.match_training import export_training_data, record_training_label
from manga_manager.infrastructure.db_models import (
    CatalogCoverAsset,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSourceSeries,
    JobBase,
    MatchTrainingLabel,
)


def test_review_label_snapshots_identities_and_exports_cached_covers(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    cover = tmp_path / "covers" / "aa" / "cover.jpg"
    cover.parent.mkdir(parents=True)
    cover.write_bytes(b"cached-cover")
    with sessions() as session, session.begin():
        left_series = CatalogSeries(title="Left", normalized_title="left")
        right_series = CatalogSeries(title="Right", normalized_title="right")
        session.add_all([left_series, right_series])
        session.flush()
        left = CatalogSourceSeries(
            series_id=left_series.id,
            source="asura",
            source_id="left",
            title="Left",
            normalized_title="left",
            url="https://asurascans.com/comics/left",
        )
        right = CatalogSourceSeries(
            series_id=right_series.id,
            source="mangafire",
            source_id="right",
            title="Right",
            normalized_title="right",
            url="https://mangafire.to/manga/right",
        )
        session.add_all([left, right])
        session.flush()
        session.add(
            CatalogCoverAsset(
                source_series_id=left.id,
                content_checksum="a" * 64,
                relative_path=cover.relative_to(tmp_path).as_posix(),
            )
        )
        decision = CatalogMatchDecision(
            left_source_series_id=left.id,
            right_source_series_id=right.id,
            confidence=0.9,
            scorer_version="orb-test",
            feature_vector_json={"cover_inliers": 30},
        )
        session.add(decision)
        session.flush()
        record_training_label(
            session,
            left_source_series_id=left.id,
            right_source_series_id=right.id,
            label=1,
            origin="test",
            decision=decision,
        )
    output = tmp_path / "training"

    assert export_training_data(sessions, tmp_path, output) == 1
    record = json.loads((output / "matches.jsonl").read_text().strip())
    assert record["label"] == 1
    assert record["features"] == {"cover_inliers": 30}
    assert (output / record["images"]["left"]).read_bytes() == b"cached-cover"
    with sessions() as session:
        assert session.scalar(select(MatchTrainingLabel)).left_identity_json["title"] == "Left"
