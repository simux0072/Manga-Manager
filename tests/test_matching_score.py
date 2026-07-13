from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from manga_manager.application import matching_score
from manga_manager.application.matching_score import score_series_pair
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogCoverSignature,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    JobBase,
)


def make_series(session: Session, title: str, source: str, *, description: str = ""):
    series = CatalogSeries(
        title=title, normalized_title=title.casefold(), description=description,
    )
    session.add(series)
    session.flush()
    identity = CatalogSourceSeries(
        series_id=series.id, source=source, source_id=f"{source}-{series.id}",
        title=title, normalized_title=title.casefold(), url=f"https://example/{series.id}",
    )
    session.add(identity)
    session.flush()
    return series, identity


def test_polluted_provider_aliases_cannot_create_a_perfect_match() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        left, left_identity = make_series(session, "Alpha", "asura")
        right, right_identity = make_series(session, "Beta", "mangafire")
        session.add_all([
            CatalogSeriesAlias(
                series_id=left.id, source_series_id=left_identity.id,
                display_value="Asura Scans Home", normalized_value="asura scans home",
            ),
            CatalogSeriesAlias(
                series_id=right.id, source_series_id=right_identity.id,
                display_value="Asura Scans Home", normalized_value="asura scans home",
            ),
        ])
        score = score_series_pair(session, left.id, right.id)
        assert score["title"] < 0.3
        assert score["score"] < 0.11


def test_noble_lady_style_cover_description_and_chapters_receive_strong_floor(
    monkeypatch,
) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    description = "A noblewoman reforms herself while challenging a corrupt aristocratic world."
    with Session(engine) as session, session.begin():
        left, left_identity = make_series(
            session, "Noble Lady Reformation Guide", "asura", description=description
        )
        right, right_identity = make_series(
            session, "There Are No Bad Young Ladies in This World", "kingofshojo",
            description=description,
        )
        for series in (left, right):
            for number in ("1", "2", "3"):
                session.add(CatalogChapter(
                    series_id=series.id, canonical_number=number, display_number=number,
                ))
        for identity in (left_identity, right_identity):
            session.add(CatalogCoverSignature(
                source_series_id=identity.id, algorithm_version="test",
                feature_json={"hashes": ["0" * 16]}, keypoints_blob=b"", descriptors_blob=b"",
            ))
        monkeypatch.setattr(
            matching_score,
            "compare_signatures",
            lambda _left, _right: {
                "cover_compared": True, "cover_match": True,
                "cover_hash_distance": 2, "cover_inlier_ratio": 0.9,
            },
        )
        score = score_series_pair(session, left.id, right.id)
        assert score["score"] >= 0.88
        assert score["cover"] >= 0.95
        assert score["description"] == 1
        assert score["chapter_overlap"] == 1


def test_shared_external_identifier_is_capped_at_ninety_nine_percent(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        left, _left_identity = make_series(session, "Left", "asura")
        right, _right_identity = make_series(session, "Right", "mangafire")
        monkeypatch.setattr(
            matching_score,
            "_external_ids",
            lambda _session, _ids: {
                left.id: {"anilist:42"}, right.id: {"anilist:42"},
            },
        )
        assert score_series_pair(session, left.id, right.id)["score"] == 0.99
