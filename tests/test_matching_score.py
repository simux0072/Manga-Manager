from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from manga_manager.application import matching_score
from manga_manager.application.matching_score import (
    rescore_pending_decisions_for_series,
    score_series_pair,
)
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogCoverSignature,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    JobBase,
)


def make_series(session: Session, title: str, source: str, *, description: str = ""):
    series = CatalogSeries(
        title=title,
        normalized_title=title.casefold(),
        description=description,
    )
    session.add(series)
    session.flush()
    identity = CatalogSourceSeries(
        series_id=series.id,
        source=source,
        source_id=f"{source}-{series.id}",
        title=title,
        normalized_title=title.casefold(),
        url=f"https://example/{series.id}",
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
        session.add_all(
            [
                CatalogSeriesAlias(
                    series_id=left.id,
                    source_series_id=left_identity.id,
                    display_value="Asura Scans Home",
                    normalized_value="asura scans home",
                ),
                CatalogSeriesAlias(
                    series_id=right.id,
                    source_series_id=right_identity.id,
                    display_value="Asura Scans Home",
                    normalized_value="asura scans home",
                ),
            ]
        )
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
            session,
            "There Are No Bad Young Ladies in This World",
            "kingofshojo",
            description=description,
        )
        for series in (left, right):
            for number in ("1", "2", "3"):
                session.add(
                    CatalogChapter(
                        series_id=series.id,
                        canonical_number=number,
                        display_number=number,
                    )
                )
        for identity in (left_identity, right_identity):
            session.add(
                CatalogCoverSignature(
                    source_series_id=identity.id,
                    algorithm_version="test",
                    feature_json={"hashes": ["0" * 16]},
                    keypoints_blob=b"",
                    descriptors_blob=b"",
                )
            )
        monkeypatch.setattr(
            matching_score,
            "compare_signatures",
            lambda _left, _right: {
                "cover_compared": True,
                "cover_match": True,
                "cover_hash_distance": 2,
                "cover_inlier_ratio": 0.9,
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
                left.id: {"anilist:42"},
                right.id: {"anilist:42"},
            },
        )
        assert score_series_pair(session, left.id, right.id)["score"] == 0.99


def test_title_similarity_requires_more_than_one_generic_shared_word() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        left, _ = make_series(session, "Return of the Demonic Instructor", "asura")
        right, _ = make_series(session, "Return of the Frozen Player", "mangafire")

        score = score_series_pair(session, left.id, right.id)

        assert score["title"] <= 0.45
        assert score["title_match"] is False


def test_latest_chapters_match_within_two_chapter_provider_buffer() -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        left, _ = make_series(session, "Left", "asura")
        right, _ = make_series(session, "Right", "mangafire")
        session.add_all(
            [
                CatalogChapter(
                    series_id=left.id,
                    canonical_number="50",
                    display_number="50",
                    sort_number=Decimal("50"),
                ),
                CatalogChapter(
                    series_id=right.id,
                    canonical_number="52",
                    display_number="52",
                    sort_number=Decimal("52"),
                ),
            ]
        )

        score = score_series_pair(session, left.id, right.id)

        assert score["latest_chapter_compared"] is True
        assert score["latest_chapter_match"] is True
        assert score["latest_chapter_delta"] == "2"
        assert score["latest_chapter_similarity"] == 1


def test_cover_evidence_outranks_exact_title_without_cover(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        title_left, _ = make_series(session, "Same Name", "asura")
        title_right, _ = make_series(session, "Same Name", "mangafire")
        cover_left, cover_left_identity = make_series(session, "Completely Different", "asura")
        cover_right, cover_right_identity = make_series(session, "Unrelated Words", "kingofshojo")
        for identity in (cover_left_identity, cover_right_identity):
            session.add(
                CatalogCoverSignature(
                    source_series_id=identity.id,
                    algorithm_version="test",
                    feature_json={"hashes": ["0" * 16]},
                    keypoints_blob=b"",
                    descriptors_blob=b"",
                )
            )
        monkeypatch.setattr(
            matching_score,
            "compare_signatures",
            lambda _left, _right: {
                "cover_compared": True,
                "cover_match": True,
                "cover_evidence_state": "match",
                "cover_hash_distance": 0,
                "cover_inlier_ratio": 0.9,
            },
        )

        title_score = score_series_pair(session, title_left.id, title_right.id)
        cover_score = score_series_pair(session, cover_left.id, cover_right.id)

        assert cover_score["score"] > title_score["score"]
        assert cover_score["score"] >= 0.86


def test_rescore_updates_evidence_but_never_accepts_pending_match(monkeypatch) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    with Session(engine) as session, session.begin():
        left, left_identity = make_series(session, "Left", "asura")
        right, right_identity = make_series(session, "Right", "mangafire")
        for identity in (left_identity, right_identity):
            session.add(
                CatalogCoverSignature(
                    source_series_id=identity.id,
                    algorithm_version="test",
                    feature_json={"hashes": ["0" * 16]},
                    keypoints_blob=b"",
                    descriptors_blob=b"",
                )
            )
        decision = CatalogMatchDecision(
            left_source_series_id=left_identity.id,
            right_source_series_id=right_identity.id,
            confidence=0,
            evidence_json={},
        )
        session.add(decision)
        monkeypatch.setattr(
            matching_score,
            "compare_signatures",
            lambda _left, _right: {
                "cover_compared": True,
                "cover_match": True,
                "cover_evidence_state": "match",
                "cover_hash_distance": 0,
                "cover_inlier_ratio": 1.0,
            },
        )

        updated = rescore_pending_decisions_for_series(session, left.id)

        assert updated == 1
        assert decision.decision == "pending"
        assert decision.confidence >= 0.86
        assert decision.evidence_json["cover_evidence_state"] == "match"
        assert decision.scorer_version == "cover-primary-v2"
