from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from difflib import SequenceMatcher

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain import normalize_title
from manga_manager.application.cover_evidence import compare_signatures
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogCoverSignature,
    CatalogExternalIdentifier,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
)


SCORER_VERSION = "cover-primary-v3"

TITLE_WEIGHT = 0.20
COVER_WEIGHT = 0.55
DESCRIPTION_WEIGHT = 0.08
CHAPTER_OVERLAP_WEIGHT = 0.05
LATEST_CHAPTER_WEIGHT = 0.12
LATEST_CHAPTER_BUFFER = Decimal("2")


def rescore_pending_decisions_for_series(session: Session, series_id: int) -> int:
    """Refresh pending evidence for a canonical series without deciding or merging it."""
    selected_identity_ids = set(
        session.scalars(
            select(CatalogSourceSeries.id).where(CatalogSourceSeries.series_id == series_id)
        )
    )
    if not selected_identity_ids:
        return 0
    decisions = session.scalars(
        select(CatalogMatchDecision).where(
            CatalogMatchDecision.decision == "pending",
            (CatalogMatchDecision.left_source_series_id.in_(selected_identity_ids))
            | (CatalogMatchDecision.right_source_series_id.in_(selected_identity_ids)),
        )
    ).all()
    all_identity_ids = {
        identity_id
        for decision in decisions
        for identity_id in (
            decision.left_source_series_id,
            decision.right_source_series_id,
        )
    }
    identities = {
        identity.id: identity
        for identity in session.scalars(
            select(CatalogSourceSeries).where(CatalogSourceSeries.id.in_(all_identity_ids or {-1}))
        )
    }
    candidate_ids = {
        identity.series_id for identity in identities.values() if identity.series_id != series_id
    }
    evidence_by_series = score_candidate_set(session, [series_id], sorted(candidate_ids))
    updated = 0
    for decision in decisions:
        left = identities.get(decision.left_source_series_id)
        right = identities.get(decision.right_source_series_id)
        if left is None or right is None or left.series_id == right.series_id:
            continue
        candidate_id = right.series_id if left.series_id == series_id else left.series_id
        evidence = evidence_by_series.get(candidate_id)
        if evidence is None:
            continue
        decision.confidence = float(evidence["score"])
        decision.evidence_json = {
            **evidence,
            "title_or_alias": [left.title, right.title],
            "policy": "manual_review_required",
        }
        decision.scorer_version = str(evidence["scorer_version"])
        decision.feature_vector_json = evidence
        updated += 1
    return updated


def score_series_pair(session: Session, left_id: int, right_id: int) -> dict[str, object]:
    external = _external_ids(session, (left_id, right_id))
    shared_external = sorted(external[left_id] & external[right_id])
    titles = _titles(session, (left_id, right_id))
    title = max(
        (_matching_title_similarity(a, b) for a in titles[left_id] for b in titles[right_id]),
        default=0.0,
    )
    rows = {
        row.id: row
        for row in session.scalars(
            select(CatalogSeries).where(CatalogSeries.id.in_((left_id, right_id)))
        )
    }
    left_description = rows.get(left_id).description if rows.get(left_id) else ""
    right_description = rows.get(right_id).description if rows.get(right_id) else ""
    description = _text_similarity(left_description, right_description)
    chapter = _chapter_overlap(session, left_id, right_id)
    latest = _latest_chapter_evidence(session, left_id, right_id)
    cover, cover_evidence = _cover_score(session, left_id, right_id)
    score = _combined_score(title, cover, description, chapter, latest, cover_evidence)
    result = {
        "score": 0.99 if shared_external else round(min(score, 0.98), 6),
        "scorer_version": SCORER_VERSION,
        "title": round(title, 6),
        "title_match": title >= 0.75,
        "cover": round(cover, 6),
        "description": round(description, 6),
        "chapter_overlap": round(chapter, 6),
        **latest,
        **cover_evidence,
    }
    if shared_external:
        result["shared_external_id"] = shared_external
    return result


def strongest_candidate_score(
    session: Session, selected_ids: list[int], candidate_id: int
) -> dict[str, object]:
    return score_candidate_set(session, selected_ids, [candidate_id]).get(
        candidate_id, {"score": 0.0, "matched_selected_id": None}
    )


def score_candidate_set(
    session: Session,
    selected_ids: list[int],
    candidate_ids: list[int],
) -> dict[int, dict[str, object]]:
    """Score many candidates after loading shared evidence in bounded set queries."""
    all_ids = sorted(set(selected_ids).union(candidate_ids))
    if not selected_ids or not candidate_ids:
        return {}

    series_rows = {
        row.id: row
        for row in session.scalars(select(CatalogSeries).where(CatalogSeries.id.in_(all_ids)))
    }
    titles: dict[int, list[str]] = defaultdict(list)
    for row in series_rows.values():
        titles[row.id].append(row.title)
    for alias in session.scalars(
        select(CatalogSeriesAlias).where(CatalogSeriesAlias.series_id.in_(all_ids))
    ):
        if alias.display_value.strip().casefold() not in {
            "asura scans home",
            "home",
            "manga list",
        }:
            titles[alias.series_id].append(alias.display_value)

    external: dict[int, set[str]] = defaultdict(set)
    for row in session.scalars(
        select(CatalogExternalIdentifier).where(CatalogExternalIdentifier.series_id.in_(all_ids))
    ):
        external[row.series_id].add(f"{row.provider}:{row.value}")

    chapters: dict[int, set[str]] = defaultdict(set)
    latest_chapters: dict[int, Decimal] = {}
    for series_id, number, sort_number in session.execute(
        select(
            CatalogChapter.series_id,
            CatalogChapter.canonical_number,
            CatalogChapter.sort_number,
        ).where(CatalogChapter.series_id.in_(all_ids))
    ):
        chapters[series_id].add(number)
        if sort_number is not None:
            latest_chapters[series_id] = max(
                latest_chapters.get(series_id, sort_number), sort_number
            )

    identities: dict[int, list[int]] = defaultdict(list)
    identity_rows = session.scalars(
        select(CatalogSourceSeries).where(CatalogSourceSeries.series_id.in_(all_ids))
    ).all()
    for identity in identity_rows:
        identities[identity.series_id].append(identity.id)
    identity_ids = [identity.id for identity in identity_rows]
    signatures = {
        row.source_series_id: row
        for row in session.scalars(
            select(CatalogCoverSignature).where(
                CatalogCoverSignature.source_series_id.in_(identity_ids or [-1])
            )
        )
    }

    def score_pair(left_id: int, right_id: int) -> dict[str, object]:
        shared_external = sorted(external[left_id] & external[right_id])
        title_score = max(
            (
                _matching_title_similarity(left, right)
                for left in titles[left_id]
                for right in titles[right_id]
            ),
            default=0.0,
        )
        left_row = series_rows.get(left_id)
        right_row = series_rows.get(right_id)
        description = _text_similarity(
            left_row.description if left_row else "",
            right_row.description if right_row else "",
        )
        union = chapters[left_id] | chapters[right_id]
        chapter = len(chapters[left_id] & chapters[right_id]) / len(union) if union else 0.0
        latest = _latest_chapter_values(latest_chapters.get(left_id), latest_chapters.get(right_id))
        cover = 0.0
        cover_evidence: dict[str, object] = {
            "cover_compared": False,
            "cover_match": False,
            "cover_evidence_state": "unavailable",
        }
        for left_identity in identities[left_id]:
            left_signature = signatures.get(left_identity)
            if left_signature is None:
                continue
            for right_identity in identities[right_id]:
                right_signature = signatures.get(right_identity)
                if right_signature is None:
                    continue
                evidence = compare_signatures(left_signature, right_signature)
                candidate_cover = _cover_similarity(evidence)
                if candidate_cover > cover or not cover_evidence.get("cover_compared"):
                    cover = candidate_cover
                    cover_evidence = {
                        **evidence,
                        "cover_left_source_series_id": left_identity,
                        "cover_right_source_series_id": right_identity,
                    }
        score = _combined_score(
            title_score,
            cover,
            description,
            chapter,
            latest,
            cover_evidence,
        )
        result = {
            "score": 0.99 if shared_external else round(min(score, 0.98), 6),
            "scorer_version": SCORER_VERSION,
            "title": round(title_score, 6),
            "title_match": title_score >= 0.75,
            "cover": round(cover, 6),
            "description": round(description, 6),
            "chapter_overlap": round(chapter, 6),
            **latest,
            **cover_evidence,
        }
        if shared_external:
            result["shared_external_id"] = shared_external
        return result

    result: dict[int, dict[str, object]] = {}
    for candidate_id in candidate_ids:
        scored = [
            (selected_id, score_pair(selected_id, candidate_id)) for selected_id in selected_ids
        ]
        matched_id, best = max(
            scored,
            key=lambda value: float(value[1]["score"]),
            default=(None, {"score": 0.0}),
        )
        result[candidate_id] = {**best, "matched_selected_id": matched_id}
    return result


def _titles(session: Session, ids: tuple[int, int]) -> dict[int, list[str]]:
    values: dict[int, list[str]] = defaultdict(list)
    for row in session.scalars(select(CatalogSeries).where(CatalogSeries.id.in_(ids))):
        values[row.id].append(row.title)
    for alias in session.scalars(
        select(CatalogSeriesAlias).where(CatalogSeriesAlias.series_id.in_(ids))
    ):
        normalized = alias.display_value.strip().casefold()
        if normalized in {"asura scans home", "home", "manga list"}:
            continue
        values[alias.series_id].append(alias.display_value)
    return values


def _external_ids(session: Session, ids: tuple[int, int]) -> dict[int, set[str]]:
    values: dict[int, set[str]] = defaultdict(set)
    for row in session.scalars(
        select(CatalogExternalIdentifier).where(CatalogExternalIdentifier.series_id.in_(ids))
    ):
        values[row.series_id].add(f"{row.provider}:{row.value}")
    return values


def _chapter_overlap(session: Session, left_id: int, right_id: int) -> float:
    numbers: dict[int, set[str]] = defaultdict(set)
    for series_id, number in session.execute(
        select(CatalogChapter.series_id, CatalogChapter.canonical_number).where(
            CatalogChapter.series_id.in_((left_id, right_id))
        )
    ):
        numbers[series_id].add(number)
    union = numbers[left_id] | numbers[right_id]
    return len(numbers[left_id] & numbers[right_id]) / len(union) if union else 0.0


def _latest_chapter_evidence(session: Session, left_id: int, right_id: int) -> dict[str, object]:
    values: dict[int, Decimal] = {}
    for series_id, sort_number in session.execute(
        select(CatalogChapter.series_id, CatalogChapter.sort_number)
        .where(
            CatalogChapter.series_id.in_((left_id, right_id)),
            CatalogChapter.sort_number.is_not(None),
        )
        .order_by(CatalogChapter.series_id, CatalogChapter.sort_number.desc())
    ):
        values.setdefault(series_id, sort_number)
    return _latest_chapter_values(values.get(left_id), values.get(right_id))


def _latest_chapter_values(
    left: Decimal | None,
    right: Decimal | None,
) -> dict[str, object]:
    if left is None or right is None:
        return {
            "latest_chapter_compared": False,
            "latest_chapter_similarity": 0.0,
        }
    delta = abs(left - right)
    if delta <= LATEST_CHAPTER_BUFFER:
        similarity = 1.0
    elif delta <= LATEST_CHAPTER_BUFFER + Decimal("4"):
        similarity = float(1 - (delta - LATEST_CHAPTER_BUFFER) / Decimal("4"))
    else:
        similarity = 0.0
    return {
        "latest_chapter_compared": True,
        "latest_chapter_left": _decimal_text(left),
        "latest_chapter_right": _decimal_text(right),
        "latest_chapter_delta": _decimal_text(delta),
        "latest_chapter_similarity": round(similarity, 6),
        "latest_chapter_match": delta <= LATEST_CHAPTER_BUFFER,
    }


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _cover_score(session: Session, left_id: int, right_id: int) -> tuple[float, dict]:
    identities = session.scalars(
        select(CatalogSourceSeries).where(CatalogSourceSeries.series_id.in_((left_id, right_id)))
    ).all()
    left = [row for row in identities if row.series_id == left_id]
    right = [row for row in identities if row.series_id == right_id]
    best_score = 0.0
    best_evidence: dict = {
        "cover_compared": False,
        "cover_match": False,
        "cover_evidence_state": "unavailable",
    }
    for left_identity in left:
        left_signature = session.get(CatalogCoverSignature, left_identity.id)
        if left_signature is None:
            continue
        for right_identity in right:
            right_signature = session.get(CatalogCoverSignature, right_identity.id)
            if right_signature is None:
                continue
            evidence = compare_signatures(left_signature, right_signature)
            score = _cover_similarity(evidence)
            if score > best_score or not best_evidence.get("cover_compared"):
                best_score = score
                best_evidence = {
                    **evidence,
                    "cover_left_source_series_id": left_identity.id,
                    "cover_right_source_series_id": right_identity.id,
                }
    return min(best_score, 1.0), best_evidence


def _cover_similarity(evidence: dict[str, object]) -> float:
    if not evidence.get("cover_compared"):
        return 0.0
    distance = int(evidence.get("cover_hash_distance", 64))
    ratio = float(evidence.get("cover_inlier_ratio", 0))
    raw = max(0.0, 1.0 - distance / 32.0, ratio)
    state = str(evidence.get("cover_evidence_state") or "inconclusive")
    if state == "match" or evidence.get("cover_match"):
        return max(raw, 0.98)
    if state == "likely":
        return min(max(raw, 0.7), 0.89)
    if state == "different":
        return min(raw, 0.15)
    return min(raw, 0.6)


def _combined_score(
    title: float,
    cover: float,
    description: float,
    chapter_overlap: float,
    latest: dict[str, object],
    cover_evidence: dict[str, object],
) -> float:
    latest_score = float(latest.get("latest_chapter_similarity", 0.0))
    score = (
        TITLE_WEIGHT * title
        + COVER_WEIGHT * cover
        + DESCRIPTION_WEIGHT * description
        + CHAPTER_OVERLAP_WEIGHT * chapter_overlap
        + LATEST_CHAPTER_WEIGHT * latest_score
    )
    if cover_evidence.get("cover_match"):
        score = max(score, 0.86)
        if latest_score == 1.0 or max(title, description) >= 0.75:
            score = max(score, 0.90)
    return score


def _matching_title_similarity(left: str, right: str) -> float:
    normalized_left = normalize_title(left)
    normalized_right = normalize_title(right)
    if not normalized_left or not normalized_right:
        return 0.0
    if normalized_left == normalized_right:
        return 1.0
    left_tokens = set(normalized_left.split())
    right_tokens = set(normalized_right.split())
    common = left_tokens & right_tokens
    token_score = len(common) / len(left_tokens | right_tokens)
    sequence_score = SequenceMatcher(None, normalized_left, normalized_right).ratio()
    score = 0.6 * token_score + 0.4 * sequence_score
    # A shared generic word such as "villain" or "return" must not by itself create a strong
    # title match. Exact aliases still score 1.0 above.
    if len(common) < 2:
        score = min(score, 0.45)
    return score


def _text_similarity(left: str, right: str) -> float:
    left_value = " ".join(left.casefold().split())
    right_value = " ".join(right.casefold().split())
    if not left_value or not right_value:
        return 0.0
    return SequenceMatcher(None, left_value, right_value).ratio()
