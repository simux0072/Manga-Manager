from __future__ import annotations

from collections import defaultdict
from difflib import SequenceMatcher

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain import title_similarity
from manga_manager.application.cover_evidence import compare_signatures
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogCoverSignature,
    CatalogExternalIdentifier,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
)


SCORER_VERSION = "shared-evidence-v1"


def score_series_pair(session: Session, left_id: int, right_id: int) -> dict[str, object]:
    external = _external_ids(session, (left_id, right_id))
    shared_external = sorted(external[left_id] & external[right_id])
    if shared_external:
        return {
            "score": 0.99,
            "scorer_version": SCORER_VERSION,
            "shared_external_id": shared_external,
            "title": 1.0,
            "cover": 1.0,
            "description": 1.0,
            "chapter_overlap": 1.0,
        }
    titles = _titles(session, (left_id, right_id))
    title = max(
        (title_similarity(a, b) for a in titles[left_id] for b in titles[right_id]),
        default=0.0,
    )
    rows = {row.id: row for row in session.scalars(
        select(CatalogSeries).where(CatalogSeries.id.in_((left_id, right_id)))
    )}
    left_description = rows.get(left_id).description if rows.get(left_id) else ""
    right_description = rows.get(right_id).description if rows.get(right_id) else ""
    description = _text_similarity(left_description, right_description)
    chapter = _chapter_overlap(session, left_id, right_id)
    cover, cover_evidence = _cover_score(session, left_id, right_id)
    score = 0.35 * title + 0.35 * cover + 0.15 * description + 0.15 * chapter
    if cover >= 0.85 and max(description, chapter) >= 0.45:
        score = max(score, 0.88)
    return {
        "score": round(min(score, 0.98), 6),
        "scorer_version": SCORER_VERSION,
        "title": round(title, 6),
        "title_match": title >= 0.65,
        "cover": round(cover, 6),
        "description": round(description, 6),
        "chapter_overlap": round(chapter, 6),
        **cover_evidence,
    }


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
        select(CatalogExternalIdentifier).where(
            CatalogExternalIdentifier.series_id.in_(all_ids)
        )
    ):
        external[row.series_id].add(f"{row.provider}:{row.value}")

    chapters: dict[int, set[str]] = defaultdict(set)
    for series_id, number in session.execute(
        select(CatalogChapter.series_id, CatalogChapter.canonical_number).where(
            CatalogChapter.series_id.in_(all_ids)
        )
    ):
        chapters[series_id].add(number)

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
        if shared_external:
            return {
                "score": 0.99,
                "scorer_version": SCORER_VERSION,
                "shared_external_id": shared_external,
                "title": 1.0,
                "cover": 1.0,
                "description": 1.0,
                "chapter_overlap": 1.0,
            }
        title_score = max(
            (
                title_similarity(left, right)
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
        chapter = (
            len(chapters[left_id] & chapters[right_id]) / len(union) if union else 0.0
        )
        cover = 0.0
        cover_evidence: dict[str, object] = {
            "cover_compared": False,
            "cover_match": False,
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
                distance = int(evidence.get("cover_hash_distance", 64))
                ratio = float(evidence.get("cover_inlier_ratio", 0))
                candidate_cover = max(0.0, 1.0 - distance / 32.0, ratio)
                if evidence.get("cover_match"):
                    candidate_cover = max(candidate_cover, 0.95)
                if candidate_cover > cover:
                    cover = candidate_cover
                    cover_evidence = evidence
        score = 0.35 * title_score + 0.35 * cover + 0.15 * description + 0.15 * chapter
        if cover >= 0.85 and max(description, chapter) >= 0.45:
            score = max(score, 0.88)
        return {
            "score": round(min(score, 0.98), 6),
            "scorer_version": SCORER_VERSION,
            "title": round(title_score, 6),
            "title_match": title_score >= 0.65,
            "cover": round(cover, 6),
            "description": round(description, 6),
            "chapter_overlap": round(chapter, 6),
            **cover_evidence,
        }

    result: dict[int, dict[str, object]] = {}
    for candidate_id in candidate_ids:
        scored = [
            (selected_id, score_pair(selected_id, candidate_id))
            for selected_id in selected_ids
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


def _cover_score(session: Session, left_id: int, right_id: int) -> tuple[float, dict]:
    identities = session.scalars(
        select(CatalogSourceSeries).where(CatalogSourceSeries.series_id.in_((left_id, right_id)))
    ).all()
    left = [row for row in identities if row.series_id == left_id]
    right = [row for row in identities if row.series_id == right_id]
    best_score = 0.0
    best_evidence: dict = {"cover_compared": False, "cover_match": False}
    for left_identity in left:
        left_signature = session.get(CatalogCoverSignature, left_identity.id)
        if left_signature is None:
            continue
        for right_identity in right:
            right_signature = session.get(CatalogCoverSignature, right_identity.id)
            if right_signature is None:
                continue
            evidence = compare_signatures(left_signature, right_signature)
            distance = int(evidence.get("cover_hash_distance", 64))
            ratio = float(evidence.get("cover_inlier_ratio", 0))
            score = max(0.0, 1.0 - distance / 32.0, ratio)
            if evidence.get("cover_match"):
                score = max(score, 0.95)
            if score > best_score:
                best_score, best_evidence = score, evidence
    return min(best_score, 1.0), best_evidence


def _text_similarity(left: str, right: str) -> float:
    left_value = " ".join(left.casefold().split())
    right_value = " ".join(right.casefold().split())
    if not left_value or not right_value:
        return 0.0
    return SequenceMatcher(None, left_value, right_value).ratio()
