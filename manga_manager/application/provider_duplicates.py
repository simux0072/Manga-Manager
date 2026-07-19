from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from manga_manager.domain.catalog import normalize_title
from manga_manager.domain.matching import provider_identities_equivalent
from manga_manager.infrastructure.db_models import (
    CatalogChapter,
    CatalogChapterRelease,
    CatalogExternalIdentifier,
    CatalogSourceSeries,
)


MINIMUM_STRONG_CHAPTERS = 5
MINIMUM_CHAPTER_OVERLAP_RATIO = 0.9


@dataclass(frozen=True, slots=True)
class ProviderDuplicateGroup:
    source: str
    normalized_title: str
    identity_ids: tuple[int, ...]
    series_ids: tuple[int, ...]
    evidence: dict[str, Any]


def duplicate_identity_evidence(
    left: CatalogSourceSeries,
    right: CatalogSourceSeries,
    *,
    left_chapters: set[str],
    right_chapters: set[str],
    left_external_ids: dict[str, str] | None = None,
    right_external_ids: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Classify two same-provider identities without trusting their title alone."""
    overlap = len(left_chapters & right_chapters)
    minimum = min(len(left_chapters), len(right_chapters))
    ratio = overlap / minimum if minimum else 0.0
    left_external_ids = left_external_ids or {}
    right_external_ids = right_external_ids or {}
    conflicting_external_ids = sorted(
        provider
        for provider in left_external_ids.keys() & right_external_ids.keys()
        if left_external_ids[provider] != right_external_ids[provider]
    )
    identity_equivalent = provider_identities_equivalent(left, right)
    left_title = normalize_title(left.title) or left.normalized_title
    right_title = normalize_title(right.title) or right.normalized_title
    title_agrees = bool(left_title) and left_title == right_title
    strong_chapter_overlap = (
        minimum >= MINIMUM_STRONG_CHAPTERS
        and ratio >= MINIMUM_CHAPTER_OVERLAP_RATIO
    )
    equivalent = (
        identity_equivalent
        or (title_agrees and strong_chapter_overlap and not conflicting_external_ids)
    )
    return {
        "equivalent": equivalent,
        "identity_equivalent": identity_equivalent,
        "title_agrees": title_agrees,
        "chapter_overlap": overlap,
        "compared_chapters": minimum,
        "chapter_overlap_ratio": round(ratio, 4),
        "strong_chapter_overlap": strong_chapter_overlap,
        "conflicting_external_ids": conflicting_external_ids,
    }


def provider_duplicate_groups(
    session: Session,
    *,
    series_ids: set[int] | None = None,
    excluded_sources: set[str] | None = None,
    lock: bool = False,
) -> list[ProviderDuplicateGroup]:
    """Find disjoint, mutually equivalent same-provider identity groups."""
    query = select(CatalogSourceSeries).order_by(CatalogSourceSeries.id)
    if series_ids is not None:
        query = query.where(CatalogSourceSeries.series_id.in_(series_ids or {-1}))
    if excluded_sources:
        query = query.where(CatalogSourceSeries.source.not_in(excluded_sources))
    if lock:
        query = query.with_for_update()
    identities = session.scalars(query).all()
    candidates: dict[tuple[str, str], list[CatalogSourceSeries]] = {}
    for identity in identities:
        title = normalize_title(identity.title) or identity.normalized_title
        if title:
            candidates.setdefault((identity.source, title), []).append(identity)
    candidates = {
        key: rows for key, rows in candidates.items() if len(rows) > 1
    }
    identity_ids = {row.id for rows in candidates.values() for row in rows}
    if not identity_ids:
        return []
    chapters: dict[int, set[str]] = {identity_id: set() for identity_id in identity_ids}
    for identity_id, canonical_number in session.execute(
        select(
            CatalogChapterRelease.source_series_id,
            CatalogChapter.canonical_number,
        )
        .join(CatalogChapter, CatalogChapter.id == CatalogChapterRelease.chapter_id)
        .where(
            CatalogChapterRelease.source_series_id.in_(identity_ids or {-1}),
            CatalogChapter.sort_number.is_not(None),
        )
    ):
        chapters[identity_id].add(canonical_number)
    external_ids: dict[int, dict[str, str]] = {
        identity_id: {} for identity_id in identity_ids
    }
    for identity_id, provider, value in session.execute(
        select(
            CatalogExternalIdentifier.source_series_id,
            CatalogExternalIdentifier.provider,
            CatalogExternalIdentifier.value,
        ).where(CatalogExternalIdentifier.source_series_id.in_(identity_ids or {-1}))
    ):
        external_ids[identity_id][provider] = value

    result: list[ProviderDuplicateGroup] = []
    for (source, title), rows in sorted(candidates.items()):
        pair_evidence = {
            tuple(sorted((left.id, right.id))): duplicate_identity_evidence(
                left,
                right,
                left_chapters=chapters[left.id],
                right_chapters=chapters[right.id],
                left_external_ids=external_ids[left.id],
                right_external_ids=external_ids[right.id],
            )
            for left, right in combinations(rows, 2)
        }
        # Build deterministic cliques. Transitive title/chapter similarity alone is not enough:
        # every member must be independently equivalent to every other member in the group.
        groups: list[list[CatalogSourceSeries]] = []
        for row in rows:
            target = next(
                (
                    group
                    for group in groups
                    if all(
                        pair_evidence[tuple(sorted((row.id, member.id)))]["equivalent"]
                        for member in group
                    )
                ),
                None,
            )
            if target is None:
                groups.append([row])
            else:
                target.append(row)
        for group in groups:
            if len(group) < 2:
                continue
            evidence_rows = [
                {
                    "identity_ids": list(pair),
                    **pair_evidence[pair],
                }
                for pair in combinations(sorted(row.id for row in group), 2)
            ]
            result.append(
                ProviderDuplicateGroup(
                    source=source,
                    normalized_title=title,
                    identity_ids=tuple(row.id for row in group),
                    series_ids=tuple(sorted({row.series_id for row in group})),
                    evidence={
                        "policy": "same_title_and_strong_chapter_overlap",
                        "pairs": evidence_rows,
                    },
                )
            )
    return result


def equivalent_series_clusters(
    session: Session,
    series_ids: set[int],
) -> dict[int, tuple[int, ...]]:
    """Map each equivalent canonical record to its complete provider-duplicate cluster."""
    parents = {series_id: series_id for series_id in series_ids}

    def find(value: int) -> int:
        while parents[value] != value:
            parents[value] = parents[parents[value]]
            value = parents[value]
        return value

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parents[max(left_root, right_root)] = min(left_root, right_root)

    for group in provider_duplicate_groups(session, series_ids=series_ids):
        for left, right in combinations(group.series_ids, 2):
            union(left, right)
    clusters: dict[int, list[int]] = {}
    for series_id in series_ids:
        clusters.setdefault(find(series_id), []).append(series_id)
    by_series: dict[int, tuple[int, ...]] = {}
    for values in clusters.values():
        cluster = tuple(sorted(values))
        for series_id in values:
            by_series[series_id] = cluster
    return by_series
