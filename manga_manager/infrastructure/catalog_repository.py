from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, func, select, text, tuple_
from sqlalchemy.orm import Session

from app.domain import ChapterItem, SeriesItem
from manga_manager.domain.catalog import (
    canonical_chapter_number,
    chapter_sort_number,
    normalize_title,
)
from manga_manager.infrastructure.db_models import (
    CatalogAlternateSourceListing,
    CatalogChapter,
    CatalogChapterRelease,
    CatalogExternalIdentifier,
    CatalogMatchDecision,
    CatalogSeries,
    CatalogSeriesAlias,
    CatalogSourceSeries,
    CatalogSourceState,
    ProviderPolicy,
)
from app.adapters.asura import split_asura_source_id
from manga_manager.application.provider_duplicates import duplicate_identity_evidence


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def update_poll_cadence(
    policy: ProviderPolicy,
    *,
    successful: bool,
    changed: bool,
) -> None:
    metadata = dict(policy.metadata_json or {})
    try:
        base = int(metadata.get("base_poll_seconds") or 0)
        current = int(metadata.get("adaptive_poll_seconds") or base)
        streak = int(metadata.get("unchanged_poll_streak") or 0)
    except (TypeError, ValueError):
        return
    if base <= 0:
        return
    if not successful:
        current = min(base * 4, round(max(current, base) * 1.5))
        streak = 0
    elif changed:
        current = max(base // 2, round(current * 0.75))
        streak = 0
    else:
        streak += 1
        current = min(base * 4, round(current * (1.1 + min(streak, 5) * 0.02)))
    metadata["adaptive_poll_seconds"] = max(60, current)
    metadata["unchanged_poll_streak"] = streak
    metadata["last_poll_had_changes"] = changed
    policy.metadata_json = metadata


def listing_observation_version(item: SeriesItem) -> str:
    explicit = str(item.metadata.get("latest_chapter") or "").strip()
    if explicit:
        return explicit
    rows = item.metadata.get("recent_chapters")
    if not isinstance(rows, list):
        return ""
    values = [
        str(row.get("number") or "").strip()
        for row in rows
        if isinstance(row, dict) and row.get("number")
    ]
    return max(values, key=chapter_sort_number, default="")


class CatalogRepository:
    def source_frontier(self, session: Session, source: str) -> list[dict[str, str]]:
        state = session.get(CatalogSourceState, source)
        return list(state.frontier_json) if state is not None else []

    def ingest(
        self,
        session: Session,
        item: SeriesItem,
        chapters: Iterable[ChapterItem],
    ) -> CatalogSourceSeries:
        chapter_items = list(chapters)
        normalized_source_id = self._normalized_identity(item)
        normalized_item_title = normalize_title(item.title)
        if (
            normalized_item_title
            and session.bind is not None
            and session.bind.dialect.name == "postgresql"
        ):
            # Separate refresh workers can discover two aliases of one provider listing at the
            # same time. Serialize only this provider/title identity decision, not the pull.
            session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, hashtext(:identity_key))"),
                {
                    "namespace": 0x4D445550,
                    "identity_key": f"{item.source}:{normalized_item_title}",
                },
            )
        source_series = session.scalar(
            select(CatalogSourceSeries).where(
                CatalogSourceSeries.source == item.source,
                (CatalogSourceSeries.normalized_source_id == normalized_source_id)
                | (CatalogSourceSeries.source_id == item.source_id),
            )
        )
        alternate = None
        if source_series is None:
            alternate = session.scalar(
                select(CatalogAlternateSourceListing).where(
                    CatalogAlternateSourceListing.source == item.source,
                    CatalogAlternateSourceListing.source_id == item.source_id,
                )
            )
            if alternate is not None:
                source_series = session.get(CatalogSourceSeries, alternate.primary_source_series_id)
        if source_series is None:
            equivalent = self._equivalent_provider_identity(session, item, chapter_items)
            if equivalent is not None:
                source_series, evidence = equivalent
                alternate = CatalogAlternateSourceListing(
                    primary_source_series_id=source_series.id,
                    source=item.source,
                    source_id=item.source_id,
                    title=item.title,
                    url=item.url,
                    evidence_json=evidence,
                )
                session.add(alternate)
                session.flush()
        if source_series is None:
            series = self._matching_series(session, item) or CatalogSeries(
                title=item.title,
                normalized_title=normalize_title(item.title),
                description=item.description,
                cover_url=item.cover_url,
                metadata_json={},
            )
            session.add(series)
            session.flush()
            source_series = CatalogSourceSeries(
                series_id=series.id,
                source=item.source,
                source_id=item.source_id,
                normalized_source_id=normalized_source_id,
                revision_override=(
                    str(item.metadata.get("asura_revision_override") or "")
                    if item.source == "asura"
                    else ""
                ),
                title=item.title,
                normalized_title=normalize_title(item.title),
                url=item.url,
            )
            session.add(source_series)
            session.flush()
            self._record_match_candidates(session, source_series, item)
        else:
            series = session.get(CatalogSeries, source_series.series_id)
            if series is None:
                raise RuntimeError(f"canonical series {source_series.series_id} is missing")

        now = utcnow()
        if alternate is None:
            source_series.title = item.title
            source_series.source_id = (
                normalized_source_id if item.source == "asura" else item.source_id
            )
            source_series.normalized_source_id = normalized_source_id
            source_series.normalized_title = normalized_item_title
            source_series.url = item.url
        else:
            # Keep the selected primary identity stable while recording that this historical
            # alternate is still live and may carry fresher metadata.
            alternate.title = item.title
            alternate.url = item.url
        if item.source == "asura":
            source_series.revision_override = str(
                item.metadata.get("asura_revision_override") or ""
            )
        source_series.description = item.description
        source_series.cover_url = item.cover_url
        source_series.popularity = item.popularity
        source_series.metadata_json = dict(item.metadata)
        source_series.last_checked_at = now
        source_series.detail_fetched_at = now
        source_series.observation_version = listing_observation_version(item)
        source_series.observation_seen_at = now
        if not series.description and item.description:
            series.description = item.description
        if not series.cover_url and item.cover_url:
            series.cover_url = item.cover_url
        series.updated_at = now

        self._sync_aliases(session, series.id, source_series.id, item.aliases)
        self._sync_external_ids(
            session,
            series.id,
            source_series.id,
            item.external_ids,
        )
        self._upsert_chapters(session, series.id, source_series.id, chapter_items)
        self._recompute_latest(session, series.id)
        self._refresh_match_scores(session, source_series)
        session.flush()
        return source_series

    @staticmethod
    def _equivalent_provider_identity(
        session: Session,
        item: SeriesItem,
        chapters: list[ChapterItem],
    ) -> tuple[CatalogSourceSeries, dict] | None:
        """Resolve a new provider ID as an alternate only with strong catalog evidence."""
        normalized_title = normalize_title(item.title)
        incoming_chapters = {
            canonical_chapter_number(chapter.number)
            for chapter in chapters
            if chapter_sort_number(chapter.number) is not None
        }
        if not normalized_title or not incoming_chapters:
            return None
        candidates = session.scalars(
            select(CatalogSourceSeries)
            .where(
                CatalogSourceSeries.source == item.source,
                CatalogSourceSeries.normalized_title == normalized_title,
            )
            .order_by(CatalogSourceSeries.id)
        ).all()
        if not candidates:
            return None
        candidate_ids = {candidate.id for candidate in candidates}
        candidate_chapters: dict[int, set[str]] = {
            candidate_id: set() for candidate_id in candidate_ids
        }
        for identity_id, chapter_number in session.execute(
            select(
                CatalogChapterRelease.source_series_id,
                CatalogChapter.canonical_number,
            )
            .join(CatalogChapter, CatalogChapter.id == CatalogChapterRelease.chapter_id)
            .where(
                CatalogChapterRelease.source_series_id.in_(candidate_ids),
                CatalogChapter.sort_number.is_not(None),
            )
        ):
            candidate_chapters[identity_id].add(chapter_number)
        external_ids: dict[int, dict[str, str]] = {
            candidate_id: {} for candidate_id in candidate_ids
        }
        for identity_id, provider, value in session.execute(
            select(
                CatalogExternalIdentifier.source_series_id,
                CatalogExternalIdentifier.provider,
                CatalogExternalIdentifier.value,
            ).where(CatalogExternalIdentifier.source_series_id.in_(candidate_ids))
        ):
            external_ids[identity_id][provider] = value
        incoming = CatalogSourceSeries(
            series_id=0,
            source=item.source,
            source_id=item.source_id,
            normalized_source_id=CatalogRepository._normalized_identity(item),
            title=item.title,
            normalized_title=normalized_title,
            url=item.url,
        )
        matches = []
        for candidate in candidates:
            evidence = duplicate_identity_evidence(
                candidate,
                incoming,
                left_chapters=candidate_chapters[candidate.id],
                right_chapters=incoming_chapters,
                left_external_ids=external_ids[candidate.id],
                right_external_ids=item.external_ids,
            )
            if evidence["equivalent"]:
                matches.append((candidate, evidence))
        if not matches:
            return None
        return max(
            matches,
            key=lambda match: (
                match[1]["chapter_overlap_ratio"],
                match[1]["chapter_overlap"],
                -match[0].id,
            ),
        )

    @staticmethod
    def _normalized_identity(item: SeriesItem) -> str:
        if item.source == "asura":
            return split_asura_source_id(item.source_id)[0]
        return item.source_id.strip()

    def record_poll_success(
        self,
        session: Session,
        *,
        source: str,
        frontier: list[dict[str, str]],
        partial_failures: int = 0,
        metrics: dict[str, int | bool] | None = None,
    ) -> None:
        state = self._source_state(session, source)
        state.health_status = "degraded" if partial_failures else "healthy"
        state.consecutive_failures = 0
        state.last_error = f"{partial_failures} item failures" if partial_failures else ""
        state.frontier_json = list(frontier)
        state.cursor_json = {**(state.cursor_json or {}), "last_pull": dict(metrics or {})}
        state.cooldown_until = None
        state.last_poll_at = utcnow()
        state.updated_at = utcnow()
        policy = session.get(ProviderPolicy, source)
        if policy is not None:
            if policy.clean_since is None:
                policy.clean_since = utcnow()
            update_poll_cadence(
                policy,
                successful=partial_failures == 0,
                changed=bool((metrics or {}).get("candidates")),
            )
        session.flush()

    def record_poll_failure(
        self,
        session: Session,
        *,
        source: str,
        error: str,
        cooldown_until: datetime | None = None,
    ) -> None:
        state = self._source_state(session, source)
        # Imported pre-v2 rows may predate the non-null application default.
        state.consecutive_failures = (state.consecutive_failures or 0) + 1
        if cooldown_until is None and state.consecutive_failures >= 3:
            minutes = min(5 * (2 ** (state.consecutive_failures - 3)), 360)
            cooldown_until = utcnow() + timedelta(minutes=minutes)
        state.health_status = "cooldown" if cooldown_until else "degraded"
        state.last_error = error[:4000]
        state.cooldown_until = cooldown_until
        state.last_poll_at = utcnow()
        state.updated_at = utcnow()
        policy = session.get(ProviderPolicy, source)
        if policy is not None:
            policy.clean_since = None
            update_poll_cadence(policy, successful=False, changed=False)
            if cooldown_until is not None:
                seconds = max(60, int((cooldown_until - utcnow()).total_seconds()))
                policy.cooldown_seconds = max(policy.cooldown_seconds, seconds)
                metadata = dict(policy.metadata_json or {})
                metadata.update(
                    {
                        "recovery_probe_step": 0,
                        "next_recovery_probe": (utcnow() + timedelta(minutes=1)).isoformat(),
                        "recovery_probe_successes": 0,
                    }
                )
                policy.metadata_json = metadata
        session.flush()

    def _matching_series(self, session: Session, item: SeriesItem) -> CatalogSeries | None:
        for provider, value in item.external_ids.items():
            identifier = session.scalar(
                select(CatalogExternalIdentifier).where(
                    CatalogExternalIdentifier.provider == provider,
                    CatalogExternalIdentifier.value == value,
                )
            )
            if identifier is not None:
                already_has_source = session.scalar(
                    select(CatalogSourceSeries.id).where(
                        CatalogSourceSeries.series_id == identifier.series_id,
                        CatalogSourceSeries.source == item.source,
                    )
                )
                if already_has_source is None:
                    return session.get(CatalogSeries, identifier.series_id)

        return None

    def _record_match_candidates(
        self, session: Session, source_series: CatalogSourceSeries, item: SeriesItem
    ) -> None:
        normalized_values = {normalize_title(item.title)} | {
            normalize_title(alias) for alias in item.aliases
        }
        normalized_values.discard("")
        candidates = session.scalars(
            select(CatalogSourceSeries)
            .where(CatalogSourceSeries.id != source_series.id)
            .where(CatalogSourceSeries.source != source_series.source)
            .where(CatalogSourceSeries.normalized_title.in_(normalized_values))
            .order_by(CatalogSourceSeries.id)
        ).all()
        candidate_pairs = {
            tuple(sorted((candidate.id, source_series.id))) for candidate in candidates
        }
        existing_pairs = set(
            session.execute(
                select(
                    CatalogMatchDecision.left_source_series_id,
                    CatalogMatchDecision.right_source_series_id,
                ).where(
                    tuple_(
                        CatalogMatchDecision.left_source_series_id,
                        CatalogMatchDecision.right_source_series_id,
                    ).in_(candidate_pairs or {(-1, -1)})
                )
            ).all()
        )
        from manga_manager.application.matching_score import score_candidate_set

        evidence_by_series = score_candidate_set(
            session,
            [source_series.series_id],
            sorted({candidate.series_id for candidate in candidates}),
        )
        for candidate in candidates:
            left_id, right_id = sorted((candidate.id, source_series.id))
            if (left_id, right_id) in existing_pairs:
                continue
            evidence = evidence_by_series[candidate.series_id]
            session.add(
                CatalogMatchDecision(
                    left_source_series_id=left_id,
                    right_source_series_id=right_id,
                    confidence=float(evidence["score"]),
                    evidence_json={
                        **evidence,
                        "title_or_alias": sorted(normalized_values),
                        "policy": "manual_review_required_without_shared_external_id",
                    },
                    scorer_version=str(evidence["scorer_version"]),
                    feature_vector_json=evidence,
                )
            )

    @staticmethod
    def _refresh_match_scores(session: Session, source_series: CatalogSourceSeries) -> None:
        from manga_manager.application.matching_score import (
            rescore_pending_decisions_for_series,
        )

        rescore_pending_decisions_for_series(session, source_series.series_id)

    def _sync_aliases(
        self,
        session: Session,
        series_id: int,
        source_series_id: int,
        aliases: Iterable[str],
    ) -> None:
        values = list(aliases)
        desired = {normalize_title(value) for value in values if normalize_title(value)}
        stale = delete(CatalogSeriesAlias).where(
            CatalogSeriesAlias.source_series_id == source_series_id
        )
        if desired:
            stale = stale.where(CatalogSeriesAlias.normalized_value.not_in(desired))
        session.execute(stale)
        session.execute(
            delete(CatalogSeriesAlias).where(
                CatalogSeriesAlias.source_series_id == source_series_id,
                func.lower(CatalogSeriesAlias.display_value).in_(
                    ("asura scans home", "home", "manga list")
                ),
            )
        )
        # Alias synchronization can race across provider refresh workers for the
        # same canonical series. Lock once for the complete set, not once per alias.
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :series_id)"),
                {"namespace": 0x4D414C49, "series_id": series_id},
            )
        seen: set[str] = set()
        for display_value in values:
            normalized = normalize_title(display_value)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            existing = session.scalar(
                select(CatalogSeriesAlias).where(
                    CatalogSeriesAlias.series_id == series_id,
                    CatalogSeriesAlias.normalized_value == normalized,
                )
            )
            if existing is None:
                session.add(
                    CatalogSeriesAlias(
                        series_id=series_id,
                        source_series_id=source_series_id,
                        display_value=display_value,
                        normalized_value=normalized,
                    )
                )

    def _sync_external_ids(
        self,
        session: Session,
        series_id: int,
        source_series_id: int,
        external_ids: dict[str, str],
    ) -> None:
        for provider, value in external_ids.items():
            if not provider or not value:
                continue
            source_identifier = session.scalar(
                select(CatalogExternalIdentifier).where(
                    CatalogExternalIdentifier.source_series_id == source_series_id,
                    CatalogExternalIdentifier.provider == provider,
                )
            )
            existing = session.scalar(
                select(CatalogExternalIdentifier).where(
                    CatalogExternalIdentifier.provider == provider,
                    CatalogExternalIdentifier.value == value,
                )
            )
            if source_identifier is not None:
                if existing is None or existing.id == source_identifier.id:
                    source_identifier.value = value
                continue
            if existing is None:
                session.add(
                    CatalogExternalIdentifier(
                        series_id=series_id,
                        source_series_id=source_series_id,
                        provider=provider,
                        value=value,
                    )
                )

    def _upsert_chapters(
        self,
        session: Session,
        series_id: int,
        source_series_id: int,
        items: list[ChapterItem],
    ) -> None:
        by_number: dict[str, ChapterItem] = {}
        for item in items:
            canonical = canonical_chapter_number(item.number)
            if canonical:
                by_number[canonical] = item
        if not by_number:
            return
        chapters = {
            chapter.canonical_number: chapter
            for chapter in session.scalars(
                select(CatalogChapter).where(
                    CatalogChapter.series_id == series_id,
                    CatalogChapter.canonical_number.in_(by_number),
                )
            )
        }
        now = utcnow()
        for canonical, item in by_number.items():
            chapter = chapters.get(canonical)
            if chapter is None:
                chapter = CatalogChapter(
                    series_id=series_id,
                    canonical_number=canonical,
                    display_number=item.number,
                    sort_number=chapter_sort_number(item.number),
                    title=item.title,
                )
                session.add(chapter)
                chapters[canonical] = chapter
            elif not chapter.title and item.title:
                chapter.title = item.title
                chapter.updated_at = now
        session.flush()

        releases = {
            release.source_release_id: release
            for release in session.scalars(
                select(CatalogChapterRelease).where(
                    CatalogChapterRelease.source_series_id == source_series_id,
                    CatalogChapterRelease.source_release_id.in_(by_number),
                )
            )
        }
        for canonical, item in by_number.items():
            release = releases.get(canonical)
            if release is None:
                release = CatalogChapterRelease(
                    chapter_id=chapters[canonical].id,
                    source_series_id=source_series_id,
                    source=item.source,
                    source_release_id=canonical,
                    title=item.title,
                    url=item.url,
                )
                session.add(release)
            release.title = item.title
            release.url = item.url
            release.published_at = item.published_at
        # Runtime sessions intentionally disable autoflush. Latest-release queries must see
        # releases created above, otherwise new series remain blank and existing series lag one
        # observation behind.
        session.flush()

    @staticmethod
    def _recompute_latest(session: Session, series_id: int) -> None:
        series = session.get(CatalogSeries, series_id)
        if series is None:
            return
        numeric = session.execute(
            select(CatalogChapter, CatalogChapterRelease)
            .join(CatalogChapterRelease)
            .where(CatalogChapter.series_id == series_id, CatalogChapter.sort_number.is_not(None))
            .order_by(CatalogChapter.sort_number.desc(), CatalogChapterRelease.id.desc())
            .limit(1)
        ).first()
        fallback = None
        if numeric is None:
            fallback = session.execute(
                select(CatalogChapter, CatalogChapterRelease)
                .join(CatalogChapterRelease)
                .where(CatalogChapter.series_id == series_id)
                .order_by(
                    CatalogChapterRelease.published_at.desc().nullslast(),
                    CatalogChapterRelease.id.desc(),
                )
                .limit(1)
            ).first()
        selected = numeric or fallback
        if selected is None:
            return
        chapter, release = selected
        series.latest_release_number = chapter.display_number
        series.latest_release_source = release.source
        series.latest_release_at = release.published_at or release.first_seen_at
        series.integrity_state = "healthy"

    def _source_state(self, session: Session, source: str) -> CatalogSourceState:
        state = session.get(CatalogSourceState, source)
        if state is None:
            state = CatalogSourceState(source=source)
            session.add(state)
        return state
