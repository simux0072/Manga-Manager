from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.domain import ChapterItem, SeriesItem
from manga_manager.domain.catalog import (
    canonical_chapter_number,
    chapter_sort_number,
    normalize_title,
)
from manga_manager.infrastructure.db_models import (
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


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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
        source_series = session.scalar(
            select(CatalogSourceSeries).where(
                CatalogSourceSeries.source == item.source,
                CatalogSourceSeries.source_id == item.source_id,
            )
        )
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
        source_series.title = item.title
        source_series.normalized_title = normalize_title(item.title)
        source_series.url = item.url
        source_series.description = item.description
        source_series.cover_url = item.cover_url
        source_series.popularity = item.popularity
        source_series.metadata_json = dict(item.metadata)
        source_series.last_checked_at = now
        source_series.detail_fetched_at = now
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
        for chapter_item in chapters:
            self._upsert_chapter(session, series.id, source_series.id, chapter_item)
        session.flush()
        return source_series

    def record_poll_success(
        self,
        session: Session,
        *,
        source: str,
        frontier: list[dict[str, str]],
        partial_failures: int = 0,
    ) -> None:
        state = self._source_state(session, source)
        state.health_status = "degraded" if partial_failures else "healthy"
        state.consecutive_failures = 0
        state.last_error = f"{partial_failures} item failures" if partial_failures else ""
        state.frontier_json = list(frontier)
        state.cooldown_until = None
        state.last_poll_at = utcnow()
        state.updated_at = utcnow()
        policy = session.get(ProviderPolicy, source)
        if policy is not None and policy.clean_since is None:
            policy.clean_since = utcnow()
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
            if cooldown_until is not None:
                seconds = max(60, int((cooldown_until - utcnow()).total_seconds()))
                policy.cooldown_seconds = max(policy.cooldown_seconds, seconds)
                metadata = dict(policy.metadata_json or {})
                metadata.update(
                    {"recovery_probe_step": 0, "next_recovery_probe": (utcnow() + timedelta(minutes=1)).isoformat(), "recovery_probe_successes": 0}
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
        for candidate in candidates:
            left_id, right_id = sorted((candidate.id, source_series.id))
            existing = session.scalar(
                select(CatalogMatchDecision.id).where(
                    CatalogMatchDecision.left_source_series_id == left_id,
                    CatalogMatchDecision.right_source_series_id == right_id,
                )
            )
            if existing is not None:
                continue
            same_cover = bool(
                item.cover_url
                and candidate.cover_url
                and normalized_url(item.cover_url) == normalized_url(candidate.cover_url)
            )
            session.add(
                CatalogMatchDecision(
                    left_source_series_id=left_id,
                    right_source_series_id=right_id,
                    confidence=0.94 if same_cover else 0.70,
                    evidence_json={
                        "title_or_alias": sorted(normalized_values),
                        "cover_match": same_cover,
                        "policy": "manual_review_required_without_shared_external_id",
                    },
                )
            )

    def _sync_aliases(
        self,
        session: Session,
        series_id: int,
        source_series_id: int,
        aliases: Iterable[str],
    ) -> None:
        for display_value in aliases:
            normalized = normalize_title(display_value)
            if not normalized:
                continue
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

    def _upsert_chapter(
        self,
        session: Session,
        series_id: int,
        source_series_id: int,
        item: ChapterItem,
    ) -> None:
        canonical = canonical_chapter_number(item.number)
        chapter = session.scalar(
            select(CatalogChapter).where(
                CatalogChapter.series_id == series_id,
                CatalogChapter.canonical_number == canonical,
            )
        )
        if chapter is None:
            chapter = CatalogChapter(
                series_id=series_id,
                canonical_number=canonical,
                display_number=item.number,
                sort_number=chapter_sort_number(item.number),
                title=item.title,
            )
            session.add(chapter)
            session.flush()
        elif not chapter.title and item.title:
            chapter.title = item.title
            chapter.updated_at = utcnow()

        release = session.scalar(
            select(CatalogChapterRelease).where(
                CatalogChapterRelease.source_series_id == source_series_id,
                CatalogChapterRelease.source_release_id == canonical,
            )
        )
        if release is None:
            release = CatalogChapterRelease(
                chapter_id=chapter.id,
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
        series = session.get(CatalogSeries, series_id)
        release_time = item.published_at or utcnow()
        current_latest = series.latest_release_at if series is not None else None
        if current_latest is not None and current_latest.tzinfo is None:
            current_latest = current_latest.replace(tzinfo=timezone.utc)
        if release_time.tzinfo is None:
            release_time = release_time.replace(tzinfo=timezone.utc)
        if series is not None and (current_latest is None or release_time >= current_latest):
            series.latest_release_at = release_time
            series.latest_release_number = item.number
            series.latest_release_source = item.source
            series.integrity_state = "healthy"

    def _source_state(self, session: Session, source: str) -> CatalogSourceState:
        state = session.get(CatalogSourceState, source)
        if state is None:
            state = CatalogSourceState(source=source)
            session.add(state)
        return state


def normalized_url(value: str) -> str:
    return value.strip().lower().split("?", 1)[0].rstrip("/")
