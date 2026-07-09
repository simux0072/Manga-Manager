from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Series(Base):
    __tablename__ = "series"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(String(500), index=True)
    normalized_title: Mapped[str] = mapped_column(String(500), index=True)
    aliases: Mapped[str] = mapped_column(Text, default="")
    description: Mapped[str] = mapped_column(Text, default="")
    cover_url: Mapped[str] = mapped_column(Text, default="")
    genres: Mapped[str] = mapped_column(Text, default="")
    popularity: Mapped[float] = mapped_column(Float, default=0)
    external_ids: Mapped[str] = mapped_column(Text, default="")
    cover_path: Mapped[str] = mapped_column(Text, default="")
    kavita_series_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    kavita_library_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kavita_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(30), default="new", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    sources: Mapped[list[SourceSeries]] = relationship(back_populates="series")
    chapters: Mapped[list[Chapter]] = relationship(back_populates="series")
    progress: Mapped[SeriesProgress | None] = relationship(back_populates="series")


class SourceSeries(Base):
    __tablename__ = "source_series"
    __table_args__ = (UniqueConstraint("source", "source_id", name="uq_source_series"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(ForeignKey("series.id"), index=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    source_id: Mapped[str] = mapped_column(String(500), index=True)
    title: Mapped[str] = mapped_column(String(500))
    url: Mapped[str] = mapped_column(Text)
    normalized_title: Mapped[str] = mapped_column(String(500), index=True)
    aliases: Mapped[str] = mapped_column(Text, default="")
    cover_url: Mapped[str] = mapped_column(Text, default="")
    description: Mapped[str] = mapped_column(Text, default="")
    genres: Mapped[str] = mapped_column(Text, default="")
    popularity: Mapped[float] = mapped_column(Float, default=0)
    external_ids: Mapped[str] = mapped_column(Text, default="")
    cover_path: Mapped[str] = mapped_column(Text, default="")
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    detail_fetched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    series: Mapped[Series] = relationship(back_populates="sources")
    releases: Mapped[list[ChapterRelease]] = relationship(back_populates="source_series")


class Chapter(Base):
    __tablename__ = "chapter"
    __table_args__ = (UniqueConstraint("series_id", "number", name="uq_series_chapter"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(ForeignKey("series.id"), index=True)
    number: Mapped[str] = mapped_column(String(40), index=True)
    title: Mapped[str] = mapped_column(String(500), default="")
    best_source: Mapped[str] = mapped_column(String(50), default="")
    downloaded_source: Mapped[str] = mapped_column(String(50), default="")
    cbz_path: Mapped[str] = mapped_column(Text, default="")
    kavita_chapter_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    kavita_volume_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    kavita_mapped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    series: Mapped[Series] = relationship(back_populates="chapters")
    releases: Mapped[list[ChapterRelease]] = relationship(back_populates="chapter")
    progress: Mapped[ChapterProgress | None] = relationship(back_populates="chapter")


class SeriesProgress(Base):
    __tablename__ = "series_progress"
    __table_args__ = (UniqueConstraint("series_id", name="uq_series_progress_series"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(ForeignKey("series.id"), index=True)
    status: Mapped[str] = mapped_column(String(30), default="interested", index=True)
    note: Mapped[str] = mapped_column(Text, default="")
    rating: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    series: Mapped[Series] = relationship(back_populates="progress")


class ChapterProgress(Base):
    __tablename__ = "chapter_progress"
    __table_args__ = (UniqueConstraint("chapter_id", name="uq_chapter_progress_chapter"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_id: Mapped[int] = mapped_column(ForeignKey("chapter.id"), index=True)
    status: Mapped[str] = mapped_column(String(30), default="unread", index=True)
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    chapter: Mapped[Chapter] = relationship(back_populates="progress")


class ChapterRelease(Base):
    __tablename__ = "chapter_release"
    __table_args__ = (UniqueConstraint("source_series_id", "number", name="uq_source_chapter"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_id: Mapped[int | None] = mapped_column(ForeignKey("chapter.id"), nullable=True, index=True)
    source_series_id: Mapped[int] = mapped_column(ForeignKey("source_series.id"), index=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    number: Mapped[str] = mapped_column(String(40), index=True)
    title: Mapped[str] = mapped_column(String(500), default="")
    url: Mapped[str] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    downloadable_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    downloaded: Mapped[bool] = mapped_column(Boolean, default=False)

    chapter: Mapped[Chapter | None] = relationship(back_populates="releases")
    source_series: Mapped[SourceSeries] = relationship(back_populates="releases")


class DownloadJob(Base):
    __tablename__ = "download_job"
    __table_args__ = (
        UniqueConstraint("chapter_release_id", name="uq_download_job_chapter_release"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_release_id: Mapped[int] = mapped_column(ForeignKey("chapter_release.id"), index=True)
    status: Mapped[str] = mapped_column(String(30), default="queued", index=True)
    priority: Mapped[int] = mapped_column(Integer, default=100, index=True)
    job_type: Mapped[str] = mapped_column(String(30), default="normal", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text, default="")
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class KavitaSyncJob(Base):
    __tablename__ = "kavita_sync_job"
    __table_args__ = (
        UniqueConstraint("series_id", name="uq_kavita_sync_job_series"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    series_id: Mapped[int] = mapped_column(ForeignKey("series.id"), index=True)
    status: Mapped[str] = mapped_column(String(30), default="queued", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text, default="")
    retry_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    folder_path: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    series: Mapped[Series] = relationship()


class DownloadedFile(Base):
    __tablename__ = "downloaded_file"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_id: Mapped[int] = mapped_column(ForeignKey("chapter.id"), index=True)
    chapter_release_id: Mapped[int] = mapped_column(ForeignKey("chapter_release.id"), index=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    path: Mapped[str] = mapped_column(Text)
    checksum: Mapped[str] = mapped_column(String(128), default="")
    image_count: Mapped[int] = mapped_column(Integer, default=0)
    active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    replaced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class SourceHealth(Base):
    __tablename__ = "source_health"

    source: Mapped[str] = mapped_column(String(50), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    last_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str] = mapped_column(Text, default="")
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)


class SourcePullJob(Base):
    __tablename__ = "source_pull_job"
    __table_args__ = (
        Index(
            "uq_source_pull_job_active_source",
            "source",
            unique=True,
            sqlite_where=text("status IN ('queued', 'running')"),
            postgresql_where=text("status IN ('queued', 'running')"),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source: Mapped[str] = mapped_column(String(50), default="", index=True)
    status: Mapped[str] = mapped_column(String(30), default="queued", index=True)
    total_items: Mapped[int] = mapped_column(Integer, default=0)
    processed_items: Mapped[int] = mapped_column(Integer, default=0)
    error: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ManualMatchRule(Base):
    __tablename__ = "manual_match_rule"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_series_id: Mapped[int] = mapped_column(ForeignKey("source_series.id"), index=True)
    target_series_id: Mapped[int | None] = mapped_column(ForeignKey("series.id"), nullable=True)
    action: Mapped[str] = mapped_column(String(20))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class MatchCandidate(Base):
    __tablename__ = "match_candidate"
    __table_args__ = (
        UniqueConstraint("source_series_id", "candidate_series_id", name="uq_match_candidate"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_series_id: Mapped[int] = mapped_column(ForeignKey("source_series.id"), index=True)
    candidate_series_id: Mapped[int] = mapped_column(ForeignKey("series.id"), index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0)
    status: Mapped[str] = mapped_column(String(20), default="pending", index=True)
    reason: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    source_series: Mapped[SourceSeries] = relationship()
    candidate_series: Mapped[Series] = relationship()


class ActivityEvent(Base):
    __tablename__ = "activity_event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[str] = mapped_column(String(50), index=True)
    status: Mapped[str] = mapped_column(String(30), default="info", index=True)
    message: Mapped[str] = mapped_column(Text, default="")
    source: Mapped[str] = mapped_column(String(50), default="", index=True)
    series_id: Mapped[int | None] = mapped_column(ForeignKey("series.id"), nullable=True, index=True)
    chapter_id: Mapped[int | None] = mapped_column(ForeignKey("chapter.id"), nullable=True, index=True)
    download_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("download_job.id"), nullable=True, index=True
    )
    kavita_sync_job_id: Mapped[int | None] = mapped_column(
        ForeignKey("kavita_sync_job.id"), nullable=True, index=True
    )
    metadata_json: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
