from __future__ import annotations

import sqlite3
from pathlib import Path

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from manga_manager.application.catalog_recovery import CatalogRecovery
from manga_manager.infrastructure.db_models import (
    CatalogSeries,
    CatalogSourceSeries,
    JobBase,
    WorkJob,
)


def legacy_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE series(id INTEGER PRIMARY KEY, title TEXT, status TEXT);
        CREATE TABLE chapter(id INTEGER PRIMARY KEY, series_id INTEGER);
        CREATE TABLE source_series(
          id INTEGER PRIMARY KEY, source TEXT, source_id TEXT
        );
        CREATE TABLE chapter_release(
          id INTEGER PRIMARY KEY, chapter_id INTEGER, source_series_id INTEGER
        );
        CREATE TABLE downloaded_file(
          id INTEGER PRIMARY KEY, chapter_id INTEGER, chapter_release_id INTEGER, active INTEGER
        );
        INSERT INTO series VALUES(1, 'Downloaded Example', 'ignored');
        INSERT INTO chapter VALUES(1, 1);
        INSERT INTO source_series VALUES(1, 'mangafire', 'downloaded-example');
        INSERT INTO chapter_release VALUES(1, 1, 1);
        INSERT INTO downloaded_file VALUES(1, 1, 1, 1);
        """
    )
    connection.commit()
    connection.close()


def test_recovery_tracks_downloaded_legacy_series_including_ignored(tmp_path: Path) -> None:
    engine = create_engine("sqlite:///:memory:")
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        series = CatalogSeries(
            title="Downloaded Example",
            normalized_title="downloaded example",
            status="untracked",
        )
        session.add(series)
        session.flush()
        session.add(
            CatalogSourceSeries(
                series_id=series.id,
                source="mangafire",
                source_id="downloaded-example",
                title=series.title,
                normalized_title=series.normalized_title,
                url="https://mangafire.to/manga/downloaded-example",
            )
        )
    legacy = tmp_path / "legacy.db"
    legacy_database(legacy)
    recovery = CatalogRecovery(sessions)

    dry_run = recovery.run(legacy, apply=False)
    assert dry_run[0].action == "track"
    with sessions() as session:
        assert session.scalar(select(CatalogSeries.status)) == "untracked"

    applied = recovery.run(legacy, apply=True)
    assert applied == dry_run
    with sessions() as session:
        assert session.scalar(select(CatalogSeries.status)) == "interested"
        kinds = set(session.scalars(select(WorkJob.kind)))
        assert "library_repair" in kinds
        assert session.scalar(select(func.count()).select_from(CatalogSeries)) == 1
        job_count = session.scalar(select(func.count()).select_from(WorkJob))
    second = recovery.run(legacy, apply=True)
    assert second[0].action == "verified"
    with sessions() as session:
        assert session.scalar(select(func.count()).select_from(WorkJob)) == job_count
