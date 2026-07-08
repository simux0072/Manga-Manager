from collections.abc import Iterator
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.settings import settings


class Base(DeclarativeBase):
    pass


connect_args = {"check_same_thread": False} if settings.database_url.startswith("sqlite") else {}
engine = create_engine(settings.database_url, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def init_db() -> None:
    from app import models  # noqa: F401

    if settings.database_url == "sqlite:///:memory:":
        Base.metadata.create_all(bind=engine)
        return
    run_migrations()
    apply_compat_migrations()


def run_migrations() -> None:
    config_path = Path("alembic.ini")
    if not config_path.exists():
        Base.metadata.create_all(bind=engine)
        return

    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    config = Config(str(config_path))
    config.set_main_option("sqlalchemy.url", settings.database_url)
    if table_names and "alembic_version" not in table_names:
        ensure_compatible_unversioned_schema(engine.dialect.name, table_names)
        apply_compat_migrations()
        command.stamp(config, "head")
        return
    command.upgrade(config, "head")


def ensure_compatible_unversioned_schema(dialect_name: str, table_names: set[str]) -> None:
    if not table_names or "alembic_version" in table_names or dialect_name == "sqlite":
        return
    tables = ", ".join(sorted(table_names))
    raise RuntimeError(
        "Refusing to stamp an existing non-SQLite database without an Alembic version. "
        "Run Alembic migrations against a fresh database, or manually stamp the verified "
        f"schema after inspection. Existing tables: {tables}"
    )


def apply_compat_migrations() -> None:
    """Small compatibility layer until Alembic migrations are formalized."""
    if engine.dialect.name != "sqlite":
        return
    inspector = inspect(engine)
    if "chapter_release" not in inspector.get_table_names():
        return

    columns_by_table = {
        table: {column["name"] for column in inspector.get_columns(table)}
        for table in inspector.get_table_names()
    }
    ddl: list[str] = []
    if "cover_path" not in columns_by_table.get("series", set()):
        ddl.append("ALTER TABLE series ADD COLUMN cover_path TEXT DEFAULT '' NOT NULL")
    if "cover_path" not in columns_by_table.get("source_series", set()):
        ddl.append("ALTER TABLE source_series ADD COLUMN cover_path TEXT DEFAULT '' NOT NULL")
    if "external_ids" not in columns_by_table.get("series", set()):
        ddl.append("ALTER TABLE series ADD COLUMN external_ids TEXT DEFAULT '' NOT NULL")
    if "kavita_series_id" not in columns_by_table.get("series", set()):
        ddl.append("ALTER TABLE series ADD COLUMN kavita_series_id INTEGER")
    if "kavita_library_id" not in columns_by_table.get("series", set()):
        ddl.append("ALTER TABLE series ADD COLUMN kavita_library_id INTEGER")
    if "kavita_synced_at" not in columns_by_table.get("series", set()):
        ddl.append("ALTER TABLE series ADD COLUMN kavita_synced_at DATETIME")
    if "external_ids" not in columns_by_table.get("source_series", set()):
        ddl.append("ALTER TABLE source_series ADD COLUMN external_ids TEXT DEFAULT '' NOT NULL")
    if "detail_fetched_at" not in columns_by_table.get("source_series", set()):
        ddl.append("ALTER TABLE source_series ADD COLUMN detail_fetched_at DATETIME")
    if "first_seen_at" not in columns_by_table.get("chapter_release", set()):
        ddl.append("ALTER TABLE chapter_release ADD COLUMN first_seen_at DATETIME")
    if "retry_after" not in columns_by_table.get("download_job", set()):
        ddl.append("ALTER TABLE download_job ADD COLUMN retry_after DATETIME")
    if "chapter" in columns_by_table and "kavita_chapter_id" not in columns_by_table["chapter"]:
        ddl.append("ALTER TABLE chapter ADD COLUMN kavita_chapter_id INTEGER")
    if "chapter" in columns_by_table and "kavita_volume_id" not in columns_by_table["chapter"]:
        ddl.append("ALTER TABLE chapter ADD COLUMN kavita_volume_id INTEGER")
    if "chapter" in columns_by_table and "kavita_mapped_at" not in columns_by_table["chapter"]:
        ddl.append("ALTER TABLE chapter ADD COLUMN kavita_mapped_at DATETIME")
    create_kavita_sync_job = "kavita_sync_job" not in columns_by_table
    create_series_progress = "series_progress" not in columns_by_table
    create_chapter_progress = "chapter_progress" not in columns_by_table
    create_activity_event = "activity_event" not in columns_by_table

    with engine.begin() as connection:
        for statement in ddl:
            connection.execute(text(statement))
        if create_kavita_sync_job:
            connection.execute(
                text(
                    "CREATE TABLE kavita_sync_job ("
                    "id INTEGER PRIMARY KEY, "
                    "series_id INTEGER NOT NULL, "
                    "status VARCHAR(30) NOT NULL, "
                    "attempts INTEGER NOT NULL, "
                    "error TEXT NOT NULL, "
                    "retry_after DATETIME, "
                    "folder_path TEXT NOT NULL, "
                    "created_at DATETIME NOT NULL, "
                    "updated_at DATETIME NOT NULL, "
                    "FOREIGN KEY(series_id) REFERENCES series (id), "
                    "CONSTRAINT uq_kavita_sync_job_series UNIQUE (series_id)"
                    ")"
                )
            )
            connection.execute(
                text("CREATE INDEX ix_kavita_sync_job_series_id ON kavita_sync_job (series_id)")
            )
            connection.execute(
                text("CREATE INDEX ix_kavita_sync_job_status ON kavita_sync_job (status)")
            )
        if create_series_progress:
            connection.execute(
                text(
                    "CREATE TABLE series_progress ("
                    "id INTEGER PRIMARY KEY, "
                    "series_id INTEGER NOT NULL, "
                    "status VARCHAR(30) DEFAULT 'interested' NOT NULL, "
                    "note TEXT DEFAULT '' NOT NULL, "
                    "rating INTEGER, "
                    "created_at DATETIME NOT NULL, "
                    "updated_at DATETIME NOT NULL, "
                    "FOREIGN KEY(series_id) REFERENCES series (id), "
                    "CONSTRAINT uq_series_progress_series UNIQUE (series_id)"
                    ")"
                )
            )
            connection.execute(
                text("CREATE INDEX ix_series_progress_series_id ON series_progress (series_id)")
            )
            connection.execute(
                text("CREATE INDEX ix_series_progress_status ON series_progress (status)")
            )
        if create_chapter_progress:
            connection.execute(
                text(
                    "CREATE TABLE chapter_progress ("
                    "id INTEGER PRIMARY KEY, "
                    "chapter_id INTEGER NOT NULL, "
                    "status VARCHAR(30) DEFAULT 'unread' NOT NULL, "
                    "read_at DATETIME, "
                    "created_at DATETIME NOT NULL, "
                    "updated_at DATETIME NOT NULL, "
                    "FOREIGN KEY(chapter_id) REFERENCES chapter (id), "
                    "CONSTRAINT uq_chapter_progress_chapter UNIQUE (chapter_id)"
                    ")"
                )
            )
            connection.execute(
                text("CREATE INDEX ix_chapter_progress_chapter_id ON chapter_progress (chapter_id)")
            )
            connection.execute(
                text("CREATE INDEX ix_chapter_progress_status ON chapter_progress (status)")
            )
        if create_activity_event:
            connection.execute(
                text(
                    "CREATE TABLE activity_event ("
                    "id INTEGER PRIMARY KEY, "
                    "kind VARCHAR(50) NOT NULL, "
                    "status VARCHAR(30) DEFAULT 'info' NOT NULL, "
                    "message TEXT DEFAULT '' NOT NULL, "
                    "source VARCHAR(50) DEFAULT '' NOT NULL, "
                    "series_id INTEGER, "
                    "chapter_id INTEGER, "
                    "download_job_id INTEGER, "
                    "kavita_sync_job_id INTEGER, "
                    "metadata_json TEXT DEFAULT '' NOT NULL, "
                    "created_at DATETIME NOT NULL, "
                    "FOREIGN KEY(series_id) REFERENCES series (id), "
                    "FOREIGN KEY(chapter_id) REFERENCES chapter (id), "
                    "FOREIGN KEY(download_job_id) REFERENCES download_job (id), "
                    "FOREIGN KEY(kavita_sync_job_id) REFERENCES kavita_sync_job (id)"
                    ")"
                )
            )
            for name, column in {
                "kind": "kind",
                "status": "status",
                "source": "source",
                "series_id": "series_id",
                "chapter_id": "chapter_id",
                "download_job_id": "download_job_id",
                "kavita_sync_job_id": "kavita_sync_job_id",
                "created_at": "created_at",
            }.items():
                connection.execute(
                    text(f"CREATE INDEX ix_activity_event_{name} ON activity_event ({column})")
                )
        if "first_seen_at" in columns_by_table.get("chapter_release", set()) or any(
            "first_seen_at" in statement for statement in ddl
        ):
            connection.execute(
                text(
                    "UPDATE chapter_release "
                    "SET first_seen_at = COALESCE(first_seen_at, published_at, CURRENT_TIMESTAMP)"
                )
            )
        indexes = (
            {
                index["name"]
                for index in inspector.get_indexes("download_job")
                if index.get("name")
            }
            if "download_job" in columns_by_table
            else set()
        )
        if "download_job" in columns_by_table and "uq_download_job_chapter_release" not in indexes:
            connection.execute(text(deduplicate_download_jobs_sql(columns_by_table["download_job"])))
            connection.execute(
                text(
                    "CREATE UNIQUE INDEX uq_download_job_chapter_release "
                    "ON download_job (chapter_release_id)"
                )
            )


def deduplicate_download_jobs_sql(columns: set[str]) -> str:
    if "status" not in columns:
        return (
            "DELETE FROM download_job "
            "WHERE id NOT IN ("
            "SELECT MIN(id) FROM download_job GROUP BY chapter_release_id"
            ")"
        )

    timestamp_expr = "'1970-01-01 00:00:00'"
    if "updated_at" in columns and "created_at" in columns:
        timestamp_expr = "COALESCE(updated_at, created_at, '1970-01-01 00:00:00')"
    elif "updated_at" in columns:
        timestamp_expr = "COALESCE(updated_at, '1970-01-01 00:00:00')"
    elif "created_at" in columns:
        timestamp_expr = "COALESCE(created_at, '1970-01-01 00:00:00')"

    return f"""
DELETE FROM download_job
WHERE id IN (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY chapter_release_id
                ORDER BY
                    CASE status
                        WHEN 'complete' THEN 1
                        WHEN 'delayed' THEN 2
                        WHEN 'running' THEN 3
                        WHEN 'queued' THEN 4
                        WHEN 'failed' THEN 5
                        ELSE 6
                    END,
                    {timestamp_expr} DESC,
                    id ASC
            ) AS row_number
        FROM download_job
    ) ranked
    WHERE row_number > 1
)
"""


def get_session() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session
