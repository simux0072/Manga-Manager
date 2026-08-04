from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ALEMBIC_CONFIG = PROJECT_ROOT / "alembic.v2.ini"


def create_database_engine(
    database_url: str,
    *,
    allow_sqlite_for_tests: bool = False,
    role: str = "cli",
) -> Engine:
    if not database_url.startswith("postgresql+") and not allow_sqlite_for_tests:
        raise ValueError("the v2 runtime requires a PostgreSQL database URL")
    if database_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
        pool_options = {}
    else:
        connect_args = {
            "connect_timeout": 10,
            "application_name": f"manga-manager-{role}",
            "options": "-c statement_timeout=30000 -c lock_timeout=5000",
        }
        # SSE streams release their request session before waiting, and worker
        # database work is split across a small number of bounded executor lanes.
        # Keep modest burst headroom without retaining twenty PostgreSQL backends.
        pool_size, max_overflow = {
            "web": (4, 2),
            "worker": (6, 2),
            "cli": (1, 1),
        }.get(role, (1, 1))
        pool_options = {
            "pool_size": pool_size,
            "max_overflow": max_overflow,
            "pool_timeout": 10,
            "pool_recycle": 1800,
            "pool_use_lifo": True,
        }
    return create_engine(
        database_url,
        connect_args=connect_args,
        pool_pre_ping=True,
        **pool_options,
    )


def create_session_factory(engine: Engine) -> Callable[[], Session]:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def run_migrations(
    database_url: str,
    *,
    config_path: Path = DEFAULT_ALEMBIC_CONFIG,
) -> None:
    config = Config(str(config_path))
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")
