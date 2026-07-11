from __future__ import annotations

import uvicorn
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from manga_manager.infrastructure.db_models import CatalogSeries, JobBase
from manga_manager.web.app import create_app

engine = create_engine(
    "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
)
JobBase.metadata.create_all(engine)
sessions = sessionmaker(engine, expire_on_commit=False)
with sessions() as session, session.begin():
    session.add_all(
        CatalogSeries(
            title=f"A deliberately long responsive manga title number {index}",
            normalized_title=f"a deliberately long responsive manga title number {index}",
            status="reading" if index % 2 else "interested",
        )
        for index in range(30)
    )

app = create_app(sessions)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=18001)
