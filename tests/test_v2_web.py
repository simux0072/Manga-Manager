from __future__ import annotations

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from manga_manager.infrastructure.db_models import CatalogSeries, CatalogSourceSeries, JobBase
from manga_manager.web.app import create_app


def app_with_catalog():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=__import__("sqlalchemy").pool.StaticPool,
    )
    JobBase.metadata.create_all(engine)
    sessions = sessionmaker(engine, expire_on_commit=False)
    with sessions() as session, session.begin():
        one = CatalogSeries(
            title="A Very Long Example Manga Title",
            normalized_title="a very long example manga title",
            status="untracked",
        )
        two = CatalogSeries(title="Tracked", normalized_title="tracked", status="reading")
        session.add_all([one, two])
        session.flush()
        session.add(
            CatalogSourceSeries(
                series_id=one.id,
                source="asura",
                source_id="example",
                title=one.title,
                normalized_title=one.normalized_title,
                url="https://example.test",
            )
        )
    return create_app(sessions), sessions


async def test_discovery_search_source_and_fragment() -> None:
    app, _ = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/discovery", params={"q": "long", "source": "asura"})
        fragment = await client.get("/discovery", headers={"HX-Request": "true"})
    assert response.status_code == 200
    assert "A Very Long Example" in response.text
    assert "<!doctype html>" not in fragment.text


async def test_library_state_progressive_post_and_redirects() -> None:
    app, sessions = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        library = await client.get("/library")
        with sessions() as session:
            series_id = session.query(CatalogSeries).filter_by(title="Tracked").one().id
        response = await client.post(
            f"/series/{series_id}/state", data={"state": "caught_up"}, follow_redirects=False
        )
        redirect = await client.get("/info", follow_redirects=False)
    assert "Tracked" in library.text
    assert response.status_code == 303
    with sessions() as session:
        assert session.get(CatalogSeries, series_id).status == "caught_up"
    assert redirect.headers["location"] == "/operations"


async def test_operations_and_health() -> None:
    app, _ = app_with_catalog()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/healthz")).json()["architecture"] == "postgresql-v2"
        assert (await client.get("/operations")).status_code == 200
