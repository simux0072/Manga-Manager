from pathlib import Path


def test_react_frontend_replaces_htmx_templates() -> None:
    package = Path("frontend/package.json").read_text()
    assert '"react"' in package
    assert '"@tanstack/react-query"' in package
    assert not Path("app/static/htmx.min.js").exists()
    assert not list(Path("manga_manager/web/templates").glob("*.html"))


def test_media_cards_drawer_and_mobile_shell_contract() -> None:
    css = Path("frontend/src/styles.css").read_text()
    assert "minmax(320px,1fr)" in css
    assert ".job-drawer{position:fixed" in css
    assert ".drawer-scrim{position:fixed" in css
    assert "@media(max-width:640px)" in css
    assert ".bottom-nav{position:fixed" in css


def test_typeahead_multi_source_optimistic_tracking_and_sse_contract() -> None:
    application = Path("frontend/src/App.tsx").read_text()
    assert "useDebounced(query)" in application
    assert "selected.includes(source)" in application
    assert "setQueriesData({queryKey:['discovery']}" in application
    assert "Undo" in application
    assert "new EventSource" in application
    assert "sessionStorage.getItem('eventCursor')" in application


def test_every_manga_workspace_uses_cover_component() -> None:
    application = Path("frontend/src/App.tsx").read_text()
    for component in ("DiscoveryCard", "LibraryCard", "UpdateCard", "MatchSideCard"):
        section = application.split(f"function {component}", 1)[1].split("function ", 1)[0]
        assert "<Cover" in section
