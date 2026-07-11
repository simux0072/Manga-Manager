from pathlib import Path


def test_official_htmx_is_vendored_with_license() -> None:
    runtime = Path("app/static/htmx.min.js").read_text()
    license_text = Path("app/static/HTMX-LICENSE.txt").read_text()
    assert len(runtime) > 50_000
    assert runtime.startswith("var htmx=")
    assert "Zero-Clause BSD" in license_text


def test_responsive_shell_keeps_drawer_overlay_and_titles_wrapped() -> None:
    css = Path("app/static/styles.css").read_text()
    assert ".job-drawer { position: fixed" in css
    assert ".media-card h2 { overflow-wrap: anywhere; white-space: normal" in css
    assert "@media (max-width: 760px)" in css
    assert ".cover-grid { grid-template-columns:repeat(2,minmax(0,1fr))" in css
    assert ".topbar,.content { margin-left:0; }" in css


def test_fragment_forms_and_sse_reconnection_contract() -> None:
    templates = "\n".join(
        path.read_text() for path in Path("manga_manager/web/templates").glob("*.html")
    )
    javascript = Path("app/static/v2.js").read_text()
    assert "hx-post=" in templates
    assert "hx-target=" in templates
    assert "hx-swap=" in templates
    assert "sessionStorage.getItem(\"jobLastEventId\")" in javascript
    assert "?after=${lastEventId}" in javascript
    assert 'source.addEventListener("counts"' in javascript
