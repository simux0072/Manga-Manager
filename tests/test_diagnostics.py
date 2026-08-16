from __future__ import annotations

from manga_manager.application.diagnostics import redact_text


def test_diagnostic_text_redacts_credentials_queries_and_tokens() -> None:
    value = (
        "postgresql://manga:super-secret@postgres/db "
        "https://example.test/chapter?token=visible "
        "api_key=also-visible password: third-secret "
        "Authorization: Bearer fourth-secret"
    )

    redacted = redact_text(value)

    assert "super-secret" not in redacted
    assert "visible" not in redacted
    assert "third-secret" not in redacted
    assert "fourth-secret" not in redacted
    assert redacted.count("[redacted]") == 5


def test_diagnostic_text_is_bounded() -> None:
    assert redact_text("x" * 20, limit=10) == "xxxxxxxxxx…"
