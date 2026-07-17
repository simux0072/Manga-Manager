"""Provision a disposable Kavita instance for Manga Manager integration testing."""

from __future__ import annotations

import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.request


BASE_URL = os.environ.get("KAVITA_E2E_URL", "http://manga-manager-stage-kavita:5000").rstrip(
    "/"
)
USERNAME = os.environ.get("KAVITA_E2E_USERNAME", "manga-manager-e2e")
PASSWORD = os.environ.get("KAVITA_E2E_PASSWORD") or secrets.token_urlsafe(18)
EXISTING_API_KEY = os.environ.get("KAVITA_E2E_API_KEY", "")
OUTPUT_ENV = os.environ.get("KAVITA_ENV_OUTPUT", "")
WAIT_SECONDS = int(os.environ.get("KAVITA_WAIT_SECONDS", "300"))


class CredentialMismatchError(RuntimeError):
    """The persistent Kavita administrator does not match local credentials."""


def request(path: str, *, method: str = "GET", payload=None, token: str = ""):
    body = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = urllib.request.urlopen(
        urllib.request.Request(BASE_URL + path, data=body, headers=headers, method=method),
        timeout=60,
    )
    raw = response.read()
    return json.loads(raw) if raw else None


def ensure_library(token: str) -> dict:
    libraries = request("/api/Library/libraries", token=token) or []
    library = next((row for row in libraries if row.get("name") == "Manga Manager E2E"), None)
    library_payload = {
        "id": int(library["id"]) if library else 0,
        "name": "Manga Manager E2E",
        "type": 0,
        "folders": ["/manga"],
        "folderWatching": False,
        "includeInDashboard": True,
        "includeInSearch": True,
        "manageCollections": True,
        "manageReadingLists": True,
        "allowScrobbling": False,
        "allowMetadataMatching": False,
        "enableMetadata": True,
        "removePrefixForSortName": False,
        "inheritWebLinksFromFirstChapter": False,
        "defaultLanguage": "en",
        "metadataProvider": 0,
        "fileGroupTypes": [1],
        "excludePatterns": [],
    }
    if library is None:
        request(
            "/api/Library/create",
            method="POST",
            token=token,
            payload=library_payload,
        )
        libraries = request("/api/Library/libraries", token=token) or []
        library = next(
            (row for row in libraries if row.get("name") == "Manga Manager E2E"),
            None,
        )
        if library is None:
            raise RuntimeError("Kavita accepted library creation but did not return the library")
    else:
        request(
            "/api/Library/update",
            method="POST",
            token=token,
            payload=library_payload,
        )
    return library


def main() -> None:
    last_error = "no response"
    for _ in range(max(1, WAIT_SECONDS)):
        try:
            if request("/api/Admin/exists") is False:
                user = request(
                    "/api/Account/register",
                    method="POST",
                    payload={"username": USERNAME, "password": PASSWORD, "email": None},
                )
            else:
                user = request(
                    "/api/Account/login",
                    method="POST",
                    payload={"username": USERNAME, "password": PASSWORD, "apiKey": None},
                )
            break
        except urllib.error.HTTPError as exc:
            if exc.code in {401, 403}:
                raise CredentialMismatchError(
                    "Kavita already has a different administrator; preserve the configured "
                    "Kavita credential file or reset the local Kavita config volume"
                ) from exc
            last_error = f"HTTP {exc.code} from {exc.url}"
            time.sleep(1)
        except OSError as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(1)
    else:
        raise RuntimeError(
            f"Kavita did not become ready within {WAIT_SECONDS}s; last error: {last_error}"
        )
    token = str(user["token"])
    auth_key_value = EXISTING_API_KEY
    if not auth_key_value:
        auth_key = request(
            "/api/Account/create-auth-key",
            method="POST",
            token=token,
            payload={"name": "Manga Manager E2E", "keyLength": 32, "expiresUtc": None},
        )
        auth_key_value = str(auth_key.get("key") or auth_key.get("authKey"))
    library = ensure_library(token)
    result = {
        "url": BASE_URL,
        "username": USERNAME,
        "password": PASSWORD,
        "api_key": auth_key_value,
        "library_id": library["id"],
    }
    if OUTPUT_ENV:
        with open(OUTPUT_ENV, "w", encoding="utf-8") as handle:
            handle.write(f"KAVITA_URL={BASE_URL}\n")
            handle.write(f"KAVITA_USERNAME={USERNAME}\n")
            handle.write(f"KAVITA_PASSWORD={PASSWORD}\n")
            handle.write(f"KAVITA_API_KEY={auth_key_value}\n")
            handle.write("KAVITA_LIBRARY_ROOT=/manga\n")
        os.chmod(OUTPUT_ENV, 0o600)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except CredentialMismatchError as exc:
        # The local launcher uses this distinct status to recover an orphaned disposable
        # config volume without mistaking network or migration failures for bad credentials.
        print(str(exc), file=sys.stderr)
        raise SystemExit(42) from None
