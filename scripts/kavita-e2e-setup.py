"""Provision a disposable Kavita instance for Manga Manager integration testing."""

from __future__ import annotations

import json
import os
import secrets
import time
import urllib.error
import urllib.request


BASE_URL = os.environ.get("KAVITA_E2E_URL", "http://manga-manager-kavita-e2e:5000").rstrip("/")
USERNAME = os.environ.get("KAVITA_E2E_USERNAME", "manga-manager-e2e")
PASSWORD = os.environ.get("KAVITA_E2E_PASSWORD") or secrets.token_urlsafe(18)
EXISTING_API_KEY = os.environ.get("KAVITA_E2E_API_KEY", "")


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


def main() -> None:
    for _ in range(60):
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
        except (OSError, urllib.error.HTTPError):
            time.sleep(1)
    else:
        raise RuntimeError("Kavita did not become ready")
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
        library = request(
            "/api/Library/create",
            method="POST",
            token=token,
            payload=library_payload,
        )
    else:
        request(
            "/api/Library/update",
            method="POST",
            token=token,
            payload=library_payload,
        )
    print(
        json.dumps(
            {
                "url": BASE_URL,
                "username": USERNAME,
                "password": PASSWORD,
                "api_key": auth_key_value,
                "library_id": library["id"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
