"""Verify that a Kavita series and chapter expose the exact same uploaded cover."""

from __future__ import annotations

import argparse
import hashlib
import urllib.parse
import urllib.request


def fetch(base_url: str, path: str, api_key: str, identifier: tuple[str, int]) -> bytes:
    query = urllib.parse.urlencode({identifier[0]: identifier[1], "apiKey": api_key})
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}?{query}", headers={"x-api-key": api_key}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        content = response.read()
    if not content:
        raise RuntimeError(f"Kavita returned an empty cover from {path}")
    return content


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--series-id", required=True, type=int)
    parser.add_argument("--chapter-id", required=True, type=int)
    args = parser.parse_args()
    series = fetch(args.url, "/api/Image/series-cover", args.api_key, ("seriesId", args.series_id))
    chapter = fetch(
        args.url, "/api/Image/chapter-cover", args.api_key, ("chapterId", args.chapter_id)
    )
    series_hash = hashlib.sha256(series).hexdigest()
    chapter_hash = hashlib.sha256(chapter).hexdigest()
    if series_hash != chapter_hash:
        raise RuntimeError(f"cover mismatch: series={series_hash} chapter={chapter_hash}")
    print(f"cover=ok sha256={series_hash} bytes={len(series)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
