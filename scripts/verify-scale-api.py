from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request


def get_json(url: str) -> tuple[dict, float]:
    started = time.perf_counter()
    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.load(response)
    return payload, (time.perf_counter() - started) * 1_000


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify generated catalog/job scale behavior")
    parser.add_argument("--base-url", default="http://127.0.0.1:18002")
    parser.add_argument("--expected-series", type=int, default=1_600)
    parser.add_argument("--max-first-page-ms", type=float, default=1_000)
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    seen: set[int] = set()
    cursor = ""
    first_page_ms = 0.0
    while True:
        query = urllib.parse.urlencode({"limit": 50, "cursor": cursor})
        payload, elapsed = get_json(f"{base}/api/v2/discovery?{query}")
        if not seen:
            first_page_ms = elapsed
        for item in payload["items"]:
            identifier = int(item["id"])
            if identifier in seen:
                raise RuntimeError(f"duplicate discovery cursor item: {identifier}")
            seen.add(identifier)
        cursor = str(payload.get("next_cursor") or "")
        if not cursor:
            break

    groups, group_ms = get_json(f"{base}/api/v2/job-groups?state=queued")
    maintenance = [
        item for item in groups["items"] if item["kind"] == "library_repair"
    ]
    if len(maintenance) != 1:
        raise RuntimeError(f"expected one grouped maintenance entry, got {len(maintenance)}")
    if len(seen) != args.expected_series:
        raise RuntimeError(f"expected {args.expected_series} discovery rows, got {len(seen)}")
    if first_page_ms >= args.max_first_page_ms or group_ms >= args.max_first_page_ms:
        raise RuntimeError(
            f"first page too slow: discovery={first_page_ms:.2f}ms groups={group_ms:.2f}ms"
        )
    print(
        json.dumps(
            {
                "discovery_first_page_ms": round(first_page_ms, 2),
                "discovery_rows": len(seen),
                "job_groups_first_page_ms": round(group_ms, 2),
                "maintenance_tasks": maintenance[0]["task_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
