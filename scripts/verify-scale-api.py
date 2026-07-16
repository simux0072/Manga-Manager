from __future__ import annotations

import argparse
import json
import time
import urllib.parse
import urllib.request


def get_json(url: str) -> tuple[dict, float, int]:
    started = time.perf_counter()
    with urllib.request.urlopen(url, timeout=10) as response:
        payload = json.load(response)
        query_count = int(response.headers.get("X-SQL-Query-Count", "-1"))
    return payload, (time.perf_counter() - started) * 1_000, query_count


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify generated catalog/job scale behavior")
    parser.add_argument("--base-url", default="http://127.0.0.1:18002")
    parser.add_argument("--expected-series", type=int, default=1_600)
    parser.add_argument("--max-first-page-ms", type=float, default=1_000)
    parser.add_argument("--max-route-queries", type=int, default=25)
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    seen: set[int] = set()
    cursor = ""
    first_page_ms = 0.0
    discovery_queries = -1
    while True:
        query = urllib.parse.urlencode({"limit": 50, "cursor": cursor})
        payload, elapsed, query_count = get_json(f"{base}/api/v2/discovery?{query}")
        if not seen:
            first_page_ms = elapsed
            discovery_queries = query_count
        for item in payload["items"]:
            identifier = int(item["id"])
            if identifier in seen:
                raise RuntimeError(f"duplicate discovery cursor item: {identifier}")
            seen.add(identifier)
        cursor = str(payload.get("next_cursor") or "")
        if not cursor:
            break

    groups, group_ms, group_queries = get_json(f"{base}/api/v2/job-groups?state=queued")
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
    route_probes = {
        "library": "/api/v2/library?limit=30",
        "updates": "/api/v2/updates?limit=20",
        "matches": "/api/v2/matches?limit=24",
        "activity": "/api/v2/activity?limit=100",
        "operations": "/api/v2/operations",
    }
    measurements = {
        "discovery": {"milliseconds": first_page_ms, "queries": discovery_queries},
        "job_groups": {"milliseconds": group_ms, "queries": group_queries},
    }
    for name, path in route_probes.items():
        _payload, elapsed, query_count = get_json(f"{base}{path}")
        measurements[name] = {"milliseconds": elapsed, "queries": query_count}
    missing_headers = [name for name, value in measurements.items() if value["queries"] < 0]
    if missing_headers:
        raise RuntimeError(f"missing SQL measurement headers: {', '.join(missing_headers)}")
    excessive_queries = {
        name: value["queries"]
        for name, value in measurements.items()
        if value["queries"] > args.max_route_queries
    }
    if excessive_queries:
        raise RuntimeError(
            f"route query budget exceeded ({args.max_route_queries}): {excessive_queries}"
        )
    slow_routes = {
        name: round(value["milliseconds"], 2)
        for name, value in measurements.items()
        if value["milliseconds"] >= args.max_first_page_ms
    }
    if slow_routes:
        raise RuntimeError(
            f"route latency budget exceeded ({args.max_first_page_ms:.0f}ms): {slow_routes}"
        )
    print(
        json.dumps(
            {
                "discovery_first_page_ms": round(first_page_ms, 2),
                "discovery_rows": len(seen),
                "job_groups_first_page_ms": round(group_ms, 2),
                "maintenance_tasks": maintenance[0]["task_count"],
                "route_measurements": {
                    name: {
                        "milliseconds": round(value["milliseconds"], 2),
                        "queries": value["queries"],
                    }
                    for name, value in measurements.items()
                },
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
