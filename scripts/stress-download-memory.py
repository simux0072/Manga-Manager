from __future__ import annotations

import asyncio
from pathlib import Path

from app.adapters.http import iter_ordered_bytes
from app.settings import settings


class SyntheticClient:
    base_url = "https://mangafire.memory.test"

    async def get_bytes(self, _url: str, referer: str = "") -> bytes:
        await asyncio.sleep(0)
        return bytes(settings.max_page_bytes)


async def consume(job: int) -> int:
    total = 0
    urls = [f"https://mangafire.memory.test/{job}/{page}" for page in range(12)]
    async for page in iter_ordered_bytes(SyntheticClient(), urls, concurrency=4):
        total += len(page)
        await asyncio.sleep(0.01)
    return total


async def main() -> None:
    totals = await asyncio.gather(*(consume(job) for job in range(4)))
    peak_path = Path("/sys/fs/cgroup/memory.peak")
    peak = int(peak_path.read_text().strip()) if peak_path.is_file() else 0
    limit = 1024 * 1024 * 1024
    print(f"jobs=4 bytes={sum(totals)} memory_peak={peak} limit={limit}")
    if peak and peak >= limit:
        raise SystemExit("worker memory peak reached the 1 GiB limit")


if __name__ == "__main__":
    asyncio.run(main())
