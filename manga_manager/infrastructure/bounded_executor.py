from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any


class AsyncBoundedExecutor:
    """Small explicit worker lane with a portable asyncio completion bridge."""

    def __init__(self, *, workers: int, thread_name_prefix: str) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, workers),
            thread_name_prefix=thread_name_prefix,
        )

    async def run(self, function, /, *args, **kwargs) -> Any:
        future = self._executor.submit(partial(function, *args, **kwargs))
        try:
            # Python 3.14 currently stalls the default run_in_executor bridge on
            # the old validation host. Polling also keeps cancellation behavior
            # identical on the Python 3.12 deployment image.
            while not future.done():
                await asyncio.sleep(0.005)
            return future.result()
        except asyncio.CancelledError:
            future.cancel()
            raise

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=False)
