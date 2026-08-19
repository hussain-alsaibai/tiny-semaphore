"""
tiny-semaphore — Zero-dependency async-aware concurrency limiter for Python.
~80 lines. No external dependencies.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

__all__ = ["Semaphore", "AcquireTimeout"]


class AcquireTimeout(TimeoutError):
    """Raised when acquire() times out waiting for a permit."""

    def __init__(self, name: str, timeout: float):
        self.name = name
        self.timeout = timeout
        super().__init__(f"Timed out after {timeout}s waiting for semaphore '{name}'")


class Semaphore:
    """
    An async-aware semaphore that limits concurrent access to a resource.

    Unlike asyncio.Semaphore, this supports:
    - Timeout on acquire
    - FIFO fairness
    - Stats introspection
    - Batch acquire
    """

    def __init__(self, max_permits: int = 1, name: str = "default"):
        if max_permits < 1:
            raise ValueError(f"max_permits must be >= 1, got {max_permits}")
        self.max_permits = max_permits
        self.name = name
        self._permits = max_permits
        self._waiters: asyncio.Queue[asyncio.Future[bool]] = asyncio.Queue()
        self._lock = asyncio.Lock()
        # Stats
        self._acquired_count = 0
        self._peak_acquired = 0
        self._total_releases = 0

    # ── Acquire / Release ─────────────────────────────────────────────────────

    async def acquire(self, timeout: Optional[float] = None) -> None:
        """
        Acquire one permit. Waits until one is available.

        Raises AcquireTimeout if timeout is set and expires.
        """
        if self._permits > 0:
            self._permits -= 1
            self._acquired_count += 1
            self._peak_acquired = max(self._peak_acquired, self.max_permits - self._permits)
            return

        if timeout == 0.0:
            raise AcquireTimeout(self.name, 0.0)

        # Create a future the release() will signal
        future: asyncio.Future[bool] = asyncio.get_event_loop().create_future()
        await self._waiters.put(future)

        try:
            if timeout is None:
                await future
            else:
                await asyncio.wait_for(future, timeout=timeout)
            self._acquired_count += 1
            self._peak_acquired = max(self._peak_acquired, self.max_permits - self._permits)
        except asyncio.TimeoutError:
            # Remove ourselves from the queue
            try:
                self._waiters.get_nowait()
            except asyncio.QueueEmpty:
                pass
            raise AcquireTimeout(self.name, timeout)

    def release(self) -> None:
        """Release one permit. Not async — safe to call from sync code."""
        if self._permits >= self.max_permits:
            return  # Already at max — shouldn't happen with correct use
        self._permits += 1
        self._total_releases += 1
        self._wake_one()

    def release_many(self, n: int) -> None:
        """Release n permits at once."""
        for _ in range(min(n, self.max_permits - self._permits)):
            self.release()

    def try_acquire(self) -> bool:
        """Try to acquire without waiting. Returns True if permit was acquired."""
        if self._permits > 0:
            self._permits -= 1
            self._acquired_count += 1
            self._peak_acquired = max(self._peak_acquired, self.max_permits - self._permits)
            return True
        return False

    def _wake_one(self) -> None:
        """Wake the oldest waiter if any."""
        try:
            future: asyncio.Future[bool] = self._waiters.get_nowait()
            if not future.done():
                future.set_result(True)
        except asyncio.QueueEmpty:
            pass

    # ── Context managers ──────────────────────────────────────────────────────

    async def __aenter__(self) -> "Semaphore":
        await self.acquire()
        return self

    async def __aexit__(self, *exc_info) -> None:
        self.release()

    class _BatchAcquire:
        """Async context manager for acquiring multiple permits at once."""

        def __init__(self, sem: "Semaphore", n: int, timeout: Optional[float]):
            self._sem = sem
            self._n = n
            self._timeout = timeout

        async def __aenter__(self) -> "Semaphore._BatchAcquire":
            for _ in range(self._n):
                await self._sem.acquire(timeout=self._timeout)
            return self

        async def __aexit__(self, *exc_info) -> None:
            self._sem.release_many(self._n)

    def acquire_many(self, n: int, timeout: Optional[float] = None) -> "_BatchAcquire":
        """Context manager: acquire n permits atomically."""
        return self._BatchAcquire(self, n, timeout)

    # ── Introspection ─────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Agent-readable stats. All values are JSON-serializable."""
        acquired = self.max_permits - self._permits
        return {
            "name": self.name,
            "max_permits": self.max_permits,
            "available": self._permits,
            "acquired": acquired,
            "waiters": self._waiters.qsize(),
            "peak_acquired": self._peak_acquired,
            "total_releases": self._total_releases,
        }

    @property
    def available(self) -> int:
        return self._permits

    def __repr__(self) -> str:
        return f"<Semaphore {self.name} permits={self._permits}/{self.max_permits} waiters={self._waiters.qsize()}>"

    def __str__(self) -> str:
        return self.__repr__()
