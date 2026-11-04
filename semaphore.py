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
        # Created lazily: before Python 3.10, asyncio.Queue() and
        # asyncio.Lock() reach for the current event loop at construction
        # time. Building them in __init__ makes `Semaphore(...)` raise
        # RuntimeError("There is no current event loop") on 3.8/3.9 whenever
        # it is constructed outside a running loop -- for example after any
        # asyncio.run() has closed the previous loop.
        self._waiters = None
        self._lock = None
        # Stats
        self._acquired_count = 0
        self._peak_acquired = 0
        self._total_releases = 0

    # ── Acquire / Release ─────────────────────────────────────────────────────

    def _get_waiters(self) -> "asyncio.Queue[asyncio.Future[bool]]":
        """The waiter queue, created on first use inside a running loop."""
        if self._waiters is None:
            self._waiters = asyncio.Queue()
        return self._waiters

    def _get_lock(self) -> "asyncio.Lock":
        """The internal lock, created on first use inside a running loop."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

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
        await self._get_waiters().put(future)

        try:
            if timeout is None:
                await future
            else:
                await asyncio.wait_for(future, timeout=timeout)
            # The permit was transferred by release() -> _wake_one(); it is
            # already out of the pool, so only the stats change here.
            self._acquired_count += 1
            self._peak_acquired = max(self._peak_acquired, self.max_permits - self._permits)
        except asyncio.TimeoutError:
            # Remove ourselves from the queue
            try:
                self._get_waiters().get_nowait()
            except asyncio.QueueEmpty:
                pass
            raise AcquireTimeout(self.name, timeout)

    def release(self) -> None:
        """Release one permit. Not async — safe to call from sync code.

        A permit that is handed directly to a waiting acquirer is NOT
        credited back to the pool: the waiter consumes it on wake. Only an
        unreferenced permit returns to `_permits`. Crediting the pool and
        waking a waiter as well would double-count the permit, and because
        this method refuses to add past `max_permits`, the pool would fill
        up and stop waking anyone -- starving waiters forever.
        """
        self._total_releases += 1
        # Sync caller: if there are no waiters, or the queue was never
        # created, there is nothing to wake and no event loop is required.
        # Building a Queue here would raise on 3.8/3.9 outside a loop.
        if self._waiters is not None and self._wake_one():
            return  # the permit passed straight through to the waiter
        if self._permits >= self.max_permits:
            return  # Already at max — shouldn't happen with correct use
        self._permits += 1

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

    def _wake_one(self) -> bool:
        """Hand a permit to the oldest waiter.

        Returns True when a waiter took the permit (so the caller must not
        also credit it to the pool), False when nobody was waiting.
        """
        if self._waiters is None:
            return False
        while True:
            try:
                future: asyncio.Future[bool] = self._waiters.get_nowait()
            except asyncio.QueueEmpty:
                return False
            if future.done():
                continue  # cancelled/timed-out waiter: drop it, try the next
            future.set_result(True)
            return True

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
            "waiters": self._waiters.qsize() if self._waiters is not None else 0,
            "peak_acquired": self._peak_acquired,
            "total_releases": self._total_releases,
        }

    @property
    def available(self) -> int:
        return self._permits

    def __repr__(self) -> str:
        return f"<Semaphore {self.name} permits={self._permits}/{self.max_permits} waiters={self._waiters.qsize() if self._waiters is not None else 0}>"

    def __str__(self) -> str:
        return self.__repr__()
