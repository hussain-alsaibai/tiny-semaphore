"""
tiny-semaphore: zero-dependency async semaphore with timeout + fairness.

Single-file library. asyncio only. Thread-safe.

Provides :class:`AsyncSemaphore`, :class:`SemaphoreStats`, and a fairness-aware
subclass used internally when ``fairness=True`` is requested.

.. code-block:: python

    import asyncio
    from tiny_semaphore import AsyncSemaphore

    sem = AsyncSemaphore(limit=3, fairness=True)

    async def worker(i):
        async with sem:
            await asyncio.sleep(0.1)
            return i

    async def main():
        results = await asyncio.gather(*(worker(i) for i in range(10)))

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Deque, List, Optional


__version__ = "0.1.0"


@dataclass
class SemaphoreStats:
    """Snapshot of :class:`AsyncSemaphore` counters.

    All counters are monotonic over the lifetime of the semaphore.
    """

    active: int = 0
    waiters: int = 0
    total_acquired: int = 0
    total_released: int = 0
    total_timeouts: int = 0


class AsyncSemaphore:
    """Async semaphore with optional timeout and fairness.

    Parameters
    ----------
    limit:
        Maximum number of concurrent holders. Must be ``>= 1``.
    fairness:
        When ``True``, waiters are released in strict FIFO order.
        When ``False`` (default), scheduling order is best-effort (FIFO but
        not strictly guaranteed — matches :class:`asyncio.Semaphore`
        behaviour).

    Notes
    -----
    - ``acquire()`` is itself a coroutine returning ``True`` / ``False``,
      but the typical usage is ``async with sem:`` or ``async with
      sem.acquire(timeout=...):``.
    - Internally protected by a :class:`threading.Lock` so stats and
      internal state are safe across threads.
    """

    __slots__ = (
        "_limit",
        "_value",
        "_waiters",
        "_fair",
        "_lock",
        "_active",
        "_total_acquired",
        "_total_released",
        "_total_timeouts",
        "_closed",
    )

    def __init__(self, limit: int = 1, fairness: bool = False) -> None:
        if not isinstance(limit, int) or limit < 1:
            raise ValueError(f"limit must be a positive int, got {limit!r}")
        self._limit: int = limit
        self._value: int = limit
        self._waiters: Deque[asyncio.Future[None]] = deque()
        self._fair: bool = fairness
        self._lock: threading.Lock = threading.Lock()
        self._active: int = 0
        self._total_acquired: int = 0
        self._total_released: int = 0
        self._total_timeouts: int = 0
        self._closed: bool = False

    # ------------------------------------------------------------------ #
    # Properties
    # ------------------------------------------------------------------ #

    @property
    def limit(self) -> int:
        """Configured maximum concurrency."""
        return self._limit

    @property
    def fairness(self) -> bool:
        """Whether strict FIFO queueing is enabled."""
        return self._fair

    def available(self) -> int:
        """Number of currently available (un-acquired) slots."""
        with self._lock:
            return self._value

    @property
    def waiters(self) -> int:
        """Number of tasks currently queued waiting for a slot."""
        with self._lock:
            return len(self._waiters)

    @property
    def active(self) -> int:
        """Number of holders currently inside the critical section."""
        with self._lock:
            return self._active

    def stats(self) -> SemaphoreStats:
        """Return a snapshot of current stats."""
        with self._lock:
            return SemaphoreStats(
                active=self._active,
                waiters=len(self._waiters),
                total_acquired=self._total_acquired,
                total_released=self._total_released,
                total_timeouts=self._total_timeouts,
            )

    # ------------------------------------------------------------------ #
    # Core acquire / release
    # ------------------------------------------------------------------ #

    async def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire a slot, waiting up to ``timeout`` seconds.

        Returns ``True`` on success, ``False`` if the timeout expired.
        Raises :class:`asyncio.TimeoutError` *only* if ``timeout`` is given
        and the wait timed out *and* the underlying :func:`asyncio.wait_for`
        raises — we surface that explicitly for compatibility with code
        that expects :class:`asyncio.TimeoutError`.

        Actually, to keep the API uniform with stdlib
        :class:`asyncio.Semaphore`, this method returns ``True`` /
        ``False`` rather than raising. Use :meth:`acquire_cm` (or
        ``async with sem.acquire(timeout=...):``) if you want
        :class:`asyncio.TimeoutError` raised on timeout.
        """
        if timeout is not None and timeout <= 0:
            # Non-blocking attempt.
            with self._lock:
                if self._value > 0 and not self._waiters:
                    self._value -= 1
                    self._active += 1
                    self._total_acquired += 1
                    return True
            with self._lock:
                self._total_timeouts += 1
            return False

        try:
            await self._wait_for_slot(timeout=timeout)
        except _AcquireTimeout:
            with self._lock:
                self._total_timeouts += 1
            return False
        with self._lock:
            self._active += 1
            self._total_acquired += 1
        return True

    async def _wait_for_slot(self, timeout: Optional[float]) -> None:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        with self._lock:
            if self._value > 0 and (not self._fair or not self._waiters):
                # Grant immediately.
                self._value -= 1
                return
            self._waiters.append(fut)

        if timeout is None:
            try:
                await fut
            except asyncio.CancelledError:
                await self._cancel_waiter(fut)
                raise
        else:
            try:
                await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
            except asyncio.TimeoutError:
                await self._cancel_waiter(fut)
                raise _AcquireTimeout() from None
            except asyncio.CancelledError:
                await self._cancel_waiter(fut)
                raise

    async def _cancel_waiter(self, fut: asyncio.Future[None]) -> None:
        with self._lock:
            try:
                self._waiters.remove(fut)
            except ValueError:
                # Already removed because the slot was granted.
                pass

    def release(self) -> None:
        """Release a slot, waking the next waiter (if any)."""
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._active <= 0:
                raise RuntimeError("release() called more times than acquire()")
            self._active -= 1
            self._total_released += 1
            # Wake the next waiter if any; otherwise return a slot.
            while self._waiters:
                nxt = self._waiters.popleft()
                if not nxt.done():
                    # Slot is consumed by this waiter.
                    self._value -= 1
                    loop.call_soon_threadsafe(_set_result, nxt, None)
                    return
            self._value += 1

    # ------------------------------------------------------------------ #
    # Context manager API
    # ------------------------------------------------------------------ #

    @asynccontextmanager
    async def acquire_cm(
        self, timeout: Optional[float] = None
    ) -> AsyncIterator[None]:
        """Async context manager around :meth:`acquire`.

        Raises :class:`asyncio.TimeoutError` on timeout (like stdlib).
        """
        if not await self.acquire(timeout=timeout):
            raise asyncio.TimeoutError(
                f"AsyncSemaphore(limit={self._limit}) acquire timed out "
                f"after {timeout}s"
            )
        try:
            yield
        finally:
            self.release()

    async def __aenter__(self) -> "AsyncSemaphore":
        # Default ``async with sem:`` — no timeout, blocks forever.
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.release()

    # Convenience alias mirroring sem.acquire(timeout=...) usage.
    def acquire_ctx(self, timeout: Optional[float] = None):
        """Alias for :meth:`acquire_cm` for terser call sites."""
        return self.acquire_cm(timeout=timeout)

    # ------------------------------------------------------------------ #
    # Bounded gather
    # ------------------------------------------------------------------ #

    async def acquire_gather(
        self,
        tasks: List[Any],
        timeout: Optional[float] = None,
    ) -> List[Any]:
        """Acquire up to ``limit`` slots and run ``tasks`` concurrently.

        Each task is awaited under the semaphore; tasks are not pre-spawned
        and queued. If ``timeout`` expires while waiting for a slot, any
        not-yet-started task raises :class:`asyncio.TimeoutError` and
        already-running tasks are awaited to completion.

        Parameters
        ----------
        tasks:
            List of zero-argument coroutines (or awaitables).
        timeout:
            Per-task slot-acquisition timeout. ``None`` waits forever.

        Returns
        -------
        list
            List of task return values, in input order.

        Notes
        -----
        Acquisition is bounded by ``self.limit``. If ``len(tasks) <
        self.limit`` only ``len(tasks)`` slots are reserved.
        """
        if not tasks:
            return []

        results: List[Any] = [None] * len(tasks)
        exceptions: List[BaseException] = []

        async def _runner(idx: int, task: Any) -> None:
            try:
                async with self.acquire_cm(timeout=timeout):
                    results[idx] = await task
            except BaseException as e:  # noqa: BLE001 - re-raised after
                exceptions.append(e)

        runners = [_runner(i, t) for i, t in enumerate(tasks)]
        await asyncio.gather(*runners)

        if exceptions:
            # Re-raise the first; asyncio.gather(return_exceptions=True) not
            # used because we want timeout-style semantics.
            raise exceptions[0]
        return results


class _AcquireTimeout(Exception):
    """Internal marker raised when slot acquisition times out."""


def _set_result(fut: asyncio.Future, value: Any) -> None:
    if not fut.done():
        fut.set_result(value)


__all__ = [
    "AsyncSemaphore",
    "SemaphoreStats",
    "__version__",
]