"""Regression tests for the permit hand-off in semaphore.Semaphore.

Before the fix, `release()` credited the permit back to `_permits` *and*
woke a waiter. The waiter returned from `acquire()` without taking the
permit, so the permit was counted twice. Because `release()` refuses to
raise `_permits` past `max_permits`, the pool filled up and `_wake_one()`
stopped being called -- every remaining waiter was stranded forever.

The visible symptom was `test_concurrent_acquisition` hanging the whole
suite: 5 workers against `max_permits=2` could only ever complete 4.
"""

from __future__ import annotations

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from semaphore import Semaphore  # noqa: E402


def _run(coro, timeout=5):
    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


def test_more_waiters_than_permits_all_complete():
    """5 workers on 2 permits: every one must finish, not just the first 4."""
    done = []

    async def main():
        sem = Semaphore(max_permits=2)

        async def worker(n):
            async with sem:
                done.append(n)
                await asyncio.sleep(0.01)

        await asyncio.gather(*[worker(i) for i in range(5)])

    _run(main())
    assert sorted(done) == [0, 1, 2, 3, 4], "a waiter was starved: %s" % sorted(done)


def test_permit_count_returns_to_max_after_contention():
    """After everyone releases, the pool must be full again."""
    async def main():
        sem = Semaphore(max_permits=3)

        async def worker():
            async with sem:
                await asyncio.sleep(0.01)

        await asyncio.gather(*[worker() for _ in range(10)])
        return sem

    sem = _run(main())
    assert sem.available == 3, "permits leaked or inflated: %s" % sem.available


def test_concurrency_never_exceeds_max_permits():
    """The fix must not over-grant permits to keep waiters moving."""
    current = 0
    peak = 0

    async def main():
        nonlocal current, peak
        sem = Semaphore(max_permits=2)

        async def worker():
            nonlocal current, peak
            async with sem:
                current += 1
                peak = max(peak, current)
                await asyncio.sleep(0.02)
                current -= 1

        await asyncio.gather(*[worker() for _ in range(20)])

    _run(main())
    assert peak <= 2, "over-granted: %d tasks held a permit at once (max 2)" % peak


def test_release_without_waiters_returns_permit_to_pool():
    """A release with nobody waiting must credit the pool exactly once."""
    async def main():
        sem = Semaphore(max_permits=2)
        await sem.acquire()
        await sem.acquire()
        assert sem.available == 0
        sem.release()
        assert sem.available == 1, "release did not return a permit"
        sem.release()
        assert sem.available == 2, "release did not return the second permit"
        sem.release()  # extra release must not inflate past the cap
        return sem

    sem = _run(main())
    assert sem.available == 2, "extra release inflated the pool: %s" % sem.available
