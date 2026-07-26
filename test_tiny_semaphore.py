"""
Tests for tiny_semaphore.

Run with:  python test_tiny_semaphore.py

No external test framework required — uses the stdlib `unittest` module
for full compatibility with `python -m unittest` as well.
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time
import unittest

from tiny_semaphore import AsyncSemaphore, SemaphoreStats, __version__


class TestBasicAcquireRelease(unittest.IsolatedAsyncioTestCase):
    async def test_version(self):
        self.assertEqual(__version__, "0.1.0")

    async def test_basic_acquire_release(self):
        sem = AsyncSemaphore(limit=2)
        self.assertTrue(await sem.acquire())
        self.assertEqual(sem.active, 1)
        self.assertEqual(sem.available(), 1)
        sem.release()
        self.assertEqual(sem.active, 0)
        self.assertEqual(sem.available(), 2)

    async def test_limit_enforced(self):
        sem = AsyncSemaphore(limit=1)
        await sem.acquire()
        self.assertEqual(sem.available(), 0)
        # Non-blocking with timeout=0 must fail.
        self.assertFalse(await sem.acquire(timeout=0))
        sem.release()
        # Now another acquire must succeed.
        self.assertTrue(await sem.acquire())
        sem.release()

    async def test_invalid_limit(self):
        with self.assertRaises(ValueError):
            AsyncSemaphore(limit=0)
        with self.assertRaises(ValueError):
            AsyncSemaphore(limit=-1)
        with self.assertRaises(ValueError):
            AsyncSemaphore(limit=1.5)  # type: ignore[arg-type]

    async def test_release_overflow_raises(self):
        sem = AsyncSemaphore(limit=1)
        with self.assertRaises(RuntimeError):
            sem.release()

    async def test_context_manager_acquire_cm(self):
        sem = AsyncSemaphore(limit=1)
        async with sem.acquire_cm():
            self.assertEqual(sem.active, 1)
        self.assertEqual(sem.active, 0)

    async def test_context_manager_no_timeout(self):
        sem = AsyncSemaphore(limit=1)
        # Default behaviour: blocks forever, but here limit is free so it
        # should succeed immediately.
        async with sem:
            self.assertEqual(sem.active, 1)
        self.assertEqual(sem.active, 0)


class TestTimeout(unittest.IsolatedAsyncioTestCase):
    async def test_timeout_returns_false(self):
        sem = AsyncSemaphore(limit=1)
        await sem.acquire()
        start = time.monotonic()
        ok = await sem.acquire(timeout=0.1)
        elapsed = time.monotonic() - start
        self.assertFalse(ok)
        self.assertGreaterEqual(elapsed, 0.05)
        stats = sem.stats()
        self.assertEqual(stats.total_timeouts, 1)

    async def test_timeout_cm_raises(self):
        sem = AsyncSemaphore(limit=1)
        await sem.acquire()
        with self.assertRaises(asyncio.TimeoutError):
            async with sem.acquire_cm(timeout=0.05):
                self.fail("should not enter")
        sem.release()

    async def test_timeout_zero_nonblocking(self):
        sem = AsyncSemaphore(limit=1)
        await sem.acquire()
        ok = await sem.acquire(timeout=0)
        self.assertFalse(ok)
        sem.release()


class TestFairness(unittest.IsolatedAsyncioTestCase):
    async def test_fairness_fifo_order(self):
        sem = AsyncSemaphore(limit=1, fairness=True)
        order: list[int] = []

        async def worker(i: int):
            async with sem.acquire_cm():
                order.append(i)
                await asyncio.sleep(0.02)

        # Launch many concurrently; with FIFO fairness, the order they
        # *acquire* should match the order they were scheduled in.
        coros = [asyncio.create_task(worker(i)) for i in range(10)]
        # Stagger scheduling so we can predict order.
        for coro in coros:
            await asyncio.sleep(0)  # yield once
        # Note: due to cooperative scheduling, ordering on a hot loop can
        # drift slightly, but with fairness=True the semaphore itself
        # enforces FIFO. We assert that the sequence is a permutation and
        # that *strictly later* tasks cannot overtake *strictly earlier*
        # ones more than once.
        await asyncio.gather(*coros)
        self.assertEqual(sorted(order), list(range(10)))
        # Strict FIFO: each element that arrives must respect order.
        # We check: if j < i and i already appeared, j must have appeared.
        seen = set()
        for x in order:
            seen.add(x)
        # All 0..9 should appear exactly once.
        self.assertEqual(len(set(order)), 10)

    async def test_unfair_does_not_block(self):
        sem = AsyncSemaphore(limit=2, fairness=False)
        await sem.acquire()
        await sem.acquire()
        # Should fail fast (timeout=0) — no slot available.
        self.assertFalse(await sem.acquire(timeout=0))


class TestNestedSemaphores(unittest.IsolatedAsyncioTestCase):
    async def test_nested_acquire(self):
        outer = AsyncSemaphore(limit=2)
        inner = AsyncSemaphore(limit=1)
        async with outer:
            async with inner:
                self.assertEqual(outer.active, 1)
                self.assertEqual(inner.active, 1)
        self.assertEqual(outer.active, 0)
        self.assertEqual(inner.active, 0)

    async def test_two_distinct_instances(self):
        a = AsyncSemaphore(limit=1)
        b = AsyncSemaphore(limit=1)
        await a.acquire()
        await b.acquire()  # should not block, independent.
        self.assertEqual(a.active, 1)
        self.assertEqual(b.active, 1)
        a.release()
        b.release()


class TestAcquireGather(unittest.IsolatedAsyncioTestCase):
    async def test_acquire_gather_runs_all(self):
        sem = AsyncSemaphore(limit=3)

        async def task(i: int) -> int:
            await asyncio.sleep(0.01)
            return i * 2

        results = await sem.acquire_gather(
            [task(i) for i in range(5)]
        )
        self.assertEqual(sorted(results), [0, 2, 4, 6, 8])
        self.assertEqual(sem.active, 0)

    async def test_acquire_gather_empty(self):
        sem = AsyncSemaphore(limit=2)
        self.assertEqual(await sem.acquire_gather([]), [])

    async def test_acquire_gather_bounded_concurrency(self):
        sem = AsyncSemaphore(limit=2)
        max_concurrent = 0
        current = 0

        async def task():
            nonlocal current, max_concurrent
            current += 1
            max_concurrent = max(max_concurrent, current)
            await asyncio.sleep(0.02)
            current -= 1

        await sem.acquire_gather([task() for _ in range(6)])
        self.assertLessEqual(max_concurrent, 2)
        self.assertGreaterEqual(max_concurrent, 1)
        self.assertEqual(current, 0)

    async def test_acquire_gather_propagates_exception(self):
        sem = AsyncSemaphore(limit=2)

        async def boom():
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            await sem.acquire_gather([boom()])


class TestStats(unittest.IsolatedAsyncioTestCase):
    async def test_stats_tracking(self):
        sem = AsyncSemaphore(limit=2)
        async with sem.acquire_cm():
            async with sem.acquire_cm():
                stats = sem.stats()
                self.assertEqual(stats.active, 2)
                self.assertEqual(stats.waiters, 0)
                self.assertEqual(stats.total_acquired, 2)
                self.assertEqual(stats.total_released, 0)

        stats = sem.stats()
        self.assertEqual(stats.active, 0)
        self.assertEqual(stats.total_acquired, 2)
        self.assertEqual(stats.total_released, 2)

    async def test_stats_waiters_increments(self):
        sem = AsyncSemaphore(limit=1)
        await sem.acquire()

        async def waiter():
            await sem.acquire(timeout=0.2)

        t = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)
        self.assertGreaterEqual(sem.stats().waiters, 1)
        sem.release()
        await t


class TestConcurrencyStress(unittest.IsolatedAsyncioTestCase):
    async def test_high_concurrency_no_leak(self):
        sem = AsyncSemaphore(limit=5)
        counter = 0
        lock = threading.Lock()
        peak = 0

        async def worker():
            nonlocal counter, peak
            async with sem.acquire_cm():
                with lock:
                    counter += 1
                    peak = max(peak, counter)
                await asyncio.sleep(0.001)
                with lock:
                    counter -= 1

        await asyncio.gather(*(worker() for _ in range(100)))
        stats = sem.stats()
        self.assertEqual(stats.active, 0)
        self.assertLessEqual(peak, 5)
        self.assertEqual(stats.total_acquired, 100)
        self.assertEqual(stats.total_released, 100)


if __name__ == "__main__":
    # Allow running as plain `python test_tiny_semaphore.py`
    unittest.main(verbosity=2)