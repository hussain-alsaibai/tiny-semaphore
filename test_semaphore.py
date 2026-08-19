"""Tests for tiny-semaphore."""
import asyncio
import pytest
from semaphore import Semaphore, AcquireTimeout


def test_basic_acquire_release():
    sem = Semaphore(max_permits=2)
    assert sem.available == 2

    async def run():
        await sem.acquire()
        assert sem.available == 1
        sem.release()
        assert sem.available == 2

    asyncio.run(run())


def test_try_acquire():
    sem = Semaphore(max_permits=1)
    assert sem.try_acquire() is True
    assert sem.try_acquire() is False  # No permits left
    sem.release()
    assert sem.try_acquire() is True


def test_context_manager():
    sem = Semaphore(max_permits=1)

    async def run():
        async with sem:
            assert sem.available == 0
        assert sem.available == 1

    asyncio.run(run())


def test_concurrent_acquisition():
    sem = Semaphore(max_permits=2)
    acquired = []

    async def worker(n):
        async with sem:
            acquired.append(n)
            await asyncio.sleep(0.05)

    async def run():
        await asyncio.gather(*[worker(i) for i in range(5)])

    asyncio.run(run())
    assert len(acquired) == 5


def test_acquire_timeout():
    sem = Semaphore(max_permits=1)

    async def run():
        async with sem:
            with pytest.raises(AcquireTimeout):
                await sem.acquire(timeout=0.1)

    asyncio.run(run())


def test_stats():
    sem = Semaphore(max_permits=5, name="test")

    async def run():
        await sem.acquire()
        await sem.acquire()
        sem.release()
        stats = sem.get_stats()
        assert stats["name"] == "test"
        assert stats["max_permits"] == 5
        assert stats["available"] == 4
        assert stats["acquired"] == 1
        assert stats["total_releases"] == 1

    asyncio.run(run())


def test_batch_acquire():
    sem = Semaphore(max_permits=5)

    async def run():
        async with sem.acquire_many(3):
            assert sem.available == 2
        assert sem.available == 5

    asyncio.run(run())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
