"""Regression tests for the two permit-accounting defects found 2026-11-03."""
import asyncio
import pytest


@pytest.mark.parametrize("limit", [1, 2, 5])
def test_hand_off_does_not_destroy_a_slot(limit):
    """A queued waiter must not consume a permit twice.

    Before the fix ``release()`` incremented the count and then also
    decremented it on the waiter's behalf, while the waiter never claimed the
    permit. One hand-off cycle left the semaphore permanently one slot short:
    with ``limit=1`` the next ``acquire()`` blocked forever.
    """
    from tiny_semaphore import AsyncSemaphore

    async def main():
        sem = AsyncSemaphore(limit=limit)
        # Exhaust every slot, then queue exactly one waiter behind them.
        for _ in range(limit):
            await sem.acquire()
        waiter = asyncio.create_task(sem.acquire())
        await asyncio.sleep(0.01)
        assert not waiter.done(), "waiter should be queued"

        sem.release()
        await asyncio.sleep(0.02)
        assert waiter.done(), "release() must wake the queued waiter"
        await waiter

        # limit-1 holders remain, plus the waiter: every slot is accounted for.
        # The invariant is _value == limit - _active; before the fix one slot
        # vanished here and the semaphore could never reach full capacity again.
        assert sem._active == limit, f"expected {limit} holders, got {sem._active}"
        assert sem._value == 0, f"expected 0 free slots, got {sem._value}"

    asyncio.run(main())


def test_one_slot_semaphore_is_usable_after_a_hand_off():
    """The pathological case: limit=1, one hand-off, then reuse."""
    from tiny_semaphore import AsyncSemaphore

    async def main():
        sem = AsyncSemaphore(limit=1)
        await sem.acquire()
        waiter = asyncio.create_task(sem.acquire())
        await asyncio.sleep(0.01)
        sem.release()
        await asyncio.sleep(0.02)
        await waiter
        sem.release()
        await asyncio.sleep(0.02)

        assert sem._value == 1, f"idle semaphore must offer 1 slot, got {sem._value}"
        # Would time out before the fix.
        await asyncio.wait_for(sem.acquire(), timeout=1)

    asyncio.run(main())


def test_semaphore_module_never_grants_more_than_max_permits():
    """``semaphore.Semaphore`` must not hand out a second permit to a waiter."""
    from semaphore import Semaphore

    async def main():
        sem = Semaphore(max_permits=1)
        await sem.acquire()

        waiter = asyncio.create_task(sem.acquire())
        await asyncio.sleep(0.01)
        assert not waiter.done()

        sem.release()
        await asyncio.sleep(0.01)
        assert waiter.done(), "release() must wake the waiter"
        assert sem.try_acquire() is False, (
            "a 1-permit semaphore handed out a second permit while the first is held"
        )

    asyncio.run(main())
