import asyncio
import unittest

import seamstress


class CouldNotAcquireLock(Exception):
    pass


class AsyncLockAcquirer(seamstress.TaskConfig):
    def __init__(
        self,
        *,
        lock: asyncio.Lock,
    ) -> None:
        self.lock = lock

    async def set_up_task(self) -> None:
        await self.lock.acquire()

    async def tear_down_task(self) -> None:
        self.lock.release()


class TestTaskConfig(unittest.IsolatedAsyncioTestCase):
    async def test_calls_appropriate_methods_on_entry_and_exit(self) -> None:
        lock = asyncio.Lock()

        lock_acquirer = AsyncLockAcquirer(lock=lock)

        # safety check
        self.assertFalse(
            lock.locked(),
            msg="Lock acquired before run_thread context was entered.",
        )

        async with seamstress.run_task(lock_acquirer):
            self.assertTrue(
                lock.locked(),
                msg="Lock not acquired when within run_thread context.",
            )

        self.assertFalse(
            lock.locked(),
            msg="Lock not released after exiting run_thread context.",
        )
