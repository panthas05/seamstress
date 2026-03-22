import asyncio
import contextlib
import traceback
import typing
import unittest

import seamstress
from seamstress.asynchronous.run_task import TaskStillExecuting


def build_async_lock_acquirer(
    *,
    lock: asyncio.Lock,
) -> typing.AsyncContextManager[None]:
    @contextlib.asynccontextmanager
    async def async_lock_acquirer() -> typing.AsyncIterator[None]:
        async with lock:
            yield

    return async_lock_acquirer()


def build_slow_release_async_lock_acquirer(
    *,
    lock: asyncio.Lock,
) -> typing.AsyncContextManager[None]:
    @contextlib.asynccontextmanager
    async def async_lock_acquirer() -> typing.AsyncIterator[None]:
        async with lock:
            yield
            await asyncio.sleep(1.5)

    return async_lock_acquirer()


class PropagatedException(Exception):
    pass


@contextlib.asynccontextmanager
async def raise_exception_on_entry() -> typing.AsyncIterator[None]:
    raise PropagatedException
    yield


@contextlib.asynccontextmanager
async def raise_exception_on_exit() -> typing.AsyncIterator[None]:
    yield
    raise PropagatedException


class TestAsyncHogLock(unittest.IsolatedAsyncioTestCase):
    async def test_acquires_lock_on_entry_and_releases_lock_on_exit(self) -> None:
        lock = asyncio.Lock()

        lock_acquirer = build_async_lock_acquirer(lock=lock)

        # safety check
        self.assertFalse(
            lock.locked(),
            msg="Lock acquired before run_task's context was entered.",
        )

        async with seamstress.run_task(lock_acquirer):
            self.assertTrue(
                lock.locked(),
                msg="Lock not acquired when run_task's context was entered.",
            )

        self.assertFalse(
            lock.locked(),
            msg="Lock not released after exiting run_task's context.",
        )

    async def test_raises_task_still_executing_after_timeout(
        self,
    ) -> None:
        slow_release_lock_acquirer = build_slow_release_async_lock_acquirer(
            lock=asyncio.Lock()
        )

        passed_timeout = 0.01

        with self.assertRaisesRegex(
            TaskStillExecuting,
            (
                'The task running "async_lock_acquirer" was still executing after '
                f"{passed_timeout} seconds."
            ),
        ):
            async with seamstress.run_task(
                slow_release_lock_acquirer,
                timeout=passed_timeout,
            ):
                pass

    async def test_task_still_executing_exception_message_with_one_second_timeout(
        self,
    ) -> None:
        slow_release_lock_acquirer = build_slow_release_async_lock_acquirer(
            lock=asyncio.Lock()
        )

        passed_timeout = 1.0

        with self.assertRaisesRegex(
            TaskStillExecuting,
            'The task running "async_lock_acquirer" was still executing after 1 second.',
        ):
            async with seamstress.run_task(
                slow_release_lock_acquirer,
                timeout=passed_timeout,
            ):
                pass

    async def test_propagates_exception_raised_on_context_manager_entry_back_to_test(
        self,
    ) -> None:
        with self.assertRaises(PropagatedException) as cm:
            async with seamstress.run_task(raise_exception_on_entry()):
                pass

        # verify that the printed traceback indicated that the exception had been
        # propagated from the passed context manager
        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            )
        )

        assert (
            'Raised by "raise_exception_on_entry" passed to `seamstress.run_task`.'
            in printed_output
        )

    async def test_propagates_exception_raised_on_context_manager_exit_back_to_test(
        self,
    ) -> None:
        with self.assertRaises(PropagatedException) as cm:
            async with seamstress.run_task(raise_exception_on_exit()):
                pass

        # verify that the printed traceback indicated that the exception had been
        # propagated from the passed context manager
        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            )
        )

        assert (
            'Raised by "raise_exception_on_exit" passed to `seamstress.run_task`.'
            in printed_output
        )
