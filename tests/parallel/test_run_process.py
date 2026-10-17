from __future__ import annotations

import contextlib
import multiprocessing
import re
import time
import traceback
import typing
import unittest

import seamstress
from seamstress.parallel.run_process import (
    ExceptionTooLargeToPropagate,
    ProcessStillAlive,
)

if typing.TYPE_CHECKING:
    from multiprocessing.synchronize import Lock as MultiprocessingLock


def build_process_lock_acquirer(
    *,
    lock: MultiprocessingLock,
) -> contextlib.AbstractContextManager[None]:
    """
    See docstring of `build_threading_lock_acquirer`.
    """

    @contextlib.contextmanager
    def lock_acquirer() -> typing.Generator[None]:
        with lock:
            yield

    return lock_acquirer()


def build_slow_release_process_lock_acquirer(
    *,
    lock: MultiprocessingLock,
) -> contextlib.AbstractContextManager[None]:
    """
    See docstring of `build_slow_release_thread_lock_acquirer`.
    """

    @contextlib.contextmanager
    def lock_acquirer() -> typing.Generator[None]:
        with lock:
            yield
            time.sleep(1.5)

    return lock_acquirer()


class PropagatedException(Exception):
    pass


@contextlib.contextmanager
def raise_exception_on_entry() -> typing.Generator[None]:
    raise PropagatedException
    yield


@contextlib.contextmanager
def raise_exception_on_exit() -> typing.Generator[None]:
    yield
    raise PropagatedException


class VeryLargeException(Exception):
    pass


INTEGER_LARGER_THAN_DEFAULT_SHARED_MEMORY_SIZE = 2_000


@contextlib.contextmanager
def raise_very_large_exception_on_entry() -> typing.Iterator[None]:
    raise VeryLargeException("a" * INTEGER_LARGER_THAN_DEFAULT_SHARED_MEMORY_SIZE)
    yield


@contextlib.contextmanager
def raise_very_large_exception_on_exit() -> typing.Iterator[None]:
    yield
    raise VeryLargeException("a" * INTEGER_LARGER_THAN_DEFAULT_SHARED_MEMORY_SIZE)


@unittest.skipIf(
    multiprocessing.get_start_method() != "fork",
    reason=(
        '`run_process` only works when multiprocessing start method is "fork" - '
        'support for "spawn" and "forkserver" to be added in due course.'
    ),
)
class TestRunProcess(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = multiprocessing.Lock()
        return super().setUp()

    def _lock_is_locked(self) -> bool:
        """
        If we managed to acquire the lock, it wasn't locked/acquired by another
        process, so we use that value to determine the return value of this function.
        However, we don't want to pollute test state, so if we did manage to acquire the
        lock, release it so it goes back into its unlocked/unacquired state.
        """
        # TODO: replace this method with calls to lock.locked() when python 3.14 becomes
        # the minimum supported version.
        acquired_lock = self.lock.acquire(block=False)
        if acquired_lock:
            self.lock.release()
        return not acquired_lock

    def test_runs_context_manager_to_yield_on_entry(self) -> None:
        """
        See docstring in `TestRunProcess`.
        """
        lock_acquirer = build_process_lock_acquirer(lock=self.lock)

        # safety check
        self.assertFalse(
            self._lock_is_locked(),
            msg="Lock acquired before run_process' context was entered.",
        )

        with seamstress.run_process(lock_acquirer):
            self.assertTrue(
                self._lock_is_locked(),
                msg="Lock not acquired when run_process' context had been entered.",
            )

        self.assertFalse(
            self._lock_is_locked(),
            msg="Lock not released after exiting run_process' context.",
        )

    def test_raises_if_process_still_alive_after_timeout(self) -> None:
        slow_release_lock_acquirer = build_slow_release_process_lock_acquirer(
            lock=self.lock,
        )

        with self.assertRaises(ProcessStillAlive):
            with seamstress.run_process(
                slow_release_lock_acquirer,
                timeout=0.01,
            ):
                pass

    def test_process_still_alive_exception_message(self) -> None:
        slow_release_lock_acquirer = build_slow_release_process_lock_acquirer(
            lock=self.lock,
        )

        passed_timeout = 0.01

        expected_error_message = f'The process running "lock_acquirer" was still alive after {passed_timeout} seconds.'
        with self.assertRaisesRegex(
            ProcessStillAlive,
            expected_error_message,
        ):
            with seamstress.run_process(
                slow_release_lock_acquirer,
                timeout=passed_timeout,
            ):
                pass

    def test_process_still_alive_exception_message_with_one_second_timeout(
        self,
    ) -> None:
        slow_release_lock_acquirer = build_slow_release_process_lock_acquirer(
            lock=self.lock,
        )

        passed_timeout = 1.0

        expected_error_message = (
            'The process running "lock_acquirer" was still alive after 1 second.'
        )
        with self.assertRaisesRegex(
            ProcessStillAlive,
            expected_error_message,
        ):
            with seamstress.run_process(
                slow_release_lock_acquirer,
                timeout=passed_timeout,
            ):
                pass

    def test_propagates_exception_raised_on_entry_back_to_spawning_process(
        self,
    ) -> None:
        """
        Verify that if the context manager passed to `run_process` raises an exception
        before yielding (i.e. on entry), this exception is raised back in the process
        that spawned the new process.
        """
        with self.assertRaises(PropagatedException) as cm:
            with seamstress.run_process(raise_exception_on_entry()):
                pass

        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            ),
        )

        self.assertIn(
            'Raised by "raise_exception_on_entry" passed to `seamstress.run_process`.',
            printed_output,
        )

    def test_propagates_exception_raised_on_exit_back_to_spawning_process(self) -> None:
        """
        Verify that if the context manager passed to `run_process` raises an exception
        after it's yield statement (i.e. on exit), this exception is raised back in the
        process that spawned the new process.
        """
        with self.assertRaises(PropagatedException) as cm:
            with seamstress.run_process(raise_exception_on_exit()):
                pass

        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            ),
        )

        self.assertIn(
            'Raised by "raise_exception_on_exit" passed to `seamstress.run_process`.',
            printed_output,
        )

    def _extract_suggested_shared_memory_size_from_printed_exception_output(
        self,
        *,
        printed_output: str,
    ) -> int:
        suggestion_match = re.search(r"shared_memory_size=(\d+)", printed_output)
        if not suggestion_match:
            self.fail(
                "No suggestion for shared memory size in raised exception's output",
            )
        return int(suggestion_match.group(1))

    def test_raises_when_propagated_exception_from_entry_too_large_for_default_shared_memory_size(
        self,
    ) -> None:
        """
        If the propagated exception exceeds the size of the shared memory used to pass
        the exception from the spawned process to the process that called `run_process`,
        `ExceptionTooLargeToPropagate` should be raised.

        This test tests the case that the exception is raised on context manager entry.

        The stack trace for the raised `ExceptionTooLargeToPropagate` exception should
        let the user know how much memory is required to propagate the exception.
        Extract this value, and verify that the exception does indeed get propagated
        when that much memory is used.
        """
        with self.subTest("The appropriate exception was raised"):
            with self.assertRaises(ExceptionTooLargeToPropagate) as cm:
                with seamstress.run_process(raise_very_large_exception_on_entry()):
                    pass

        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            ),
        )

        with self.subTest("The exception's traceback was sufficiently helpful"):
            self.assertIn(
                "Please tweak the call to `run_process` in this test to include the "
                "keyword argument `shared_memory_size=",
                printed_output,
            )

        new_shared_memory_size = (
            self._extract_suggested_shared_memory_size_from_printed_exception_output(
                printed_output=printed_output,
            )
        )

        with self.subTest(
            "Using the suggested shared memory size successfully propagates the large "
            "exception",
        ):
            with self.assertRaises(VeryLargeException):
                with seamstress.run_process(
                    raise_very_large_exception_on_entry(),
                    shared_memory_size=new_shared_memory_size,
                ):
                    pass

    def test_raises_when_propagated_exception_from_exit_too_large_for_default_shared_memory_size(
        self,
    ) -> None:
        """
        If the propagated exception exceeds the size of the shared memory used to pass
        the exception from the spawned process to the process that called `run_process`,
        `ExceptionTooLargeToPropagate` should be raised.

        This test tests the case that the exception is raised on context manager exit.

        The stack trace for the raised `ExceptionTooLargeToPropagate` exception should
        let the user know how much memory is required to propagate the exception.
        Extract this value, and verify that the exception does indeed get propagated
        when that much memory is used.
        """
        with self.subTest("The appropriate exception was raised"):
            with self.assertRaises(ExceptionTooLargeToPropagate) as cm:
                with seamstress.run_process(raise_very_large_exception_on_exit()):
                    pass

        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            ),
        )

        with self.subTest("The exception's traceback was sufficiently helpful"):
            self.assertIn(
                "Please tweak the call to `run_process` in this test to include the "
                "keyword argument `shared_memory_size=",
                printed_output,
            )

        new_shared_memory_size = (
            self._extract_suggested_shared_memory_size_from_printed_exception_output(
                printed_output=printed_output,
            )
        )

        with self.subTest(
            "Using the suggested shared memory size successfully propagates the large "
            "exception",
        ):
            with self.assertRaises(VeryLargeException):
                with seamstress.run_process(
                    raise_very_large_exception_on_exit(),
                    shared_memory_size=new_shared_memory_size,
                ):
                    pass
