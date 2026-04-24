import contextlib
import multiprocessing
import re
import threading
import time
import traceback
import typing
import unittest
from multiprocessing.synchronize import Lock as MultiprocessingLock

import seamstress
from seamstress.parallel import run_executor
from seamstress.parallel._custom_executors import ExceptionTooLargeToPropagate
from seamstress.parallel.run_executor import ProcessStillAlive, ThreadStillAlive


@contextlib.contextmanager
def context_manager_stub() -> typing.Iterator[None]:
    yield


class TestRunContextManagerInExecutor(unittest.TestCase):
    def test_raises_if_shared_memory_size_passed_for_a_thread_executor(self) -> None:
        with self.assertRaises(ValueError):
            run_executor._run_context_manager_in_executor(
                context_manager=context_manager_stub(),
                executor_type=run_executor.ExecutorType.THREAD,
                shared_memory_size=1,
            )


def build_threading_lock_acquirer(
    *,
    lock: threading.Lock,
) -> typing.ContextManager[None]:
    """
    Returns context manager that acquires the passed lock on entry and releases it on
    exit.
    """

    @contextlib.contextmanager
    def lock_acquirer() -> typing.Iterator[None]:
        with lock:
            yield

    return lock_acquirer()


def build_slow_release_threading_lock_acquirer(
    *,
    lock: threading.Lock,
) -> contextlib.AbstractContextManager[None]:
    """
    Returns context manager that acquires the passed lock on entry and releases it on
    exit, but sleeps for a second and a half before exiting.
    """

    @contextlib.contextmanager
    def lock_acquirer() -> typing.Iterator[None]:
        with lock:
            yield
            time.sleep(1.5)

    return lock_acquirer()


class PropagatedException(Exception):
    pass


@contextlib.contextmanager
def raise_exception_on_entry() -> typing.Iterator[None]:
    raise PropagatedException
    yield


@contextlib.contextmanager
def raise_exception_on_exit() -> typing.Iterator[None]:
    yield
    raise PropagatedException


class TestRunThread(unittest.TestCase):
    def test_runs_context_manager_to_yield_on_entry(self) -> None:
        """
        Here we test the implementation of `run_thread` using a simple context manager
        that acquires a lock on entry, and releases it on exit:
        - Before `seamstress.run_thread` is entered, the lock shouldn't be acquired
        - From within `seamstress.run_thread`'s context, the lock should be acquired
        - After exiting `seamstress.run_thread`'s context, the lock should be released
        """
        lock = threading.Lock()

        lock_acquirer = build_threading_lock_acquirer(lock=lock)

        # safety check
        self.assertFalse(
            lock.locked(),
            msg="Lock acquired before run_thread's context was entered.",
        )

        with seamstress.run_thread(lock_acquirer):
            self.assertTrue(
                lock.locked(),
                msg="Lock not acquired when run_thread's context had been entered.",
            )

        self.assertFalse(
            lock.locked(),
            msg="Lock not released after exiting run_thread's context.",
        )

    def test_raises_if_thread_still_alive_after_timeout(self) -> None:
        slow_release_lock_acquirer = build_slow_release_threading_lock_acquirer(
            lock=threading.Lock(),
        )

        with self.assertRaises(ThreadStillAlive):
            with seamstress.run_thread(
                slow_release_lock_acquirer,
                timeout=0.01,
            ):
                pass

    def test_thread_still_alive_exception_message(self) -> None:
        slow_release_lock_acquirer = build_slow_release_threading_lock_acquirer(
            lock=threading.Lock(),
        )

        passed_timeout = 0.01

        expected_error_message = f'The thread running "lock_acquirer" was still alive after {passed_timeout} seconds.'
        with self.assertRaisesRegex(
            ThreadStillAlive,
            expected_error_message,
        ):
            with seamstress.run_thread(
                slow_release_lock_acquirer,
                timeout=passed_timeout,
            ):
                pass

    def test_thread_still_alive_exception_message_with_one_second_timeout(self) -> None:
        slow_release_lock_acquirer = build_slow_release_threading_lock_acquirer(
            lock=threading.Lock(),
        )

        passed_timeout = 1.0

        expected_error_message = (
            'The thread running "lock_acquirer" was still alive after 1 second.'
        )
        with self.assertRaisesRegex(
            ThreadStillAlive,
            expected_error_message,
        ):
            with seamstress.run_thread(
                slow_release_lock_acquirer,
                timeout=passed_timeout,
            ):
                pass

    def test_propagates_exception_raised_on_entry_back_to_main_thread(self) -> None:
        """
        Verify that if the context manager passed to `run_thread` raises an exception
        before yielding, this exception is raised back in the main thread.
        """
        with self.assertRaises(PropagatedException) as cm:
            with seamstress.run_thread(raise_exception_on_entry()):
                pass

        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            ),
        )

        assert (
            'Raised by "raise_exception_on_entry" passed to `seamstress.run_thread`.'
            in printed_output
        )

    def test_propagates_exception_raised_on_exit_back_to_main_thread(self) -> None:
        """
        Verify that if the context manager passed to `run_thread` raises an exception
        after it's yield statement, this exception is raised back in the main thread.
        """
        with self.assertRaises(PropagatedException) as cm:
            with seamstress.run_thread(raise_exception_on_exit()):
                pass

        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            ),
        )

        assert (
            'Raised by "raise_exception_on_exit" passed to `seamstress.run_thread`.'
            in printed_output
        )


def build_process_lock_acquirer(
    *,
    lock: MultiprocessingLock,
) -> typing.ContextManager[None]:
    """
    See docstring of `build_threading_lock_acquirer`.
    """

    @contextlib.contextmanager
    def lock_acquirer() -> typing.Iterator[None]:
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
    def lock_acquirer() -> typing.Iterator[None]:
        with lock:
            yield
            time.sleep(1.5)

    return lock_acquirer()


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


class TestRunProcess(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = multiprocessing.Lock()
        return super().setUp()

    def _lock_is_locked(self) -> bool:
        """
        If we managed to acquire the lock, it wasn't locked/acquired by another
        process, so we use that value to determine the return value of this function.
        However, we don't want to pollute test state, so if we did manage to acquire the
        lock, release it so it goes back into its unlocked/unacquired state
        """
        # TODO: replace this method with calls to lock.locked() when python 3.14 becomes
        # the minimum supported version.
        acquired_lock = self.lock.acquire(block=False)
        if acquired_lock:
            self.lock.release()
        return not acquired_lock

    def test_runs_context_manager_to_yield_on_entry(self) -> None:
        """
        See docstring in TestRunProcess
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

        assert (
            'Raised by "raise_exception_on_entry" passed to `seamstress.run_process`.'
            in printed_output
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

        assert (
            'Raised by "raise_exception_on_exit" passed to `seamstress.run_process`.'
            in printed_output
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
            assert (
                "Please tweak the call to `run_process` in this test to include the "
                "keyword argument `shared_memory_size="
            ) in printed_output

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
            assert (
                "Please tweak the call to `run_process` in this test to include the "
                "keyword argument `shared_memory_size="
            ) in printed_output

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
