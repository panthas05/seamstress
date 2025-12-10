import contextlib
import multiprocessing
import threading
import time
import typing
import unittest
from multiprocessing.synchronize import Lock as MultiprocessingLock

import seamstress
from seamstress.parallel.run_executor import ProcessStillAlive, ThreadStillAlive


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
            lock=threading.Lock()
        )

        with self.assertRaises(ThreadStillAlive):
            with seamstress.run_thread(
                slow_release_lock_acquirer,
                timeout=0.01,
            ):
                pass

    def test_thread_still_alive_exception_message(self) -> None:
        slow_release_lock_acquirer = build_slow_release_threading_lock_acquirer(
            lock=threading.Lock()
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
            lock=threading.Lock()
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
            lock=self.lock
        )

        with self.assertRaises(ProcessStillAlive):
            with seamstress.run_process(
                slow_release_lock_acquirer,
                timeout=0.01,
            ):
                pass

    def test_process_still_alive_exception_message(self) -> None:
        slow_release_lock_acquirer = build_slow_release_process_lock_acquirer(
            lock=self.lock
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
            lock=self.lock
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
