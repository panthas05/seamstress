import contextlib
import threading
import time
import typing
import unittest

import seamstress
from seamstress.run_thread import ThreadStillAlive


def build_lock_acquirer(
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


def build_slow_release_lock_acquirer(
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

        lock_acquirer = build_lock_acquirer(lock=lock)

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
        slow_release_lock_acquirer = build_slow_release_lock_acquirer(
            lock=threading.Lock()
        )

        with self.assertRaises(ThreadStillAlive):
            with seamstress.run_thread(
                slow_release_lock_acquirer,
                timeout=0.01,
            ):
                pass

    def test_thread_still_alive_exception_message(self) -> None:
        slow_release_lock_acquirer = build_slow_release_lock_acquirer(
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
        slow_release_lock_acquirer = build_slow_release_lock_acquirer(
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
