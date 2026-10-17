from __future__ import annotations

import contextlib
import threading
import time
import traceback
import typing
import unittest

import seamstress
from seamstress.parallel.run_thread import (
    ThreadStillAlive,
    _ContextManagerRunner,
    _ThreadNotStarted,
)


@contextlib.contextmanager
def context_manager_stub() -> typing.Generator[None]:
    yield


class TestContextManagerRunner(unittest.TestCase):
    def test_raises_if_public_methods_called_in_wrong_order(self) -> None:
        runner = _ContextManagerRunner(
            context_manager=context_manager_stub(),
            join_timeout=0.01,
        )
        with self.assertRaises(_ThreadNotStarted):
            runner.exit_context_manager_in_thread()


def build_threading_lock_acquirer(
    *,
    lock: threading.Lock,
) -> contextlib.AbstractContextManager[None]:
    """
    Returns context manager that acquires the passed lock on entry and releases it on
    exit.
    """

    @contextlib.contextmanager
    def lock_acquirer() -> typing.Generator[None]:
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


class TestRunThread(unittest.TestCase):
    def test_runs_context_manager_to_yield_on_entry(self) -> None:
        """
        Here we test the implementation of `run_thread` using a simple context manager
        that acquires a lock on entry, and releases it on exit:
        - Before `seamstress.run_thread` is entered, the lock shouldn't be acquired
        - From within `seamstress.run_thread`'s context, the lock should be acquired
        - After exiting `seamstress.run_thread`'s context, the lock should be released
        """  # noqa: D415, D400
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

        self.assertIn(
            'Raised by "raise_exception_on_entry" passed to `seamstress.run_thread`.',
            printed_output,
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

        self.assertIn(
            'Raised by "raise_exception_on_exit" passed to `seamstress.run_thread`.',
            printed_output,
        )
