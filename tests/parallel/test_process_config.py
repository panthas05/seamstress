from __future__ import annotations

import multiprocessing
import typing
import unittest

import seamstress

if typing.TYPE_CHECKING:
    from multiprocessing.synchronize import Lock


class CouldNotAcquireLock(Exception):
    pass


class ProcessingLockAcquirer(seamstress.ProcessConfig):
    def __init__(
        self,
        *,
        lock: Lock,
    ) -> None:
        self.lock = lock

    def set_up_process(self) -> None:
        acquired_lock = self.lock.acquire()
        if not acquired_lock:
            raise CouldNotAcquireLock

    def tear_down_process(self) -> None:
        self.lock.release()


class TestRunThreadWithThreadConfig(unittest.TestCase):
    lock: Lock

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

    def test_calls_appropriate_methods_on_entry_and_exit(self) -> None:

        lock_acquirer = ProcessingLockAcquirer(lock=self.lock)

        # safety check
        self.assertFalse(
            self._lock_is_locked(),
            msg="Lock acquired before run_thread context was entered.",
        )

        with seamstress.run_thread(lock_acquirer):
            self.assertTrue(
                self._lock_is_locked(),
                msg="Lock not acquired when within run_thread context.",
            )

        self.assertFalse(
            self._lock_is_locked(),
            msg="Lock not released after exiting run_thread context.",
        )
