import threading
import unittest

import seamstress


class CouldNotAcquireLock(Exception):
    pass


class LockAcquirer(seamstress.ThreadConfig):
    def __init__(
        self,
        *,
        lock: threading.Lock,
    ) -> None:
        self.lock = lock

    def set_up_thread(self) -> None:
        acquired_lock = self.lock.acquire()
        if not acquired_lock:
            raise CouldNotAcquireLock

    def tear_down_thread(self) -> None:
        self.lock.release()


class TestRunThreadWithThreadConfig(unittest.TestCase):
    def test_calls_appropriate_methods_on_entry_and_exit(self) -> None:
        lock = threading.Lock()

        lock_acquirer = LockAcquirer(lock=lock)

        # safety check
        self.assertFalse(
            lock.locked(),
            msg="Lock acquired before run_thread context was entered.",
        )

        with seamstress.run_thread(lock_acquirer):
            self.assertTrue(
                lock.locked(),
                msg="Lock not acquired when within run_thread context.",
            )

        self.assertFalse(
            lock.locked(),
            msg="Lock not released after exiting run_thread context.",
        )
