import sys
import unittest

if sys.version_info < (3, 14):
    skipped_tests_message = (
        "`run_interpreter` is not available for python versions less than version 3.14"
    )
    raise unittest.SkipTest(skipped_tests_message)

import contextlib
import time
import traceback
import typing
from multiprocessing import shared_memory

import seamstress
from seamstress.parallel.run_interpreter.run_interpreter import InterpreterStillAlive


@contextlib.contextmanager
def raise_exception_on_entry() -> typing.Iterator[None]:
    raise PropagatedException
    yield


@contextlib.contextmanager
def raise_exception_on_exit() -> typing.Iterator[None]:
    yield
    raise PropagatedException


class SharedMemoryBufferUnavailable(Exception):
    pass


class ModifySharedMemory(seamstress.InterpreterConfig):
    _shared_memory: shared_memory.SharedMemory

    def __init__(self, *, shared_memory_name: str) -> None:
        self._shared_memory = shared_memory.SharedMemory(
            name=shared_memory_name,
            create=False,
        )

    @typing.override
    def set_up_interpreter(self) -> None:
        if self._shared_memory.buf is None:
            exception_message = (
                "Could not write to shared memory on entering `ModifySharedMemory`."
            )
            raise SharedMemoryBufferUnavailable(exception_message)

        self._shared_memory.buf[:] = b"foo"

    @typing.override
    def tear_down_interpreter(self) -> None:
        if self._shared_memory.buf is None:
            exception_message = (
                "Could not write to shared memory on exiting `ModifySharedMemory`."
            )
            raise SharedMemoryBufferUnavailable(exception_message)

        self._shared_memory.buf[:] = b"bar"

        self._shared_memory.close()


class LiveLongerThanPassedTimeout(seamstress.InterpreterConfig):
    _lifetime: float

    def __init__(self, *, timeout: float) -> None:
        self._lifetime = timeout + 0.1

    @typing.override
    def set_up_interpreter(self) -> None:
        pass

    @typing.override
    def tear_down_interpreter(self) -> None:
        time.sleep(self._lifetime)


class PropagatedException(Exception):
    pass


class RaiseExceptionOnEntry(seamstress.InterpreterConfig):
    @typing.override
    def set_up_interpreter(self) -> None:
        raise PropagatedException

    @typing.override
    def tear_down_interpreter(self) -> None:
        pass


class RaiseExceptionOnExit(seamstress.InterpreterConfig):
    @typing.override
    def set_up_interpreter(self) -> None:
        pass

    @typing.override
    def tear_down_interpreter(self) -> None:
        raise PropagatedException


@unittest.skipIf(
    sys.version_info < (3, 14),
    reason="`run_interpreter` is only available for Python 3.14+",
)
class TestRunInterpreter(unittest.TestCase):
    def test_runs_context_manager_to_yield_on_entry(self) -> None:
        shared_memory_ = shared_memory.SharedMemory(
            create=True,
            size=3,
        )

        modify_shared_memory = ModifySharedMemory(
            shared_memory_name=shared_memory_.name
        )

        with seamstress.run_interpreter(modify_shared_memory):
            first_buffer_access = shared_memory_.buf

            if first_buffer_access is None:
                self.fail(
                    "Could not access shared memory when within "
                    "`seamstress.run_interpreter` context"
                )

            self.assertEqual(first_buffer_access[:], b"foo")

        second_buffer_access = shared_memory_.buf

        if second_buffer_access is None:
            self.fail(
                "Could not access shared memory after exiting "
                "`seamstress.run_interpreter` context"
            )

        self.assertEqual(second_buffer_access[:], b"bar")

        shared_memory_.close()
        with contextlib.suppress(FileNotFoundError):
            shared_memory_.unlink()

    def test_raises_if_interpreter_still_alive_after_timeout(self) -> None:
        timeout = 0.01

        cm_with_long_teardown = LiveLongerThanPassedTimeout(timeout=timeout)

        expected_error_message = (
            'The interpreter running "LiveLongerThanPassedTimeout" was still alive '
            f"after {timeout} seconds."
        )
        with self.assertRaisesRegex(
            InterpreterStillAlive,
            expected_error_message,
        ):
            with seamstress.run_interpreter(
                cm_with_long_teardown,
                timeout=timeout,
            ):
                pass

    def test_interpreter_still_alive_exception_message_with_one_second_timeout(
        self,
    ) -> None:
        cm_with_long_teardown = LiveLongerThanPassedTimeout(timeout=1)

        expected_error_message = (
            'The interpreter running "LiveLongerThanPassedTimeout" was still alive '
            "after 1 second."
        )
        with self.assertRaisesRegex(
            InterpreterStillAlive,
            expected_error_message,
        ):
            with seamstress.run_interpreter(
                cm_with_long_teardown,
                timeout=1,
            ):
                pass

    def test_propagates_exception_raised_on_entry_back_to_main_thread(self) -> None:
        """
        Verify that if the context manager passed to `run_interpreter` raises an
        exception in its `__enter__` method, this exception is raised back in the main
        interpreter.
        """
        with self.assertRaises(PropagatedException) as cm:
            with seamstress.run_interpreter(RaiseExceptionOnEntry()):
                pass

        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            ),
        )

        self.assertIn(
            'Raised by "RaiseExceptionOnEntry" passed to `seamstress.run_interpreter`.',
            printed_output,
        )

    def test_propagates_exception_raised_on_exit_back_to_main_thread(self) -> None:
        """
        Verify that if the context manager passed to `run_interpreter` raises an
        exception in its `__exit__` method, this exception is raised back in the main
        interpreter.
        """
        with self.assertRaises(PropagatedException) as cm:
            with seamstress.run_interpreter(RaiseExceptionOnExit()):
                pass

        printed_output = "\n".join(
            traceback.format_exception(
                type(cm.exception),
                cm.exception,
                cm.exception.__traceback__,
            ),
        )

        self.assertIn(
            'Raised by "RaiseExceptionOnExit" passed to `seamstress.run_interpreter`.',
            printed_output,
        )
