from __future__ import annotations

import contextlib
import enum
import multiprocessing
import pickle
import threading
import typing
from multiprocessing import shared_memory

if typing.TYPE_CHECKING:
    import collections


class PropagatingThread(threading.Thread):
    """
    Propagates any exceptions raised in the thread back to the (main) thread that
    spawned it.
    """

    _exception_raised_by_target: BaseException | None
    _exception_lock: threading.Lock

    def __init__(  # noqa:PLR0913
        self,
        group: None = None,
        target: collections.abc.Callable[..., object] | None = None,
        name: str | None = None,
        args: collections.abc.Iterable[typing.Any] = (),
        kwargs: collections.abc.Mapping[str, typing.Any] | None = None,
        *,
        daemon: bool | None = None,
    ) -> None:
        self._exception_lock = threading.Lock()
        self._exception_raised_by_target = None

        super().__init__(
            group=group,
            target=target,
            name=name,
            args=args,
            kwargs=kwargs,
            daemon=daemon,
        )

    def run(self) -> None:
        try:
            return super().run()
        except Exception as e:  # noqa:BLE001
            with self._exception_lock:
                self._exception_raised_by_target = e

    def join(self, timeout: float | None = None) -> None:
        super().join(timeout=timeout)

        with self._exception_lock:
            if self._exception_raised_by_target:
                raise self._exception_raised_by_target


class SharedBufferForSizeRequiredWasNone(Exception):
    pass


def _write_size_required_for_exception_to_shared_memory(
    exception: BaseException,
    shared_memory_for_size_required: shared_memory.SharedMemory,
) -> None:
    pickled_exception_size = len(pickle.dumps(exception))

    shared_buffer = shared_memory_for_size_required.buf
    if shared_buffer is None:
        raise SharedBufferForSizeRequiredWasNone

    exception_size_bytes_repr = pickled_exception_size.to_bytes(
        shared_buffer.nbytes,
        byteorder="big",
        signed=False,
    )

    shared_buffer[: shared_buffer.nbytes] = exception_size_bytes_repr


class WriteExceptionOutcome(enum.StrEnum):
    SUCCESS = "SUCCESS"
    NOT_ENOUGH_MEMORY = "NOT_ENOUGH_MEMORY"


class SharedBufferForExceptionWasNone(Exception):
    pass


def _write_exception_to_shared_memory(
    exception: BaseException,
    shared_memory_for_exception: shared_memory.SharedMemory,
) -> WriteExceptionOutcome:
    """
    Pickles the passed exception, writing its bytes to the passed shared memory. Retuns
    """
    pickled_exception = pickle.dumps(exception)
    pickled_exception_size = len(pickled_exception)

    shared_buffer = shared_memory_for_exception.buf
    if shared_buffer is None:
        raise SharedBufferForExceptionWasNone

    if pickled_exception_size <= shared_buffer.nbytes:
        shared_buffer[:pickled_exception_size] = pickled_exception
        return WriteExceptionOutcome.SUCCESS

    return WriteExceptionOutcome.NOT_ENOUGH_MEMORY


class ExceptionTooLargeToPropagate(Exception):
    pass


class PropagatingProcess(multiprocessing.Process):
    """
    Propagates any exceptions raised in the process back to the process that spawned it.
    """

    # shared memory for passing a pickled exception from the spawned process to spawning
    # process, so we can propagate the exception back into the test
    _memory_for_exception_raised_by_target: shared_memory.SharedMemory
    # if the above shared memory wasn't big enough to hold the pickled exception, we
    # need to let the user know to specify a larger size for it - this shared memory
    # lets us pass the required size back to the spawning process
    _memory_for_size_required: shared_memory.SharedMemory

    def __init__(  # noqa:PLR0913
        self,
        kwargs: collections.abc.Mapping[str, typing.Any],
        group: None = None,
        target: collections.abc.Callable[..., object] | None = None,
        name: str | None = None,
        args: collections.abc.Iterable[typing.Any] = (),
        *,
        daemon: bool | None = None,
        shared_memory_size: int | None = None,
    ) -> None:
        memory_for_exception_size = shared_memory_size or 1_000
        self._memory_for_exception_raised_by_target = shared_memory.SharedMemory(
            create=True,
            size=memory_for_exception_size,
        )

        self._memory_for_size_required = shared_memory.SharedMemory(
            create=True,
            size=100,
        )

        super().__init__(
            group=group,
            target=target,
            name=name,
            args=args,
            kwargs=kwargs,
            daemon=daemon,
        )

    def run(self) -> None:
        try:
            return super().run()
        except Exception as exception:  # noqa:BLE001
            outcome = _write_exception_to_shared_memory(
                exception,
                self._memory_for_exception_raised_by_target,
            )
            if outcome == WriteExceptionOutcome.NOT_ENOUGH_MEMORY:
                _write_size_required_for_exception_to_shared_memory(
                    exception,
                    self._memory_for_size_required,
                )

    def join(self, timeout: float | None = None) -> None:
        super().join(timeout=timeout)

        shared_memory_already_freed = any(
            memory.buf is None
            for memory in [
                self._memory_for_exception_raised_by_target,
                self._memory_for_size_required,
            ]
        )

        if shared_memory_already_freed:
            # `join` can be called multiple times, so if memory has already been freed
            # (presumably by the below code), return early
            return

        try:
            shared_buffer_holding_size_required = self._memory_for_size_required.buf
            if shared_buffer_holding_size_required is None:
                raise SharedBufferForSizeRequiredWasNone

            size_required_buffer_bytes = shared_buffer_holding_size_required.tobytes()
            if not all(byte == 0 for byte in size_required_buffer_bytes):
                size_required = int.from_bytes(
                    size_required_buffer_bytes,
                    byteorder="big",
                    signed=False,
                )
                raise ExceptionTooLargeToPropagate(
                    "`seamstress.run_process` tried to propagate an exception from the "
                    "spawned process back to the process in which it was called, but "
                    "it was too large. Please tweak the call to `run_process` in this "
                    "test to include the keyword argument "
                    f"`shared_memory_size={size_required}`.",
                )

            shared_buffer_holding_exception = (
                self._memory_for_exception_raised_by_target.buf
            )
            if shared_buffer_holding_exception is None:
                raise SharedBufferForExceptionWasNone

            exception_buffer_bytes = shared_buffer_holding_exception.tobytes()
            # if something has been written to the buffer, unpickle and raise the
            # exception
            if not all(byte == 0 for byte in exception_buffer_bytes):
                unpickled_exception = pickle.loads(exception_buffer_bytes)
                raise unpickled_exception
        finally:
            # Clean up shared memory
            memory_to_free = [
                self._memory_for_exception_raised_by_target,
                self._memory_for_size_required,
            ]

            for memory in memory_to_free:
                memory.close()
                with contextlib.suppress(FileNotFoundError):
                    # Suppress a known bug in shared_memory, see:
                    # - https://bugs.python.org/issue39959
                    # - https://github.com/python/cpython/issues/84140
                    # - https://github.com/python/cpython/issues/82300
                    memory.unlink()
