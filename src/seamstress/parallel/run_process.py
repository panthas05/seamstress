from __future__ import annotations

import contextlib
import enum
import multiprocessing
import pickle
import typing
from multiprocessing import shared_memory

from seamstress import utils

if typing.TYPE_CHECKING:
    import collections
    from multiprocessing.synchronize import Event

### building blocks


class _SharedBufferForSizeRequiredWasNone(Exception):
    pass


def _write_size_required_for_exception_to_shared_memory(
    pickled_exception: bytes,
    shared_memory_for_size_required: shared_memory.SharedMemory,
) -> None:
    pickled_exception_size = len(pickled_exception)

    shared_buffer = shared_memory_for_size_required.buf
    if shared_buffer is None:
        raise _SharedBufferForSizeRequiredWasNone

    exception_size_bytes_repr = pickled_exception_size.to_bytes(
        shared_buffer.nbytes,
        byteorder="big",
        signed=False,
    )

    shared_buffer[: shared_buffer.nbytes] = exception_size_bytes_repr


class _WriteExceptionOutcome(enum.StrEnum):
    SUCCESS = "SUCCESS"
    NOT_ENOUGH_MEMORY = "NOT_ENOUGH_MEMORY"


class _SharedBufferForExceptionWasNone(Exception):
    pass


def _write_exception_to_shared_memory(
    pickled_exception: bytes,
    shared_memory_for_exception: shared_memory.SharedMemory,
) -> _WriteExceptionOutcome:
    """
    Writes the passed exception bytes to the passed shared memory. Returns a value
    stating whether the exception was successfully written to the passed shared memory,
    or whether it was too small to write the excepetion to.
    """
    pickled_exception_size = len(pickled_exception)

    shared_buffer = shared_memory_for_exception.buf
    if shared_buffer is None:
        raise _SharedBufferForExceptionWasNone

    if pickled_exception_size <= shared_buffer.nbytes:
        shared_buffer[:pickled_exception_size] = pickled_exception
        return _WriteExceptionOutcome.SUCCESS

    return _WriteExceptionOutcome.NOT_ENOUGH_MEMORY


class ExceptionTooLargeToPropagate(Exception):
    pass


class _PickledObjectWasNotAnException(Exception):
    pass


class _PropagatingProcess(multiprocessing.Process):
    """
    Propagates any exceptions raised in the process back to the process that spawned it.
    """

    # shared memory for passing a pickled exception from the spawned process to spawning
    # process, so we can propagate the exception back into the test
    _memory_for_exception_raised_by_target: shared_memory.SharedMemory
    # if the above shared memory wasn't big enough to hold the pickled exception, we
    # need to let the user know to specify a larger size for it - this shared memory
    # lets us pass the required size back to the main/spawning process
    _memory_for_size_required: shared_memory.SharedMemory

    @typing.override
    def __init__(
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

    @typing.override
    def run(self) -> None:
        try:
            return super().run()
        except Exception as exception:  # noqa:BLE001
            pickled_exception = pickle.dumps(exception)

            write_exception_outcome = _write_exception_to_shared_memory(
                pickled_exception,
                self._memory_for_exception_raised_by_target,
            )

            if write_exception_outcome == _WriteExceptionOutcome.NOT_ENOUGH_MEMORY:
                _write_size_required_for_exception_to_shared_memory(
                    pickled_exception,
                    self._memory_for_size_required,
                )

    @typing.override
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
                raise _SharedBufferForSizeRequiredWasNone

            size_required_buffer_bytes = shared_buffer_holding_size_required.tobytes()
            if not all(byte == 0 for byte in size_required_buffer_bytes):
                size_required = int.from_bytes(
                    size_required_buffer_bytes,
                    byteorder="big",
                    signed=False,
                )
                error_message = (
                    "`seamstress.run_process` tried to propagate an exception from the "
                    "spawned process back to the process in which `run_process` was "
                    "called, but the exception was too large for the default amount of "
                    "memory allocated to store the exception in.\nPlease tweak the "
                    "call to `run_process` in this test to include the keyword "
                    f"argument `shared_memory_size={size_required}`."
                )
                raise ExceptionTooLargeToPropagate(error_message)

            shared_buffer_holding_exception = (
                self._memory_for_exception_raised_by_target.buf
            )
            if shared_buffer_holding_exception is None:
                raise _SharedBufferForExceptionWasNone

            exception_buffer_bytes = shared_buffer_holding_exception.tobytes()
            # if something has been written to the buffer, unpickle and raise the
            # exception
            if not all(byte == 0 for byte in exception_buffer_bytes):
                unpickled_exception = pickle.loads(exception_buffer_bytes)  # noqa:S301 - we can trust the buffer's contents as it was us who wrote to it
                if not isinstance(unpickled_exception, Exception):
                    error_message = (
                        "In attempting to propagate an exception between processes, a "
                        "object that wasn't an exception was written to shared memory. "
                        "This is indicative of a bug - please file a report."
                    )
                    raise _PickledObjectWasNotAnException(error_message)

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


def _enter_context_then_wait(
    *,
    context_manager: contextlib.AbstractContextManager[None],
    context_entered_event: Event,
    exit_context_event: Event,
) -> None:
    """
    The function that is run in the spawned process, to pause its execution in a certain
    state. Enters the context manager before setting `context_entered_event`, to inform
    the main process that the spawned process has entered the context manager, and so the
    test can resume execution. Waits for the main process to set `exit_context_event`,
    so as to pause the process' execution in a particular state whilst the test in the
    main process makes assertions.
    """
    try:
        with context_manager:
            context_entered_event.set()
            exit_context_event.wait()
    except BaseException:
        # Falsely report that the context manager has been entered, so the main
        # process isn't waiting for an event that never gets set (else
        # `run_process` will hang if an exception is thrown when entering the
        # context manager)
        if not context_entered_event.is_set():
            context_entered_event.set()
        raise


class ProcessStillAlive(Exception):
    pass


def _raise_process_still_alive(
    context_manager: contextlib.AbstractContextManager[None],
    *,
    timeout: float,
) -> None:
    alive_time_description = "1 second" if timeout == 1.0 else f"{timeout} seconds"
    context_manager_identifier = (
        utils.context_managers.get_identifier_for_context_manager(context_manager)
    )
    error_message = (
        f'The process running "{context_manager_identifier}" was still alive after '
        f"{alive_time_description}. If this doesn't indicate a bug, consider passing a "
        "longer timeout value to `run_process`."
    )
    raise ProcessStillAlive(error_message)


### Class that orchestrates running the context manager in a process


class _ProcessNotStarted(Exception):
    pass


class _ContextManagerRunner:
    """
    Runs a context manager in a process, pausing the process' execution after the
    context manager has been entered in it, and then waiting until instructed to
    continue the process' execution and so exit the context manager.
    """

    _context_manager: contextlib.AbstractContextManager[None]
    _join_timeout: float

    # The process running the context manager
    _process: _PropagatingProcess

    _context_entered_event: Event
    _exit_context_event: Event

    _process_started: bool

    def __init__(
        self,
        *,
        context_manager: contextlib.AbstractContextManager[None],
        join_timeout: float,
        shared_memory_size: int | None,
    ) -> None:

        self._context_manager = context_manager
        self._join_timeout = join_timeout

        self._context_entered_event = multiprocessing.Event()
        self._exit_context_event = multiprocessing.Event()

        self._process = _PropagatingProcess(
            target=_enter_context_then_wait,
            kwargs={
                "context_manager": context_manager,
                "context_entered_event": self._context_entered_event,
                "exit_context_event": self._exit_context_event,
            },
            # Don't prevent the programme from exiting - this is only a test utility
            daemon=True,
            shared_memory_size=shared_memory_size,
        )

        self._process_started = False

    def enter_context_manager_in_process(self) -> None:
        self._process.start()
        self._process_started = True
        self._context_entered_event.wait()

    def exit_context_manager_in_process(self) -> None:
        if not self._process_started:
            error_message = (
                "Must call `enter_context_manager_in_process` before calling "
                "`exit_context_manager_in_process`."
            )
            raise _ProcessNotStarted(error_message)

        self._exit_context_event.set()

        try:
            self._process.join(timeout=self._join_timeout)
        except Exception as e:
            context_manager_identifier = (
                utils.context_managers.get_identifier_for_context_manager(
                    self._context_manager
                )
            )
            e.add_note(
                f'Raised by "{context_manager_identifier}" passed to '
                "`seamstress.run_process`.",
            )
            raise

        if self._process.is_alive():
            _raise_process_still_alive(
                self._context_manager,
                timeout=self._join_timeout,
            )


### Public API

_DEFAULT_PROCESS_JOIN_TIMEOUT = 1.0


@contextlib.contextmanager
def run_process(
    context_manager: contextlib.AbstractContextManager[None],
    *,
    timeout: float | None = None,
    shared_memory_size: int | None = None,
) -> typing.Generator[None]:
    if timeout is None:
        timeout = _DEFAULT_PROCESS_JOIN_TIMEOUT

    runner = _ContextManagerRunner(
        context_manager=context_manager,
        join_timeout=timeout,
        shared_memory_size=shared_memory_size,
    )

    runner.enter_context_manager_in_process()

    yield

    runner.exit_context_manager_in_process()
