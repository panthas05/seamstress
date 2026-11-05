from __future__ import annotations

import contextlib
import multiprocessing
import sys
import typing

from seamstress import utils

if typing.TYPE_CHECKING:
    import collections
    from multiprocessing.synchronize import Event
    from multiprocessing.synchronize import Lock as MultiprocessingLock


### building blocks


class _PropagatingProcess(multiprocessing.Process):
    """
    Propagates any exceptions raised in the process back to the process that spawned it.
    """

    _queues_access_mutex: MultiprocessingLock
    _queues_closed = False

    _connection_for_sending_exception_raised_by_target: (
        multiprocessing.connection.Connection
    )
    _connection_for_receiving_exception_raised_by_target: (
        multiprocessing.connection.Connection
    )

    @utils.typing.override
    def __init__(  # noqa: PLR0913
        self,
        kwargs: collections.abc.Mapping[str, typing.Any],
        group: None = None,
        target: collections.abc.Callable[..., object] | None = None,
        name: str | None = None,
        args: collections.abc.Iterable[typing.Any] = (),
        *,
        daemon: bool | None = None,
    ) -> None:
        (
            self._connection_for_receiving_exception_raised_by_target,
            self._connection_for_sending_exception_raised_by_target,
        ) = multiprocessing.Pipe(duplex=False)

        self._queues_access_mutex = multiprocessing.Lock()
        self._queues_closed = False

        super().__init__(
            group=group,
            target=target,
            name=name,
            args=args,
            kwargs=kwargs,
            daemon=daemon,
        )

    @utils.typing.override
    def run(self) -> None:
        try:
            return super().run()
        except Exception as exception:  # noqa:BLE001
            with self._queues_access_mutex:
                self._connection_for_sending_exception_raised_by_target.send(exception)

    @utils.typing.override
    def join(self, timeout: float | None = None) -> None:
        super().join(timeout=timeout)

        with self._queues_access_mutex:
            if self._queues_closed:
                # join can be called multiple times - use `self._queues_closed` as a
                # flag to prevent calling `connection.close()` multiple times
                return

            if self._connection_for_receiving_exception_raised_by_target.poll():
                exception_raised_in_spawned_process = (
                    self._connection_for_receiving_exception_raised_by_target.recv()
                )

                raise exception_raised_in_spawned_process

            self._connection_for_receiving_exception_raised_by_target.close()
            self._connection_for_sending_exception_raised_by_target.close()

            self._queues_closed = True


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

            if sys.version_info >= (3, 11):
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
) -> typing.Generator[None]:
    if timeout is None:
        timeout = _DEFAULT_PROCESS_JOIN_TIMEOUT

    runner = _ContextManagerRunner(
        context_manager=context_manager,
        join_timeout=timeout,
    )

    runner.enter_context_manager_in_process()

    yield

    runner.exit_context_manager_in_process()
