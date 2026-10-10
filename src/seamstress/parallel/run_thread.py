from __future__ import annotations

import contextlib
import threading
import typing

from seamstress import utils

if typing.TYPE_CHECKING:
    import collections

### Building blocks


class _PropagatingThread(threading.Thread):
    """
    Propagates any exceptions raised back to the thread that spawned it.
    """

    _exception_raised_by_target: Exception | None
    _exception_lock: threading.Lock

    @typing.override
    def __init__(
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

    @typing.override
    def run(self) -> None:
        try:
            return super().run()
        except Exception as e:  # noqa:BLE001
            with self._exception_lock:
                self._exception_raised_by_target = e

    @typing.override
    def join(self, timeout: float | None = None) -> None:
        super().join(timeout=timeout)

        with self._exception_lock:
            if self._exception_raised_by_target:
                raise self._exception_raised_by_target


def _enter_context_then_wait(
    *,
    context_manager: contextlib.AbstractContextManager[None],
    context_entered_event: threading.Event,
    exit_context_event: threading.Event,
) -> None:
    """
    The function that is run in the spawned thread, to pause its execution in a certain
    state. Enters the context manager before setting `context_entered_event`, to inform
    the main thread that the spawned thread has entered the context manager, and so the
    test can resume execution. Waits for the main thread to set `exit_context_event`, so
    as to pause the thread's execution in a particular state whilst the test in the main
    thread makes assertions.
    """
    try:
        with context_manager:
            context_entered_event.set()
            exit_context_event.wait()
    except BaseException:
        # Falsely report that the context manager has been entered, so the main
        # thread isn't waiting for an event that never gets set (else `run_thread` will
        # hang if an exception is thrown when entering the context manager)
        if not context_entered_event.is_set():
            context_entered_event.set()
        raise


class ThreadStillAlive(Exception):
    pass


def _raise_thread_still_alive(
    context_manager: contextlib.AbstractContextManager[None],
    *,
    timeout: float,
) -> None:
    alive_time_description = "1 second" if timeout == 1.0 else f"{timeout} seconds"
    context_manager_identifier = (
        utils.context_managers.get_identifier_for_context_manager(context_manager)
    )
    error_message = (
        f'The thread running "{context_manager_identifier}" was still alive after '
        f"{alive_time_description}. If this doesn't indicate a bug, consider passing a "
        "longer timeout value to `run_thread`."
    )
    raise ThreadStillAlive(error_message)


### Class that orchestrates running the context manager in a thread


class _ThreadNotStarted(Exception):
    pass


class _ContextManagerRunner:
    """
    Runs a context manager in a thread, pausing the thread's execution after the context
    manager has been entered in it, and then waiting until instructed to continue the
    thread's execution and so exit the context manager.
    """

    _context_manager: contextlib.AbstractContextManager[None]
    _join_timeout: float

    # The thread running the context manager
    _thread: _PropagatingThread

    _context_entered_event: threading.Event
    _exit_context_event: threading.Event

    _thread_started: bool

    def __init__(
        self,
        *,
        context_manager: contextlib.AbstractContextManager[None],
        join_timeout: float,
    ) -> None:

        self._context_manager = context_manager
        self._join_timeout = join_timeout

        self._context_entered_event = threading.Event()
        self._exit_context_event = threading.Event()

        self._thread = _PropagatingThread(
            target=_enter_context_then_wait,
            kwargs={
                "context_manager": context_manager,
                "context_entered_event": self._context_entered_event,
                "exit_context_event": self._exit_context_event,
            },
            # Don't prevent the programme from exiting - this is only a test utility
            daemon=True,
        )

        self._thread_started = False

    def enter_context_manager_in_thread(self) -> None:
        self._thread.start()
        self._thread_started = True
        self._context_entered_event.wait()

    def exit_context_manager_in_thread(self) -> None:
        if not self._thread_started:
            error_message = (
                "Must call `enter_context_manager_in_thread` before calling "
                "`exit_context_manager_in_thread`."
            )
            raise _ThreadNotStarted(error_message)

        self._exit_context_event.set()

        try:
            self._thread.join(timeout=self._join_timeout)
        except Exception as e:
            context_manager_identifier = (
                utils.context_managers.get_identifier_for_context_manager(
                    self._context_manager
                )
            )
            e.add_note(
                f'Raised by "{context_manager_identifier}" passed to `seamstress.run_thread`.',
            )
            raise

        if self._thread.is_alive():
            _raise_thread_still_alive(
                self._context_manager,
                timeout=self._join_timeout,
            )


### Public API

_DEFAULT_THREAD_JOIN_TIMEOUT = 1.0


@contextlib.contextmanager
def run_thread(
    context_manager: contextlib.AbstractContextManager[None],
    *,
    timeout: float | None = None,
) -> typing.Generator[None]:
    if timeout is None:
        timeout = _DEFAULT_THREAD_JOIN_TIMEOUT

    runner = _ContextManagerRunner(
        context_manager=context_manager,
        join_timeout=timeout,
    )

    runner.enter_context_manager_in_thread()

    yield

    runner.exit_context_manager_in_thread()
