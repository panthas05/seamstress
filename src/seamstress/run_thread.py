import contextlib
import threading
import types
import typing


def _enter_context_then_wait(
    *,
    context_manager: contextlib.AbstractContextManager[None],
    context_entered_event: threading.Event,
    exit_context_event: threading.Event,
) -> None:
    with context_manager:
        context_entered_event.set()
        exit_context_event.wait()


def _run_context_manager_in_thread(
    *,
    context_manager: contextlib.AbstractContextManager[None],
) -> tuple[threading.Thread, threading.Event]:
    """
    Creates and runs a thread that enters `context_manager`, before waiting
    indefinitely. Returns the waiting thread, and an event then can be used to instruct
    the thread to exit `context_manager`'s context and terminate.
    """
    context_entered_event = threading.Event()
    exit_context_event = threading.Event()

    thread = threading.Thread(
        target=_enter_context_then_wait,
        kwargs={
            "context_manager": context_manager,
            "context_entered_event": context_entered_event,
            "exit_context_event": exit_context_event,
        },
        # Don't prevent the programme from exiting - this is only a test utility
        daemon=True,
    )

    thread.start()
    # wait until `thread` signals that it has entered `context_manager`'s
    # context
    context_entered_event.wait()

    return thread, exit_context_event


DEFAULT_THREAD_JOIN_TIMEOUT = 1.0


class ThreadStillAlive(Exception):
    pass


def _get_context_manager_identifier(
    context_manager: typing.ContextManager[None],
) -> str:
    """
    Attempts to extract a helpful identifier from `context_manager` to be used in
    exception messages.
    """

    name_attr: str | None = getattr(context_manager, "__name__", None)
    if name_attr:
        return name_attr

    func_attr: types.FunctionType | None = getattr(context_manager, "func", None)
    if func_attr:
        func_name_attr: str | None = getattr(func_attr, "__name__", None)
        if func_name_attr:
            return func_name_attr

    gen_attr: types.GeneratorType[typing.Any] | None = getattr(
        context_manager, "gen", None
    )
    if gen_attr:
        gen_name_attr: str | None = getattr(gen_attr, "__name__", None)
        if gen_name_attr:
            return gen_name_attr

    return "<unknown>"


@contextlib.contextmanager
def run_thread(
    context_manager: typing.ContextManager[None],
    *,
    timeout: float | None = None,
) -> typing.Generator[None, None, None]:
    thread, exit_context_event = _run_context_manager_in_thread(
        context_manager=context_manager,
    )

    yield

    exit_context_event.set()

    timeout = timeout or DEFAULT_THREAD_JOIN_TIMEOUT
    thread.join(timeout=timeout)
    if thread.is_alive():
        alive_time_description = "1 second" if timeout == 1.0 else f"{timeout} seconds"
        context_manager_identifier = _get_context_manager_identifier(context_manager)
        raise ThreadStillAlive(
            f'The thread running "{context_manager_identifier}" was still alive after '
            f"{alive_time_description}. If this doesn't indicate a bug, consider "
            "passing a longer timeout value to `run_thread`."
        )
