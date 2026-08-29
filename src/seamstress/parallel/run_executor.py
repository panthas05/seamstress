from __future__ import annotations

import contextlib
import enum
import multiprocessing
import threading
import typing
from multiprocessing.synchronize import Event as MultiprocessingEvent

from seamstress import utils

from . import _custom_executors

Event = threading.Event | MultiprocessingEvent
Executor = _custom_executors.PropagatingThread | _custom_executors.PropagatingProcess


class ExecutorType(enum.StrEnum):
    THREAD = "thread"
    PROCESS = "process"


def _enter_context_then_wait(
    *,
    context_manager: contextlib.AbstractContextManager[None],
    context_entered_event: Event,
    exit_context_event: Event,
) -> None:
    try:
        with context_manager:
            context_entered_event.set()
            exit_context_event.wait()
    except BaseException:
        if not context_entered_event.is_set():
            context_entered_event.set()
        raise


def _run_context_manager_in_executor(
    *,
    context_manager: contextlib.AbstractContextManager[None],
    executor_type: ExecutorType,
    shared_memory_size: int | None,
) -> tuple[Executor, Event]:
    """
    Creates and runs a thread that enters `context_manager`, before waiting
    indefinitely. Returns the waiting thread, and an event then can be used to instruct
    the thread to exit `context_manager`'s context and terminate.
    """
    executor: Executor
    context_entered_event: Event
    exit_context_event: Event

    if executor_type == ExecutorType.THREAD:
        if shared_memory_size is not None:
            error_message = "`shared_memory_size` argument should not be used with a thread executor"
            raise ValueError(error_message)

        context_entered_event = threading.Event()
        exit_context_event = threading.Event()

        executor = _custom_executors.PropagatingThread(
            target=_enter_context_then_wait,
            kwargs={
                "context_manager": context_manager,
                "context_entered_event": context_entered_event,
                "exit_context_event": exit_context_event,
            },
            # Don't prevent the programme from exiting - this is only a test utility
            daemon=True,
        )
    elif executor_type == ExecutorType.PROCESS:
        context_entered_event = multiprocessing.Event()
        exit_context_event = multiprocessing.Event()

        executor = _custom_executors.PropagatingProcess(
            target=_enter_context_then_wait,
            kwargs={
                "context_manager": context_manager,
                "context_entered_event": context_entered_event,
                "exit_context_event": exit_context_event,
            },
            # Don't prevent the programme from exiting - this is only a test utility
            daemon=True,
            shared_memory_size=shared_memory_size,
        )
    else:
        typing.assert_never(executor_type)

    executor.start()
    # wait until `executor` signals that it has entered `context_manager`'s
    # context
    context_entered_event.wait()

    return executor, exit_context_event


DEFAULT_THREAD_JOIN_TIMEOUT = 1.0


class ThreadStillAlive(Exception):
    pass


class ProcessStillAlive(Exception):
    pass


def _raise_executor_still_alive(
    context_manager: typing.ContextManager[None],
    *,
    executor_type: ExecutorType,
    timeout: float,
) -> None:
    exception_class: type[ThreadStillAlive | ProcessStillAlive]
    if executor_type == ExecutorType.THREAD:
        exception_class = ThreadStillAlive
    elif executor_type == ExecutorType.PROCESS:
        exception_class = ProcessStillAlive
    else:
        typing.assert_never(executor_type)

    alive_time_description = "1 second" if timeout == 1.0 else f"{timeout} seconds"
    context_manager_identifier = (
        utils.context_managers.get_identifier_for_context_manager(context_manager)
    )
    error_message = (
        f'The {executor_type} running "{context_manager_identifier}" was still alive after '
        f"{alive_time_description}. If this doesn't indicate a bug, consider "
        f"passing a longer timeout value to `run_{executor_type}`."
    )
    raise exception_class(error_message)


@contextlib.contextmanager
def _run_executor(
    context_manager: typing.ContextManager[None],
    *,
    executor_type: ExecutorType,
    timeout: float | None = None,
    shared_memory_size: int | None = None,
) -> typing.Generator[None, None, None]:
    executor, exit_context_event = _run_context_manager_in_executor(
        context_manager=context_manager,
        executor_type=executor_type,
        shared_memory_size=shared_memory_size,
    )

    yield

    exit_context_event.set()

    timeout = timeout or DEFAULT_THREAD_JOIN_TIMEOUT
    try:
        executor.join(timeout=timeout)
    except BaseException as e:
        context_manager_identifier = (
            utils.context_managers.get_identifier_for_context_manager(context_manager)
        )
        if not isinstance(e, _custom_executors.ExceptionTooLargeToPropagate):
            e.add_note(
                f'Raised by "{context_manager_identifier}" passed to `seamstress.run_{executor_type.value}`.',
            )
        raise e  # noqa:TRY201

    if executor.is_alive():
        _raise_executor_still_alive(
            context_manager,
            executor_type=executor_type,
            timeout=timeout,
        )


def run_thread(
    context_manager: typing.ContextManager[None],
    *,
    timeout: float | None = None,
) -> typing.ContextManager[None]:
    return _run_executor(
        context_manager=context_manager,
        timeout=timeout,
        executor_type=ExecutorType.THREAD,
    )


def run_process(
    context_manager: typing.ContextManager[None],
    *,
    timeout: float | None = None,
    shared_memory_size: int | None = None,
) -> typing.ContextManager[None]:
    return _run_executor(
        context_manager=context_manager,
        timeout=timeout,
        executor_type=ExecutorType.PROCESS,
        shared_memory_size=shared_memory_size,
    )
