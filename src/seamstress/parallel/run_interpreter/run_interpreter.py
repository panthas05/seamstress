from __future__ import annotations

import sys

if sys.version_info < (3, 14):
    error_message = "Requires Python 3.14+"
    raise RuntimeError(error_message)


import contextlib
import typing
from concurrent import interpreters

from seamstress import utils

from . import _event, _resource_passing


def _enter_context_then_wait(
    *,
    context_manager_memory_view_queue_id: int,
    context_entered_file_descriptor_queue_id: int,
    exit_context_file_descriptor_queue_id: int,
    memory_view_for_raised_exception_queue_id: int,
) -> None:

    communication_queues = _resource_passing.CommunicationQueues.access(
        context_manager_memory_view_queue_id=context_manager_memory_view_queue_id,
        context_entered_file_descriptor_queue_id=context_entered_file_descriptor_queue_id,
        exit_context_file_descriptor_queue_id=exit_context_file_descriptor_queue_id,
        memory_view_for_raised_exception_queue_id=memory_view_for_raised_exception_queue_id,
    )

    context_entered_event = _event.build_event()

    _resource_passing.send_resources_to_main_interpreter(
        communication_queues,
        context_entered_event=context_entered_event,
    )

    context_manager, exit_context_waitable = (
        _resource_passing.receive_resources_from_main_interpreter(communication_queues)
    )

    try:
        with context_manager:
            context_entered_event.set()
            exit_context_waitable.wait()
    except BaseException as exception:
        if not context_entered_event.is_set():
            context_entered_event.set()

        if isinstance(exception, Exception):
            _resource_passing.send_exception_to_main_interpreter(
                communication_queues,
                exception=exception,
            )
        else:
            raise


class InterpreterStillAlive(Exception):
    pass


def _raise_interpreter_still_alive(
    context_manager: typing.ContextManager[None],
    *,
    timeout: float,
) -> None:
    alive_time_description = "1 second" if timeout == 1.0 else f"{timeout} seconds"
    context_manager_identifier = (
        utils.context_managers.get_identifier_for_context_manager(context_manager)
    )
    error_message = (
        f'The interpreter running "{context_manager_identifier}" was still alive after '
        f"{alive_time_description}. If this doesn't indicate a bug, consider "
        f"passing a longer timeout value to `run_interpreter`."
    )
    raise InterpreterStillAlive(error_message)


DEFAULT_INTERPRETER_JOIN_TIMEOUT = 1.0


@contextlib.contextmanager
def _run_context_manager_in_interpreter(
    context_manager: typing.ContextManager[None],
    *,
    timeout: float | None,
) -> typing.Generator[None, None, None]:
    communication_queues = _resource_passing.CommunicationQueues.build()

    interpreter = interpreters.create()

    spawned_interpreter_thread = interpreter.call_in_thread(
        _enter_context_then_wait,
        context_manager_memory_view_queue_id=communication_queues.context_manager_memory_view_queue.id,
        context_entered_file_descriptor_queue_id=communication_queues.context_entered_file_descriptor_queue.id,
        exit_context_file_descriptor_queue_id=communication_queues.exit_context_file_descriptor_queue.id,
        memory_view_for_raised_exception_queue_id=communication_queues.memory_view_for_raised_exception_queue.id,
    )

    exit_context_event = _event.build_event()

    _resource_passing.send_resources_to_spawned_interpreter(
        communication_queues,
        context_manager=context_manager,
        exit_context_event=exit_context_event,
    )

    context_entered_waitable = (
        _resource_passing.receive_resources_from_spawned_interpreter(
            communication_queues,
        )
    )

    context_entered_waitable.wait()

    yield

    exit_context_event.set()

    timeout = timeout or DEFAULT_INTERPRETER_JOIN_TIMEOUT

    spawned_interpreter_thread.join(timeout=timeout)

    if spawned_interpreter_thread.is_alive():
        _raise_interpreter_still_alive(context_manager, timeout=timeout)

    exception_raised_in_interpreter = (
        _resource_passing.receive_exception_from_spawned_interpreter(
            communication_queues
        )
    )

    if exception_raised_in_interpreter:
        context_manager_identifier = (
            utils.context_managers.get_identifier_for_context_manager(context_manager)
        )
        exception_raised_in_interpreter.add_note(
            f'Raised by "{context_manager_identifier}" passed to `seamstress.run_interpreter`.',
        )
        raise exception_raised_in_interpreter


def run_interpreter(
    context_manager: typing.ContextManager[None],
    *,
    timeout: float | None = None,
) -> typing.ContextManager[None]:
    return _run_context_manager_in_interpreter(
        context_manager,
        timeout=timeout,
    )
