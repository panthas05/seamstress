from __future__ import annotations

import sys

if sys.version_info < (3, 14):
    error_message = "Requires Python 3.14+"
    raise RuntimeError(error_message)


import contextlib
import pickle
import typing
from concurrent import interpreters

from . import _event


class CommunicationQueues:
    # queue to be used to pass a context manager's bytes from the main
    # interpreter to the spawned interpreter
    context_manager_memory_view_queue: interpreters.Queue
    # queue to be used to pass a read file descriptor to the spawned
    # interpreter, so it can wait for the main interpreter to signal that it
    # should exit the context of the passed context manager
    context_entered_file_descriptor_queue: interpreters.Queue
    # queue to be used to pass a read file descriptor to the main interpreter,
    # so it can wait for the spawned interpreter to signal that it has entered
    # the context of the passed context manager
    exit_context_file_descriptor_queue: interpreters.Queue
    # queue to be used to pass an exception's bytes from the spawned interpreter
    # to the main interpreter (in the case that an exception is raised during
    # the execution of the context manager)
    memory_view_for_raised_exception_queue: interpreters.Queue

    def __init__(
        self,
        *,
        context_manager_memory_view_queue: interpreters.Queue,
        context_entered_file_descriptor_queue: interpreters.Queue,
        exit_context_file_descriptor_queue: interpreters.Queue,
        memory_view_for_raised_exception_queue: interpreters.Queue,
    ) -> None:
        self.context_manager_memory_view_queue = context_manager_memory_view_queue
        self.context_entered_file_descriptor_queue = (
            context_entered_file_descriptor_queue
        )
        self.exit_context_file_descriptor_queue = exit_context_file_descriptor_queue
        self.memory_view_for_raised_exception_queue = (
            memory_view_for_raised_exception_queue
        )

    @classmethod
    def build(cls) -> CommunicationQueues:
        """
        Builds a `CommunicationQueues` instance, creating the various queues for the
        class to hold. To be called by the main interpreter that creates the queues and
        spawns the subsequent interpreter.

        We expect to pass exactly one object through each queue:
        - One `memoryview` to pass the pickled context manager to the spawned
          interpreter
        - One read file descriptor to the spawned interpreter so it can wait for the
          main interpreter to instruct it to exit the context manager
        - One read file descriptor to the main interpreter so it can wait for the
          spawned interpreter let it know that its entered the context manager
        - One `memoryview` holding any exception raised in the spawned interpreter

        As such, we can insist that the queues have a max size of 1.
        """
        return cls(
            context_manager_memory_view_queue=interpreters.create_queue(maxsize=1),
            context_entered_file_descriptor_queue=interpreters.create_queue(maxsize=1),
            exit_context_file_descriptor_queue=interpreters.create_queue(maxsize=1),
            memory_view_for_raised_exception_queue=interpreters.create_queue(maxsize=1),
        )

    @classmethod
    def access(
        cls,
        context_manager_memory_view_queue_id: int,
        context_entered_file_descriptor_queue_id: int,
        exit_context_file_descriptor_queue_id: int,
        memory_view_for_raised_exception_queue_id: int,
    ) -> CommunicationQueues:
        """
        Creates a `CommunicationQueues` instance, accessing various queues that have
        already been created by a previous call to `build`. To be called by the spawned
        interpreter (not the main one), so that it can access the queues that have
        already been created.
        """
        return cls(
            context_manager_memory_view_queue=interpreters.Queue(
                context_manager_memory_view_queue_id
            ),
            context_entered_file_descriptor_queue=interpreters.Queue(
                context_entered_file_descriptor_queue_id
            ),
            exit_context_file_descriptor_queue=interpreters.Queue(
                exit_context_file_descriptor_queue_id
            ),
            memory_view_for_raised_exception_queue=interpreters.Queue(
                memory_view_for_raised_exception_queue_id
            ),
        )


# Exceptions and type-guards - everything that is unexpected, and utilities for
# type narrowing the object that has been de-serialized from some pickled bytes


class NotAMemoryView(Exception):
    pass


class NotAContextManager(Exception):
    pass


def is_context_manager_none(
    object_: object,
) -> typing.TypeGuard[typing.ContextManager[None]]:
    return isinstance(object_, contextlib.AbstractContextManager)


class NotAnInt(Exception):
    pass


class NotAnException(Exception):
    pass


def is_exception(object_: object) -> typing.TypeGuard[Exception]:
    return isinstance(object_, Exception)


# Sending the context manager to be entered, and a read file descriptor to the
# spawned interpreter. The latter of these is needed for constructing the "exit
# context" `Waitable` (which the spawned interpreter will need to call `.wait()`
# on, so that it pauses execution until instructed to leave the context of the
# passed context manager)


def receive_resources_from_main_interpreter(
    communication_queues: CommunicationQueues,
) -> tuple[typing.ContextManager[None], _event.Waitable]:
    context_manager_memory_view = (
        communication_queues.context_manager_memory_view_queue.get()
    )
    if not isinstance(context_manager_memory_view, memoryview):
        error_message = (
            "Expected a memory view to be passed into `context_manager_memory_view_queue`, "
            f"instead got {type(context_manager_memory_view)}."
        )
        raise NotAMemoryView(error_message)

    context_manager: object = pickle.loads(context_manager_memory_view.tobytes())  # noqa: S301

    if not is_context_manager_none(context_manager):
        error_message = (
            "Expected to deserialize a context manager from the contents of the memory "
            "view passed into `context_manager_memory_view_queue`, instead got "
            f"{type(context_manager)}."
        )
        raise NotAContextManager

    exit_content_read_file_descriptor = (
        communication_queues.exit_context_file_descriptor_queue.get()
    )

    if not isinstance(exit_content_read_file_descriptor, int):
        error_message = (
            "Expected an int to be passed into `exit_context_file_descriptor_queue`, "
            f"instead got {type(exit_content_read_file_descriptor)}."
        )
        raise NotAnInt(error_message)

    exit_context_waitable = _event.Waitable(
        read_file_descriptor=exit_content_read_file_descriptor
    )

    return (context_manager, exit_context_waitable)


def send_resources_to_spawned_interpreter(
    communication_queues: CommunicationQueues,
    *,
    context_manager: typing.ContextManager[None],
    exit_context_event: _event.Event,
) -> None:
    context_manager_memoryview = memoryview(pickle.dumps(context_manager)).toreadonly()
    communication_queues.context_manager_memory_view_queue.put(
        context_manager_memoryview
    )

    communication_queues.exit_context_file_descriptor_queue.put(
        exit_context_event.read_file_descriptor
    )


# Sending a read file descriptor to the main interpreter. This is so it can
# construct the "context entered" `Waitable` (so that it knows when the spawned
# interpreter has entered the passed context manager).


def receive_resources_from_spawned_interpreter(
    communication_queues: CommunicationQueues,
) -> _event.Waitable:
    context_entered_read_file_descriptor = (
        communication_queues.context_entered_file_descriptor_queue.get()
    )

    if not isinstance(context_entered_read_file_descriptor, int):
        error_message = (
            "Expected an int to be passed into `context_entered_file_descriptor_queue`, "
            f"instead got {type(context_entered_read_file_descriptor)}."
        )
        raise NotAnInt(error_message)

    return _event.Waitable(read_file_descriptor=context_entered_read_file_descriptor)


def send_resources_to_main_interpreter(
    communication_queues: CommunicationQueues,
    *,
    context_entered_event: _event.Event,
) -> None:
    communication_queues.context_entered_file_descriptor_queue.put(
        context_entered_event.read_file_descriptor
    )


# Sending an exception to the main interpreter (so that we can propagate an
# exception raised in the spawned interpreter back to the main interpreter)


def receive_exception_from_spawned_interpreter(
    communication_queues: CommunicationQueues,
) -> Exception | None:
    if communication_queues.memory_view_for_raised_exception_queue.empty():
        return None

    raised_exception_memory_view = (
        communication_queues.memory_view_for_raised_exception_queue.get()
    )
    if not isinstance(raised_exception_memory_view, memoryview):
        error_message = (
            "Expected a memory view to be passed into "
            "`memory_view_for_raised_exception_queue`, instead got "
            f"{type(raised_exception_memory_view)}."
        )
        raise NotAMemoryView(error_message)

    exception: object = pickle.loads(raised_exception_memory_view.tobytes())  # noqa: S301

    if not is_exception(exception):
        error_message = (
            "Expected to deserialize an exception from the contents of the memory view "
            "passed into `memory_view_for_raised_exception_queue`, instead got "
            f"{type(exception)}."
        )
        raise NotAContextManager

    return exception


def send_exception_to_main_interpreter(
    communication_queues: CommunicationQueues,
    *,
    exception: Exception,
) -> None:
    exception_memoryview = memoryview(pickle.dumps(exception)).toreadonly()
    communication_queues.memory_view_for_raised_exception_queue.put(
        exception_memoryview
    )
