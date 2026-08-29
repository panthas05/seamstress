from __future__ import annotations

import asyncio
import contextlib
import typing

from seamstress import utils


async def _enter_context_then_wait(
    *,
    context_manager: typing.AsyncContextManager[None],
    context_entered_event: asyncio.Event,
    exit_context_event: asyncio.Event,
) -> None:
    try:
        async with context_manager:
            context_entered_event.set()
            await exit_context_event.wait()
    except BaseException:
        if not context_entered_event.is_set():
            context_entered_event.set()
        raise


class NoRunningEventLoop(Exception):
    pass


async def _run_context_manager_in_task(
    *,
    context_manager: typing.AsyncContextManager[None],
) -> tuple[asyncio.Task[None], asyncio.Event]:
    context_entered_event = asyncio.Event()
    exit_context_event = asyncio.Event()

    try:
        event_loop = asyncio.get_running_loop()
    except RuntimeError as e:
        error_message = (
            "Please ensure that `async_hog_lock` is called from within an async task, "
            "so that it has access to a running event loop."
        )
        raise NoRunningEventLoop(error_message) from e

    task = event_loop.create_task(
        _enter_context_then_wait(
            context_manager=context_manager,
            context_entered_event=context_entered_event,
            exit_context_event=exit_context_event,
        ),
        name=f"Async hog lock: {context_manager!r}",
    )

    # wait until `task` signals that it has acquired the lock
    await context_entered_event.wait()

    return task, exit_context_event


DEFAULT_TIMEOUT = 1.0


class TaskStillExecuting(Exception):
    pass


@contextlib.asynccontextmanager
async def run_task(
    context_manager: typing.AsyncContextManager[None],
    *,
    timeout: float | None = None,
) -> typing.AsyncIterator[None]:
    task, exit_context_event = await _run_context_manager_in_task(
        context_manager=context_manager,
    )

    yield

    exit_context_event.set()

    timeout = timeout or DEFAULT_TIMEOUT

    try:
        async with asyncio.timeout(timeout):
            try:
                await task
            except BaseException as e:
                context_manager_identifier = (
                    utils.context_managers.get_identifier_for_context_manager(
                        context_manager,
                    )
                )
                e.add_note(
                    f'Raised by "{context_manager_identifier}" passed to `seamstress.run_task`.',
                )
                raise e  # noqa:TRY201

    except asyncio.TimeoutError as e:
        alive_time_description = "1 second" if timeout == 1.0 else f"{timeout} seconds"
        context_manager_identifier = (
            utils.context_managers.get_identifier_for_context_manager(context_manager)
        )
        error_message = (
            f'The task running "{context_manager_identifier}" was still executing after '
            f"{alive_time_description}. If this doesn't indicate a bug, consider "
            "passing a longer timeout value to `run_task`."
        )
        raise TaskStillExecuting(error_message) from e
