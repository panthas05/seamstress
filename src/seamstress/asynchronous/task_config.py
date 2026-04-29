from __future__ import annotations

import abc
import typing

if typing.TYPE_CHECKING:
    import types


class TaskConfig(abc.ABC):
    @abc.abstractmethod
    async def set_up_task(self) -> None: ...

    @abc.abstractmethod
    async def tear_down_task(self) -> None: ...

    async def __aenter__(self) -> None:
        await self.set_up_task()

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: types.TracebackType | None,
    ) -> None:
        await self.tear_down_task()
