from __future__ import annotations

import sys

if sys.version_info < (3, 14):
    error_message = "Requires Python 3.14+"
    raise RuntimeError(error_message)


import abc
import typing

if typing.TYPE_CHECKING:
    import types


class InterpreterConfig(abc.ABC):
    @abc.abstractmethod
    def set_up_interpreter(self) -> None: ...

    @abc.abstractmethod
    def tear_down_interpreter(self) -> None: ...

    def __enter__(self) -> None:
        self.set_up_interpreter()

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: types.TracebackType | None,
    ) -> None:
        self.tear_down_interpreter()
