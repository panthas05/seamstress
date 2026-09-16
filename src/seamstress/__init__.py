import sys
import typing

from .asynchronous import TaskConfig, run_task
from .parallel import ProcessConfig, ThreadConfig, run_process, run_thread

__all__ = [
    "ProcessConfig",
    "TaskConfig",
    "ThreadConfig",
    "run_process",
    "run_task",
    "run_thread",
]

if sys.version_info >= (3, 14) or typing.TYPE_CHECKING:
    if sys.version_info >= (3, 14):
        from .parallel import InterpreterConfig, run_interpreter
    elif typing.TYPE_CHECKING:
        InterpreterConfig = typing.Any
        run_interpreter = typing.Any
    else:
        typing.assert_never()

    __all__ += [
        "InterpreterConfig",
        "run_interpreter",
    ]
