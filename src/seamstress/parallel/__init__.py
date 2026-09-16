import sys
import typing

from .executor_config import ProcessConfig, ThreadConfig
from .run_executor import run_process, run_thread

__all__ = [
    "ProcessConfig",
    "ThreadConfig",
    "run_process",
    "run_thread",
]

if sys.version_info >= (3, 14) or typing.TYPE_CHECKING:
    if sys.version_info >= (3, 14):
        from .run_interpreter import InterpreterConfig, run_interpreter
    elif typing.TYPE_CHECKING:
        InterpreterConfig = typing.Any
        run_interpreter = typing.Any
    else:
        typing.assert_never()

    __all__ += [
        "InterpreterConfig",
        "run_interpreter",
    ]
