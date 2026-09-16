import sys
import typing

if sys.version_info >= (3, 14) or typing.TYPE_CHECKING:
    if sys.version_info >= (3, 14):
        from .interpreter_config import InterpreterConfig
        from .run_interpreter import run_interpreter
    elif typing.TYPE_CHECKING:
        InterpreterConfig = typing.Any
        run_interpreter = typing.Any
    else:
        typing.assert_never()

    __all__ = [
        "InterpreterConfig",
        "run_interpreter",
    ]
