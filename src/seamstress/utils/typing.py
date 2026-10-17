from __future__ import annotations

import sys
import typing

if sys.version_info >= (3, 12):
    override = typing.override
else:
    if typing.TYPE_CHECKING:
        import collections

    # Define a dummy wrapper to be used instead, which insists that the
    # method has the same signature as the method it's overriding
    T = typing.TypeVar("T")
    P = typing.ParamSpec("P")

    def override(
        function: collections.abc.Callable[P, T],
    ) -> collections.abc.Callable[P, T]:
        def inner_function(*args: P.args, **kwargs: P.kwargs) -> T:
            return function(*args, **kwargs)

        return inner_function
