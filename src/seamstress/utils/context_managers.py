import types
import typing

GenericContextManager = (
    typing.AsyncContextManager[typing.Any] | typing.ContextManager[typing.Any]
)


def get_identifier_for_context_manager(
    context_manager: GenericContextManager,
) -> str:
    """
    Attempts to extract a helpful identifier from `context_manager` to be used in
    exception messages.
    """

    name_attr: str | None = getattr(context_manager, "__name__", None)
    if name_attr:
        return name_attr

    func_attr: types.FunctionType | None = getattr(context_manager, "func", None)
    if func_attr:
        func_name_attr: str | None = getattr(func_attr, "__name__", None)
        if func_name_attr:
            return func_name_attr

    gen_attr: types.GeneratorType[typing.Any] | None = getattr(
        context_manager, "gen", None
    )
    if gen_attr:
        gen_name_attr: str | None = getattr(gen_attr, "__name__", None)
        if gen_name_attr:
            return gen_name_attr

    return "<unknown>"
