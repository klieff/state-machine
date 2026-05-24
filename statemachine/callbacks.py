from dataclasses import dataclass
from inspect import (
    Parameter,
    iscoroutine,
    iscoroutinefunction,
    signature,
)
from typing import Any, Callable, Coroutine, Iterable


@dataclass(slots=True)
class CallbackSpec:
    callback: Callable
    param_count: int
    is_async: bool


def get_callback_signature(callback: Callable) -> CallbackSpec:
    if iscoroutine(callback):
        raise TypeError(f"Callback must be callable, got {type(callback).__name__}")

    inner_func = getattr(callback, "__func__", callback)
    sig = signature(inner_func)

    count = 0
    for param in sig.parameters.values():
        if param.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.POSITIONAL_ONLY):
            count += 1
        elif param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            count = 2
            break

    return CallbackSpec(
        callback=callback, param_count=count, is_async=iscoroutinefunction(callback)
    )


def prepare_callbacks(callbacks: Iterable[Callable] | Callable | None) -> list:
    if callbacks is None:
        return []
    elif callable(callbacks):
        return [get_callback_signature(callbacks)]
    return [get_callback_signature(callback) for callback in callbacks]


def invoke_callback[R](
    spec: CallbackSpec, context: Any, info: Any
) -> Coroutine[R, Any, Any] | R:
    if spec.param_count == 0:
        result = spec.callback()
    elif spec.param_count == 1:
        result = spec.callback(context)
    else:
        result = spec.callback(context, info)

    return result
