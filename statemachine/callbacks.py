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
    name: str
    callback: Callable
    param_count: int
    is_async: bool

    def invoke(self, context: Any, info: Any) -> Coroutine[Any, Any, Any] | Any:
        if self.param_count == 0:
            result = self.callback()
        elif self.param_count == 1:
            result = self.callback(context)
        else:
            result = self.callback(context, info)

        return result


def _get_callback_signature(callback: Callable) -> CallbackSpec:
    if iscoroutine(callback):
        raise TypeError(f"Callback must be callable, got {type(callback).__name__}")

    # inner_func = getattr(callback, "__func__", callback) # includes self/cls in param count
    # sig = signature(inner_func)
    sig = signature(callback)

    count = 0
    for param in sig.parameters.values():
        if param.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.POSITIONAL_ONLY):
            count += 1
        elif param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
            count = 2
            break

    return CallbackSpec(
        name=getattr(callback, "__name__", type(callback).__name__),
        callback=callback,
        param_count=count,
        is_async=iscoroutinefunction(callback),
    )


def prepare_callbacks(callbacks: Iterable[Callable] | Callable | None) -> list:
    if callbacks is None:
        return []
    elif callable(callbacks):
        return [_get_callback_signature(callbacks)]

    return [_get_callback_signature(callback) for callback in callbacks]
