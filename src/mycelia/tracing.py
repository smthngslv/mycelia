import dataclasses
import functools
import inspect
from collections.abc import Awaitable, Callable, Set as AbstractSet
from contextlib import AbstractContextManager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Final, Literal, Self, final

import logfire_api

# TODO: Must be replaced with `logfire_api` when issue fixed: https://github.com/pydantic/logfire/issues/1324.
from logfire.propagate import attach_context, get_context
from logfire_api import Logfire, LogfireSpan

# TODO: I need `get_current_span()` method in `logfire_api`: https://github.com/pydantic/logfire/issues/674.
from opentelemetry.trace import Span, get_current_span

__all__: Final[tuple[str, ...]] = (
    "DEBUG",
    "ERROR",
    "FATAL",
    "INFO",
    "NOTICE",
    "TRACE",
    "WARNING",
    "Span",
    "TraceContext",
    "TraceLevel",
    "debug",
    "error",
    "fatal",
    "get_current_span",
    "info",
    "log",
    "logfire",
    "notice",
    "span",
    "trace",
    "warning",
    "with_span",
)

logfire_api.add_non_user_code_prefix(__file__)
# TODO: The `console_log` does not have effect here for some reason, same as `stack_offset`.
logfire: Final[Logfire] = logfire_api.with_settings(tags=("mycelia",))


@final
class TraceLevel(str, Enum):
    TRACE = "trace"
    DEBUG = "debug"
    INFO = "info"
    NOTICE = "notice"
    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"


TRACE: Literal[TraceLevel.TRACE] = TraceLevel.TRACE
DEBUG: Literal[TraceLevel.DEBUG] = TraceLevel.DEBUG
INFO: Literal[TraceLevel.INFO] = TraceLevel.INFO
NOTICE: Literal[TraceLevel.NOTICE] = TraceLevel.NOTICE
WARNING: Literal[TraceLevel.WARNING] = TraceLevel.WARNING
ERROR: Literal[TraceLevel.ERROR] = TraceLevel.ERROR
FATAL: Literal[TraceLevel.FATAL] = TraceLevel.FATAL


@final
@dataclass(frozen=True)
class TraceContext:
    traceparent: str | None = None

    @classmethod
    def get_current(cls: type[Self]) -> Self:
        return cls(**get_context())

    def attach(self: Self) -> AbstractContextManager[None]:
        return attach_context(dataclasses.asdict(self))


def log(
    level: TraceLevel,
    message: str,
    /,
    *,
    tags_: AbstractSet[str] | None = None,
    exception_: BaseException | None = None,
    **attributes: Any,
) -> None:
    if tags_ is None:
        tags_ = set()

    logfire.log(level.value, message, attributes, tags=tuple(tags_), exc_info=exception_)


def trace(
    message: str,
    /,
    *,
    tags_: AbstractSet[str] | None = None,
    exception_: BaseException | None = None,
    **attributes: Any,
) -> None:
    return log(TRACE, message, tags_=tags_, exception_=exception_, **attributes)


def debug(
    message: str,
    /,
    *,
    tags_: AbstractSet[str] | None = None,
    exception_: BaseException | None = None,
    **attributes: Any,
) -> None:
    return log(DEBUG, message, tags_=tags_, exception_=exception_, **attributes)


def info(
    message: str,
    /,
    *,
    tags_: AbstractSet[str] | None = None,
    exception_: BaseException | None = None,
    **attributes: Any,
) -> None:
    return log(INFO, message, tags_=tags_, exception_=exception_, **attributes)


def notice(
    message: str,
    /,
    *,
    tags_: AbstractSet[str] | None = None,
    exception_: BaseException | None = None,
    **attributes: Any,
) -> None:
    return log(NOTICE, message, tags_=tags_, exception_=exception_, **attributes)


def warning(
    message: str,
    /,
    *,
    tags_: AbstractSet[str] | None = None,
    exception_: BaseException | None = None,
    **attributes: Any,
) -> None:
    return log(WARNING, message, tags_=tags_, exception_=exception_, **attributes)


def error(
    message: str,
    /,
    *,
    tags_: AbstractSet[str] | None = None,
    exception_: BaseException | None = None,
    **attributes: Any,
) -> None:
    return log(ERROR, message, tags_=tags_, exception_=exception_, **attributes)


def fatal(
    message: str,
    /,
    *,
    tags_: AbstractSet[str] | None = None,
    exception_: BaseException | None = None,
    **attributes: Any,
) -> None:
    return log(FATAL, message, tags_=tags_, exception_=exception_, **attributes)


def span(
    level: TraceLevel,
    name: str,
    /,
    *,
    message_: str | None = None,
    tags_: AbstractSet[str] | None = None,
    **attributes: Any,
) -> AbstractContextManager[LogfireSpan]:
    if message_ is None:
        message_ = name

    if tags_ is None:
        tags_ = set()

    return logfire.span(message_, _tags=tuple(tags_), _span_name=name, _level=level.value, **attributes)


def with_span[**P, R](
    level: TraceLevel,
    name: str,
    /,
    *,
    message_: str | None = None,
    tags_: AbstractSet[str] | None = None,
    **attributes: Any,
) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
    if message_ is None:
        message_ = name

    if tags_ is None:
        tags_ = set()

    def _decorator(function: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
        attributes.update(_get_code_attributes(function))

        @functools.wraps(function)
        async def _wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with logfire.span(message_, _tags=tuple(tags_), _span_name=name, _level=level.value, **attributes):
                return await function(*args, **kwargs)

        return _wrapper

    return _decorator


def _get_code_attributes(function: Callable) -> dict[str, Any]:
    return {
        "code.filepath": Path(inspect.getfile(function)).relative_to(Path.cwd()).as_posix(),
        "code.function": function.__qualname__,
        "code.lineno": getattr(getattr(function, "__code__", None), "co_firstlineno", None),
    }
