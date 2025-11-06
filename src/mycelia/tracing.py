import contextlib
import enum
import functools
import inspect
from collections.abc import Callable, Coroutine, Iterator, Mapping, Set as AbstractSet
from contextlib import AbstractContextManager
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, Literal, Self, cast, final

import logfire_api
import logfire_api.propagate
from logfire_api import Logfire, LogfireSpan

# TODO: This is workaround: https://github.com/pydantic/logfire/issues/674.
from logfire_api._internal.main import set_user_attributes_on_raw_span

get_current_span: Any

try:
    from opentelemetry.trace import get_current_span

except ImportError:
    get_current_span = None

if TYPE_CHECKING:
    from types import TracebackType

__all__: Final[tuple[str, ...]] = ("TraceContext", "TraceLevel", "Tracer")

logfire_api.add_non_user_code_prefix(__file__)


@final
class TraceLevel(int, Enum):
    TRACE = enum.auto()
    DEBUG = enum.auto()
    INFO = enum.auto()
    NOTICE = enum.auto()
    WARNING = enum.auto()
    ERROR = enum.auto()
    FATAL = enum.auto()


@final
class TraceContext:
    __slots__: ClassVar[tuple[str, ...]] = ("__parent",)

    @classmethod
    def get_current(cls: type[Self], /) -> Self:
        return cls(parent=logfire_api.propagate.get_context().get("traceparent"))

    @classmethod
    def from_bytes(cls: type[Self], /, packed: bytes) -> Self:
        if len(packed) == 0:
            return cls()

        unpacked: Final[str] = packed.hex()
        return cls(parent=f"{unpacked[:2]}-{unpacked[2:34]}-{unpacked[34:50]}-{unpacked[50:]}")

    @classmethod
    def from_json_dict(cls: type[Self], /, json_dict: Mapping[str, Any]) -> Self:
        return cls(parent=json_dict.get("traceparent"))

    def __init__(self: Self, /, *, parent: str | None = None) -> None:
        self.__parent: Final[str | None] = parent

    @property
    def is_empty(self: Self, /) -> bool:
        return self.__parent is None

    def to_bytes(self: Self, /) -> bytes:
        return bytes.fromhex(self.__parent.replace("-", "")) if self.__parent is not None else b""

    def to_json_dict(self: Self, /) -> dict[str, str | None]:
        # Keep well-known name here.
        return {"traceparent": self.__parent} if self.__parent is not None else {}

    @contextlib.contextmanager
    def attach(self: Self, /, tracer: "Tracer") -> Iterator[None]:
        with logfire_api.propagate.attach_context(self.to_json_dict()):
            try:
                yield

            except BaseException as exception:
                exception = exception.with_traceback(cast("TracebackType", exception.__traceback__).tb_next)
                # We have to log exception manually since the context is restored and current span cannot be updated.
                tracer.error("error", exception_=exception)
                raise


@final
class Tracer:
    __slots__: ClassVar[tuple[str, ...]] = ("__logfire", "__scope")

    TRACE: ClassVar[Literal[TraceLevel.TRACE]] = TraceLevel.TRACE
    DEBUG: ClassVar[Literal[TraceLevel.DEBUG]] = TraceLevel.DEBUG
    INFO: ClassVar[Literal[TraceLevel.INFO]] = TraceLevel.INFO
    NOTICE: ClassVar[Literal[TraceLevel.NOTICE]] = TraceLevel.NOTICE
    WARNING: ClassVar[Literal[TraceLevel.WARNING]] = TraceLevel.WARNING
    ERROR: ClassVar[Literal[TraceLevel.ERROR]] = TraceLevel.ERROR
    FATAL: ClassVar[Literal[TraceLevel.FATAL]] = TraceLevel.FATAL
    __TRACE_LEVELS: ClassVar[Mapping[int, TraceLevel]] = {
        0: TraceLevel.TRACE,
        1: TraceLevel.TRACE,
        5: TraceLevel.DEBUG,
        9: TraceLevel.INFO,
        10: TraceLevel.NOTICE,
        13: TraceLevel.WARNING,
        17: TraceLevel.ERROR,
        21: TraceLevel.FATAL,
    }

    def __init__(
        self: Self,
        scope: str,
        /,
        *,
        tags: AbstractSet[str] | None = frozenset(("mycelia",)),
        logfire: Logfire = logfire_api.DEFAULT_LOGFIRE_INSTANCE,
    ) -> None:
        self.__scope: Final[str] = scope
        self.__logfire: Final[Logfire] = logfire.with_settings(
            tags=tuple(tags) if tags is not None else (), custom_scope_suffix=scope
        )

    @property
    def level(self: Self, /) -> TraceLevel:
        return self.__TRACE_LEVELS[self.__logfire.config.min_level]

    def set_attributes_to_current_span(self: Self, level_: TraceLevel, /, **attributes: Any) -> None:
        if get_current_span is not None and self.level <= level_:
            set_user_attributes_on_raw_span(get_current_span(), attributes)

    def get_child(self: Self, scope: str, /, *, tags: AbstractSet[str] | None = None) -> Self:
        # Scope is not propagated by logfire, it has to be done manually.
        return self.__class__(f"{self.__scope}.{scope}", tags=tags, logfire=self.__logfire)

    def log(
        self: Self,
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

        self.__logfire.log(level.name.lower(), message, attributes, tags=tuple(tags_), exc_info=exception_)

    def trace(
        self: Self,
        message: str,
        /,
        *,
        tags_: AbstractSet[str] | None = None,
        exception_: BaseException | None = None,
        **attributes: Any,
    ) -> None:
        return self.log(self.TRACE, message, tags_=tags_, exception_=exception_, **attributes)

    def debug(
        self: Self,
        message: str,
        /,
        *,
        tags_: AbstractSet[str] | None = None,
        exception_: BaseException | None = None,
        **attributes: Any,
    ) -> None:
        return self.log(self.DEBUG, message, tags_=tags_, exception_=exception_, **attributes)

    def info(
        self: Self,
        message: str,
        /,
        *,
        tags_: AbstractSet[str] | None = None,
        exception_: BaseException | None = None,
        **attributes: Any,
    ) -> None:
        return self.log(self.INFO, message, tags_=tags_, exception_=exception_, **attributes)

    def notice(
        self: Self,
        message: str,
        /,
        *,
        tags_: AbstractSet[str] | None = None,
        exception_: BaseException | None = None,
        **attributes: Any,
    ) -> None:
        return self.log(self.NOTICE, message, tags_=tags_, exception_=exception_, **attributes)

    def warning(
        self: Self,
        message: str,
        /,
        *,
        tags_: AbstractSet[str] | None = None,
        exception_: BaseException | None = None,
        **attributes: Any,
    ) -> None:
        return self.log(self.WARNING, message, tags_=tags_, exception_=exception_, **attributes)

    def error(
        self: Self,
        message: str,
        /,
        *,
        tags_: AbstractSet[str] | None = None,
        exception_: BaseException | None = None,
        **attributes: Any,
    ) -> None:
        return self.log(self.ERROR, message, tags_=tags_, exception_=exception_, **attributes)

    def fatal(
        self: Self,
        message: str,
        /,
        *,
        tags_: AbstractSet[str] | None = None,
        exception_: BaseException | None = None,
        **attributes: Any,
    ) -> None:
        return self.log(self.FATAL, message, tags_=tags_, exception_=exception_, **attributes)

    def span(
        self: Self,
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

        return self.__logfire.span(
            message_, _tags=tuple(tags_), _span_name=name, _level=level.name.lower(), **attributes
        )

    def with_span_async[**P, R](
        self: Self,
        level: TraceLevel,
        name: str,
        /,
        *,
        message_: str | None = None,
        tags_: AbstractSet[str] | None = None,
        **attributes: Any,
    ) -> Callable[[Callable[P, Coroutine[None, None, R]]], Callable[P, Coroutine[None, None, R]]]:
        if message_ is None:
            message_ = name

        if tags_ is None:
            tags_ = set()

        def _decorator(function: Callable[P, Coroutine[None, None, R]]) -> Callable[P, Coroutine[None, None, R]]:
            attributes.update(self.__get_code_attributes(function))

            @functools.wraps(function)
            async def _wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                with self.__logfire.span(
                    message_, _tags=tuple(tags_), _span_name=name, _level=level.name.lower(), **attributes
                ):
                    return await function(*args, **kwargs)

            return _wrapper

        return _decorator

    def with_span_sync[**P, R](
        self: Self,
        level: TraceLevel,
        name: str,
        /,
        *,
        message_: str | None = None,
        tags_: AbstractSet[str] | None = None,
        **attributes: Any,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        if message_ is None:
            message_ = name

        if tags_ is None:
            tags_ = set()

        def _decorator(function: Callable[P, R]) -> Callable[P, R]:
            attributes.update(self.__get_code_attributes(function))

            @functools.wraps(function)
            def _wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                with self.__logfire.span(
                    message_, _tags=tuple(tags_), _span_name=name, _level=level.name.lower(), **attributes
                ):
                    return function(*args, **kwargs)

            return _wrapper

        return _decorator

    def with_reset_context_async[**P, R](
        self: Self, /, function: Callable[P, Coroutine[None, None, R]]
    ) -> Callable[P, Coroutine[None, None, R]]:
        @functools.wraps(function)
        async def _wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with TraceContext().attach(self):
                return await function(*args, **kwargs)

        return _wrapper

    def with_reset_context_sync[**P, R](self: Self, /, function: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(function)
        def _wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with TraceContext().attach(self):
                return function(*args, **kwargs)

        return _wrapper

    @staticmethod
    def __get_code_attributes(function: Callable) -> dict[str, Any]:
        return {
            "code.filepath": Path(inspect.getfile(function)).relative_to(Path.cwd()).as_posix(),
            "code.function": function.__qualname__,
            "code.lineno": getattr(getattr(function, "__code__", None), "co_firstlineno", None),
        }
