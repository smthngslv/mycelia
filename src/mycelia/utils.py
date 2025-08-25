import asyncio
import typing
from asyncio import Event as _Event, Task
from collections.abc import Coroutine, Iterable
from types import TracebackType
from typing import Any, ClassVar, Final, Self, final

from pydantic import TypeAdapter

__all__: Final[tuple[str, ...]] = ("Event", "gather", "to_json")


async def gather(coroutines: Iterable[Coroutine[Any, Any, Any]]) -> list[Any]:
    tasks: Final[tuple[Task, ...]] = tuple(asyncio.create_task(coroutine) for coroutine in coroutines)
    if len(tasks) == 0:
        return []

    await asyncio.wait(tasks)
    return [task.result() for task in tasks]


def to_json(obj: Any) -> Any:
    return TypeAdapter(type(obj)).dump_python(obj, mode="json")


@final
class Event[ValueType: Any]:
    __slots__: ClassVar[tuple[str, ...]] = ("__event", "__exception", "__value")

    def __init__(self: Self, /) -> None:
        self.__event: Final[_Event] = _Event()
        self.__value: ValueType | None = None
        self.__exception: BaseException | None = None

    def __enter__(self: Self, /) -> Self:
        self.__event.clear()
        self.__value = None
        self.__exception = None
        return self

    def __exit__(
        self: Self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        if exc_value is not None and not self.__event.is_set():
            self.__exception = exc_value
            self.__event.set()

    def set(self: Self, /, value: ValueType) -> None:
        if self.__event.is_set():
            message: Final[str] = "Event is already set."
            raise RuntimeError(message)

        self.__value = value
        self.__event.set()

    async def wait(self: Self, /) -> ValueType:
        await self.__event.wait()

        if self.__exception is not None:
            raise self.__exception

        return typing.cast("ValueType", self.__value)
