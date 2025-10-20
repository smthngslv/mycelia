import functools
from asyncio import Event, Task, TaskGroup
from collections.abc import Callable, Coroutine, Iterable, Mapping
from datetime import timedelta
from types import TracebackType
from typing import Any, ClassVar, Final, Literal, Self, cast, final, overload
from uuid import UUID

import ormsgpack
from ormsgpack import (
    OPT_DATETIME_AS_TIMESTAMP_EXT,
    OPT_NON_STR_KEYS,
    OPT_PASSTHROUGH_DATACLASS,
    OPT_PASSTHROUGH_ENUM,
    OPT_PASSTHROUGH_UUID,
    OPT_SERIALIZE_NUMPY,
    Ext,
    MsgpackDecodeError,
    MsgpackEncodeError,
)
from pydantic import BaseModel, ConfigDict

__all__: Final[tuple[str, ...]] = (
    "Codec",
    "Entity",
    "EventWithSubscribers",
    "EventWithValue",
    "SingleUseLockWithValue",
    "gather",
)


@overload
async def gather[R1: Any, R2: Any](
    coroutine_1: Coroutine[Any, Any, R1], coroutine_2: Coroutine[Any, Any, R2], /
) -> tuple[R1, R2]:
    raise NotImplementedError


@overload
async def gather[R1: Any, R2: Any, R3: Any](
    coroutine_1: Coroutine[Any, Any, R1], coroutine_2: Coroutine[Any, Any, R2], coroutine_3: Coroutine[Any, Any, R3], /
) -> tuple[R1, R2, R3]:
    raise NotImplementedError


@overload
async def gather[R1: Any, R2: Any, R3: Any, R4: Any](
    coroutine_1: Coroutine[Any, Any, R1],
    coroutine_2: Coroutine[Any, Any, R2],
    coroutine_3: Coroutine[Any, Any, R3],
    coroutine_4: Coroutine[Any, Any, R4],
    /,
) -> tuple[R1, R2, R3, R4]:
    raise NotImplementedError


@overload
async def gather[R1: Any, R2: Any, R3: Any, R4: Any, R5: Any](
    coroutine_1: Coroutine[Any, Any, R1],
    coroutine_2: Coroutine[Any, Any, R2],
    coroutine_3: Coroutine[Any, Any, R3],
    coroutine_4: Coroutine[Any, Any, R4],
    coroutine_5: Coroutine[Any, Any, R5],
    /,
) -> tuple[R1, R2, R3, R4, R5]:
    raise NotImplementedError


@overload
async def gather(coroutines: Iterable[Coroutine], /) -> tuple[Any, ...]:
    raise NotImplementedError


async def gather(*coroutines: Coroutine | Iterable[Coroutine]) -> tuple[Any, ...]:
    task_group: TaskGroup
    async with TaskGroup() as task_group:
        tasks: Final[tuple[Task, ...]] = tuple(
            map(
                task_group.create_task,
                cast(
                    "Iterable[Coroutine]",
                    coroutines if len(coroutines) != 1 or isinstance(coroutines[0], Coroutine) else coroutines[0],
                ),
            )
        )

        if len(tasks) == 0:
            return ()

    return tuple(task.result() for task in tasks)


class Codec:
    __slots__: ClassVar[tuple[str, ...]] = (
        "_deserialization_options",
        "_deserializers",
        "_serialization_options",
        "_serializers",
    )

    def __init__(
        self: Self,
        /,
        *,
        use_default: bool = True,
        serializers: Mapping[Any, tuple[int, Callable[[Any], bytes]]] | None = None,
        deserializers: Mapping[int, Callable[[bytes], Any]] | None = None,
        serialization_options: int | None = None,
        deserialization_options: int | None = None,
    ) -> None:
        self._serializers: Final[dict[Any, tuple[int, Callable[[Any], bytes]]]] = (
            {UUID: (0, self._uuid_to_bytes), timedelta: (1, self._timedelta_to_bytes)} if use_default else {}
        )

        if serializers is not None:
            self._serializers.update(serializers)

        self._deserializers: Final[dict[int, Callable[[bytes], Any]]] = (
            {0: self._bytes_to_uuid, 1: self._bytes_to_timedelta} if use_default else {}
        )

        if deserializers is not None:
            self._deserializers.update(deserializers)

        if serialization_options is None:
            serialization_options = 0

        if deserialization_options is None:
            deserialization_options = 0

        if use_default:
            # Support any type as key.
            serialization_options |= OPT_NON_STR_KEYS
            deserialization_options |= OPT_NON_STR_KEYS
            # This variant is x4 more efficient.
            serialization_options |= OPT_DATETIME_AS_TIMESTAMP_EXT
            deserialization_options |= OPT_DATETIME_AS_TIMESTAMP_EXT
            # TODO: Check, how efficient this is.
            serialization_options |= OPT_SERIALIZE_NUMPY
            # Do not serialize dataclasses implicitly.
            serialization_options |= OPT_PASSTHROUGH_DATACLASS
            # Do not serialize enums implicitly.
            serialization_options |= OPT_PASSTHROUGH_ENUM
            # Builtin variant is x2 more expensive.
            serialization_options |= OPT_PASSTHROUGH_UUID

        self._serialization_options: Final[int] = serialization_options
        self._deserialization_options: Final[int] = deserialization_options

    def to_bytes(self: Self, /, value: Any) -> bytes:
        return ormsgpack.packb(value, default=self._serialize, option=self._serialization_options)

    def from_bytes(self: Self, /, value: bytes) -> Any:
        return ormsgpack.unpackb(value, ext_hook=self._deserialize, option=self._deserialization_options)

    def _serialize(self: Self, /, value: Any) -> Any:
        value_type: type
        for value_type in type(value).mro():
            tag_and_serializer: tuple[int, Callable[[Any], bytes]] | None = self._serializers.get(value_type)

            if tag_and_serializer is not None:
                return Ext(tag_and_serializer[0], tag_and_serializer[1](value))

        raise MsgpackDecodeError

    def _deserialize(self: Self, /, tag: int, value: bytes) -> Any:
        deserializer: Final[Callable[[bytes], Any] | None] = self._deserializers.get(tag)

        if deserializer is None:
            raise MsgpackEncodeError

        return deserializer(value)

    @staticmethod
    def _uuid_to_bytes(value: UUID) -> bytes:
        return value.bytes

    @staticmethod
    def _bytes_to_uuid(value: bytes) -> UUID:
        return UUID(bytes=value)

    @staticmethod
    def _timedelta_to_bytes(value: timedelta) -> bytes:
        return ormsgpack.packb(value.total_seconds())

    @staticmethod
    def _bytes_to_timedelta(value: bytes) -> timedelta:
        return timedelta(seconds=ormsgpack.unpackb(value))


class Entity(BaseModel):
    CODEC: ClassVar[Codec] = Codec()
    model_config: ClassVar[ConfigDict] = ConfigDict(validate_default=True, from_attributes=True, frozen=True)

    @classmethod
    def from_bytes(cls: type[Self], /, packed: bytes) -> Self:
        if len(packed) == 0:
            return cls()

        remapped: Final[dict[int, Any]] = cls.CODEC.from_bytes(packed)
        index_to_field_mapping: Final[dict[int, str]] = cls.__get_index_to_field_mapping()
        params: Final[dict[str, Any]] = {index_to_field_mapping[index]: value for index, value in remapped.items()}
        return cls.model_validate(params)

    def to_bytes(self: Self, /) -> bytes:
        params: Final[dict[str, Any]] = self.model_dump(exclude_unset=True, exclude_defaults=True)
        if len(params) == 0:
            return b""

        field_to_index_mapping: Final[dict[str, int]] = self.__get_field_to_index_mapping()
        remapped: Final[dict[int, str]] = {field_to_index_mapping[name]: value for name, value in params.items()}
        return self.CODEC.to_bytes(remapped)

    def __repr__(self: Self, /) -> str:
        return f"{type(self).__name__}({super().__repr__()})"

    @classmethod
    @functools.lru_cache(maxsize=None, typed=True)
    def __get_field_to_index_mapping(cls: type[Self], /) -> dict[str, int]:
        return {field: index for index, field in enumerate(cls.model_fields)}

    @classmethod
    @functools.lru_cache(maxsize=None, typed=True)
    def __get_index_to_field_mapping(cls: type[Self], /) -> dict[int, str]:
        # It's okay to use this, since it's ordered.
        # https://docs.pydantic.dev/latest/concepts/models/#field-ordering.
        return dict(enumerate(cls.model_fields))


@final
class EventWithValue[ValueType: Any]:
    __slots__: ClassVar[tuple[str, ...]] = ("__event", "__exception", "__value")

    def __init__(self: Self, /) -> None:
        self.__event: Final[Event] = Event()
        self.__value: ValueType | None = None
        self.__exception: BaseException | None = None

    @property
    def is_set(self: Self, /) -> bool:
        return self.__event.is_set()

    def set(self: Self, value: ValueType, /) -> None:
        if self.__event.is_set():
            message: Final[str] = "Event is already set."
            raise RuntimeError(message)

        self.__value = value
        self.__event.set()

    async def wait(self: Self, /) -> ValueType:
        await self.__event.wait()

        if self.__exception is not None:
            raise self.__exception

        return cast("ValueType", self.__value)

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
    ) -> Literal[False]:
        if exc_value is not None and not self.__event.is_set():
            self.__exception = exc_value
            self.__event.set()

        return False


@final
class EventWithSubscribers:
    __slots__: ClassVar[tuple[str, ...]] = ("__event", "__subscriber_count")

    def __init__(self: Self, /) -> None:
        self.__event: Final[Event] = Event()
        self.__subscriber_count: int = 0

    @property
    def subscriber_count(self: Self) -> int:
        return self.__subscriber_count

    @property
    def is_set(self: Self, /) -> bool:
        return self.__event.is_set()

    def set(self: Self, /) -> None:
        self.__event.set()

    def subscribe(self: Self, /) -> None:
        self.__subscriber_count += 1

    def unsubscribe(self: Self, /) -> None:
        if self.__subscriber_count == 0:
            message: Final[str] = "Cannot unsubscribe before subscribing."
            raise RuntimeError(message)

        self.__subscriber_count -= 1

    async def wait(self: Self, /) -> None:
        await self.__event.wait()

    def __enter__(self: Self, /) -> Self:
        self.subscribe()
        return self

    def __exit__(
        self: Self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Literal[False]:
        self.unsubscribe()
        return False


@final
class SingleUseLockWithValue[ValueType: Any]:
    __slots__: ClassVar[tuple[str, ...]] = ("__event", "__is_used", "__value")

    def __init__(self: Self, /, value: ValueType) -> None:
        self.__event: Final[Event] = Event()
        self.__value: Final[ValueType] = value
        self.__is_used: bool = False

    @property
    def value(self: Self, /) -> ValueType:
        return self.__value

    async def __aenter__(self: Self, /) -> bool:
        if self.__is_used:
            await self.__event.wait()
            return True

        self.__is_used = True
        return False

    async def __aexit__(
        self: Self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Literal[False]:
        self.__event.set()
        return False
