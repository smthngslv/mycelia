import asyncio
import contextlib
from asyncio import CancelledError, Task
from collections.abc import Callable, Coroutine
from types import TracebackType
from typing import Any, ClassVar, Final, Self, final
from uuid import UUID

from mycelia.services.broker.interface import IBroker
from mycelia.tracing import TraceContext

__all__: Final[tuple[str, ...]] = ("LocalBroker",)


@final
class LocalBroker(IBroker):
    __slots__: ClassVar[tuple[str, ...]] = (
        "__on_graph_cancelled_callback",
        "__on_node_enqueued_callback",
        "__tasks",
        "__watcher_task",
    )

    def __init__(
        self: Self,
        /,
        on_node_enqueued_callback: Callable[[UUID, TraceContext], Coroutine],
        on_graph_cancelled_callback: Callable[[UUID], Coroutine],
    ) -> None:
        self.__on_node_enqueued_callback: Final[Callable[[UUID, TraceContext], Coroutine]] = on_node_enqueued_callback
        self.__on_graph_cancelled_callback: Final[Callable[[UUID], Coroutine]] = on_graph_cancelled_callback
        self.__tasks: Final[set[Task]] = set()
        self.__watcher_task: Final[Task[None]] = asyncio.create_task(self.__watcher())

    async def publish_node_enqueued(
        self: Self,
        /,
        node_id: UUID,
        graph_trace_context: TraceContext,
        options: Any,  # noqa: ARG002
    ) -> None:
        self.__tasks.add(asyncio.create_task(self.__on_node_enqueued_callback(node_id, graph_trace_context)))

    async def publish_graph_cancelled(self: Self, /, graph_id: UUID) -> None:
        self.__tasks.add(asyncio.create_task(self.__on_graph_cancelled_callback(graph_id)))

    async def __aenter__(self: Self, /) -> Self:
        return self

    async def __aexit__(
        self: Self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        if exc_value is not None:
            self.__watcher_task.cancel()
            raise exc_value

        with contextlib.suppress(KeyboardInterrupt, CancelledError):
            await self.__watcher_task

        await asyncio.gather(*self.__tasks)

    async def __watcher(self: Self, /) -> None:
        while True:
            if len(self.__tasks) == 0:
                await asyncio.sleep(delay=0.1)
                continue

            done: set[Task]
            pending: set[Task]
            done, pending = await asyncio.wait(self.__tasks, return_when=asyncio.FIRST_COMPLETED)
            self.__tasks.difference_update(done)
            await asyncio.gather(*done)
