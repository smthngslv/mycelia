import asyncio
import contextlib
from asyncio import Queue, Task
from collections.abc import Callable, Coroutine
from types import TracebackType
from typing import Any, ClassVar, Final, Self, final

from mycelia.domains.nodes.entities import Event, NodeCompletedEvent, NodeEnqueuedEvent
from mycelia.services.broker.interface import IBroker

__all__: Final[tuple[str, ...]] = ("LocalBroker",)


@final
class LocalBroker(IBroker):
    __slots__: ClassVar[tuple[str, ...]] = (
        "__invoke_callbacks_task",
        "__on_node_completed_callback",
        "__on_node_enqueued_callback",
        "__queue",
        "__tasks",
        "__worker",
    )

    def __init__(self: Self) -> None:
        self.__queue: Queue[Event] = Queue()
        self.__on_node_enqueued_callback: Callable[[NodeEnqueuedEvent], Coroutine] | None = None
        self.__on_node_completed_callback: Callable[[NodeCompletedEvent], Coroutine] | None = None
        self.__tasks: set[Task] = set()
        self.__invoke_callbacks_task: Final[Task[None]] = asyncio.create_task(self.__invoke_callbacks())
        self.__is_shutdown: Final[asyncio.Event] = asyncio.Event()

    async def publish(self: Self, event: Event, options: Any) -> None:  # noqa:ARG002
        self.__queue.put_nowait(event)

    def set_on_node_enqueued_callback(self: Self, function: Callable[[NodeEnqueuedEvent], Coroutine]) -> None:
        if self.__on_node_enqueued_callback is not None:
            message: Final[str] = "The `on_node_enqueued` callback is already set."
            raise RuntimeError(message)

        self.__on_node_enqueued_callback = function

    def set_on_node_completed_callback(self: Self, function: Callable[[NodeCompletedEvent], Coroutine]) -> None:
        if self.__on_node_completed_callback is not None:
            message: Final[str] = "The `on_node_completed` callback is already set."
            raise RuntimeError(message)

        self.__on_node_completed_callback = function

    async def __aenter__(self: Self) -> Self:
        return self

    async def __aexit__(
        self: Self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        if exc_value is not None:
            self.__invoke_callbacks_task.cancel()
            raise exc_value

        with contextlib.suppress(KeyboardInterrupt, asyncio.CancelledError):
            await self.__invoke_callbacks_task

        for task in self.__tasks:
            await task

    async def __invoke_callbacks(self: Self) -> None:
        while True:
            try:
                event: Event = await asyncio.wait_for(self.__queue.get(), timeout=1.0)

            except TimeoutError:
                continue

            if isinstance(event, NodeEnqueuedEvent) and self.__on_node_enqueued_callback is not None:
                self.__tasks.add(asyncio.create_task(self.__on_node_enqueued_callback(event)))

            elif isinstance(event, NodeCompletedEvent) and self.__on_node_completed_callback is not None:
                self.__tasks.add(asyncio.create_task(self.__on_node_completed_callback(event)))

            else:
                message: str = f"Unexpected event type: `{type(event)}`."
                raise RuntimeError(message)

            tasks: set[Task] = self.__tasks
            self.__tasks = set()
            task: Task
            for task in tasks:
                if not task.done():
                    self.__tasks.add(task)
                    continue

                try:
                    await task

                except Exception as exception:
                    message = f"Task `{task}` raised an exception: `{exception}`."
                    raise RuntimeError(message) from exception

        if len(self.__tasks) != 0:
            await asyncio.gather(*self.__tasks)
