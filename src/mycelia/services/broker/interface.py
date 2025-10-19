from abc import abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, Final, Protocol, Self
from uuid import UUID

from mycelia.core.entities import EnqueuedNode

__all__: Final[tuple[str, ...]] = ("IBroker", "OnNodeEnqueuedCallback", "OnSessionCancelledCallback")

type OnNodeEnqueuedCallback = Callable[[EnqueuedNode], Awaitable]
type OnSessionCancelledCallback = Callable[[UUID], Awaitable]


class IBroker[ParamsType: Any](Protocol):
    @classmethod
    @abstractmethod
    def get_bytes_from_params(cls: type[Self], /, params: ParamsType) -> bytes:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_params_from_bytes(cls: type[Self], /, packed: bytes) -> ParamsType:
        raise NotImplementedError

    @abstractmethod
    async def publish_node_enqueued(self: Self, /, params: ParamsType, node: EnqueuedNode) -> None:
        raise NotImplementedError

    @abstractmethod
    async def publish_session_cancelled(self: Self, /, id_: UUID) -> None:
        raise NotImplementedError

    @abstractmethod
    async def add_on_node_enqueued_callback(
        self: Self, /, params: ParamsType, callback: OnNodeEnqueuedCallback
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def add_on_session_cancelled_callback(self: Self, /, callback: OnSessionCancelledCallback) -> None:
        raise NotImplementedError

    @abstractmethod
    async def remove_callback(self: Self, /, callback: OnNodeEnqueuedCallback | OnSessionCancelledCallback) -> None:
        raise NotImplementedError
