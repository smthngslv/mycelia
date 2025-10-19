from abc import abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, Final, Protocol, Self
from uuid import UUID

from mycelia.core.entities import CompletedNode, InvokedNode, RunningNode

__all__: Final[tuple[str, ...]] = ("IExecutor",)


class IExecutor[ParamsType: Any](Protocol):
    @classmethod
    @abstractmethod
    def get_bytes_from_params(cls: type[Self], /, params: ParamsType) -> bytes:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_params_from_bytes(cls: type[Self], /, packed: bytes) -> ParamsType:
        raise NotImplementedError

    @abstractmethod
    async def execute_node(
        self: Self,
        /,
        params: ParamsType,
        node: RunningNode,
        invoke_node_callback: Callable[[InvokedNode[Any, Any, ParamsType], bool], Awaitable[UUID]],
    ) -> InvokedNode | CompletedNode | None:
        raise NotImplementedError
