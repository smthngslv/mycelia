from abc import abstractmethod
from typing import Any, Final, Protocol, Self, overload
from uuid import UUID

from mycelia.core.entities import CompletedNode, CreatedGraph, CreatedNode, CreatedSession, ReadyNode, StartedNode

__all__: Final[tuple[str, ...]] = ("IStorage",)


class IStorage[ParamsType: Any](Protocol):
    @classmethod
    @abstractmethod
    def get_bytes_from_params(cls: type[Self], /, params: ParamsType) -> bytes:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_params_from_bytes(cls: type[Self], /, packed: bytes) -> ParamsType:
        raise NotImplementedError

    @overload
    @abstractmethod
    async def create_node(self: Self, /, params: ParamsType, node: CreatedNode) -> bool:
        raise NotImplementedError

    @overload
    @abstractmethod
    async def create_node(self: Self, /, params: ParamsType, node: CreatedNode, graph: CreatedGraph) -> bool:
        raise NotImplementedError

    @overload
    @abstractmethod
    async def create_node(
        self: Self, /, params: ParamsType, node: CreatedNode, graph: CreatedGraph, session: CreatedSession
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def create_node(
        self: Self,
        /,
        params: ParamsType,
        node: CreatedNode,
        graph: CreatedGraph | None = None,
        session: CreatedSession | None = None,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def start_node(self: Self, /, id_: UUID) -> StartedNode:
        raise NotImplementedError

    @abstractmethod
    async def complete_node(self: Self, /, node: CompletedNode) -> list[ReadyNode]:
        raise NotImplementedError

    @abstractmethod
    async def cancel_session(self: Self, /, id_: UUID) -> bool:
        raise NotImplementedError
