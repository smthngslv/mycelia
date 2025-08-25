from abc import abstractmethod
from collections.abc import Mapping
from typing import Any, Final, Protocol, Self
from uuid import UUID

from mycelia.domains.graphs.entities import Node
from mycelia.tracing import TraceContext

__all__: Final[tuple[str, ...]] = ("IStorage",)


class IStorage(Protocol):
    @abstractmethod
    async def create_graph(self: Self, /, id_: UUID, trace_context: TraceContext) -> None:
        raise NotImplementedError

    @abstractmethod
    async def create_node(  # noqa: PLR0913
        self: Self,
        /,
        id_: UUID,
        graph_id: UUID,
        handler_id: Any,
        arguments: Mapping[int | str, Any],
        dependencies: Mapping[int | str, UUID],
        broker_options: Any,
        executor_options: Any,
        options: Any,
    ) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def get_node(self: Self, /, id_: UUID) -> Node:
        raise NotImplementedError

    @abstractmethod
    async def link_graphs(
        self: Self, /, dependent_id: UUID, dependency_id: UUID
    ) -> list[tuple[UUID, TraceContext, Any] | UUID]:
        raise NotImplementedError

    @abstractmethod
    async def mark_graph_completed(
        self: Self, /, id_: UUID, result: Any
    ) -> list[tuple[UUID, TraceContext, Any] | UUID]:
        raise NotImplementedError

    @abstractmethod
    async def mark_graph_cancelled(self: Self, /, id_: UUID) -> list[UUID]:
        raise NotImplementedError
