from abc import abstractmethod
from typing import Any, Final, Protocol, Self
from uuid import UUID

from mycelia.tracing import TraceContext

__all__: Final[tuple[str, ...]] = ("IBroker",)


class IBroker(Protocol):
    @abstractmethod
    async def publish_node_enqueued(
        self: Self, /, node_id: UUID, graph_trace_context: TraceContext, options: Any
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def publish_graph_cancelled(self: Self, /, graph_id: UUID) -> None:
        raise NotImplementedError
