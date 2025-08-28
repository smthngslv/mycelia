import typing
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, Self, final
from uuid import UUID

from mycelia.domains.graphs.entities import Node
from mycelia.services.storage.interface import IStorage
from mycelia.tracing import TraceContext

__all__: Final[tuple[str, ...]] = ("LocalStorage",)


@final
class _GraphStatus(str, Enum):
    CREATED = "created"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@final
@dataclass
class _Graph:
    id: UUID
    result: Any
    status: _GraphStatus
    trace_context: TraceContext
    dependent_graph_id: UUID | None
    dependent_node_ids: set[UUID]


@final
@dataclass
class _Node:
    id: UUID
    graph_id: UUID
    handler_id: Any
    arguments: dict[int | str, Any]
    dependencies: dict[int | str, UUID]
    broker_options: Any
    executor_options: Any


@final
class LocalStorage(IStorage):
    def __init__(self: Self, /) -> None:
        self.__graphs: Final[dict[UUID, _Graph]] = {}
        self.__nodes: Final[dict[UUID, _Node]] = {}

    async def create_graph(self: Self, /, id_: UUID, trace_context: TraceContext) -> None:
        self.__graphs[id_] = _Graph(
            id=id_,
            result=None,
            status=_GraphStatus.CREATED,
            trace_context=trace_context,
            dependent_graph_id=None,
            dependent_node_ids=set(),
        )

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
        options: Any,  # noqa: ARG002
    ) -> bool:
        if self.__graphs[graph_id].status != _GraphStatus.CREATED:
            # TODO: Add exception.
            raise RuntimeError

        node: Final[_Node] = _Node(
            id=id_,
            graph_id=graph_id,
            handler_id=handler_id,
            arguments=dict(arguments),
            dependencies={},
            broker_options=broker_options,
            executor_options=executor_options,
        )

        key: int | str
        dependency_graph_id: UUID
        for key, dependency_graph_id in dependencies.items():
            graph: _Graph = self.__graphs[dependency_graph_id]

            if graph.status == _GraphStatus.CANCELLED:
                # TODO: Add exception.
                raise RuntimeError

            if graph.status == _GraphStatus.COMPLETED:
                node.arguments[key] = graph.result
                continue

            graph.dependent_node_ids.add(node.id)
            node.dependencies[key] = dependency_graph_id

        self.__nodes[id_] = node
        return len(node.dependencies) == 0

    async def get_node(self: Self, /, id_: UUID) -> Node:
        node: Final[_Node] = self.__nodes[id_]
        return Node(
            graph_id=node.graph_id,
            handler_id=node.handler_id,
            arguments=node.arguments,
            broker_options=node.broker_options,
            executor_options=node.executor_options,
        )

    async def link_graphs(
        self: Self, /, dependent_id: UUID, dependency_id: UUID
    ) -> list[tuple[UUID, TraceContext, Any] | UUID]:
        dependency_graph: Final[_Graph] = self.__graphs[dependency_id]

        if dependency_graph.status == _GraphStatus.COMPLETED:
            return await self.mark_graph_completed(dependent_id, self.__graphs[dependency_id].result)

        if dependency_graph.status == _GraphStatus.CANCELLED:
            return typing.cast(
                "list[tuple[UUID, TraceContext, Any] | UUID]", await self.mark_graph_cancelled(dependent_id)
            )

        if dependency_graph.dependent_graph_id is not None and dependency_graph.dependent_graph_id != dependent_id:
            message: Final[str] = "A graph already has a dependent graph."
            raise RuntimeError(message)

        dependency_graph.dependent_graph_id = dependent_id
        return []

    async def mark_graph_completed(
        self: Self, /, id_: UUID, result: Any
    ) -> list[tuple[UUID, TraceContext, Any] | UUID]:
        graph: Final[_Graph] = self.__graphs[id_]
        graph.result = result
        graph.status = _GraphStatus.COMPLETED

        dependent_node_ids: Final[set[UUID]] = set(graph.dependent_node_ids)
        dependent_graph_id: UUID | None = graph.dependent_graph_id
        while dependent_graph_id is not None:
            dependent_node_ids.update(self.__graphs[dependent_graph_id].dependent_node_ids)
            dependent_graph_id = self.__graphs[dependent_graph_id].dependent_graph_id

        ready_nodes: Final[list[tuple[UUID, TraceContext, Any] | UUID]] = []
        dependent_node_id: UUID
        for dependent_node_id in dependent_node_ids:
            node: _Node = self.__nodes[dependent_node_id]
            resolved_dependencies: dict[int | str, Any] = {
                key: result for key, dependency_graph_id in node.dependencies.items() if dependency_graph_id == id_
            }

            node.arguments.update(resolved_dependencies)
            for key in resolved_dependencies:
                node.dependencies.pop(key)

            if len(node.dependencies) == 0:
                ready_nodes.append((node.id, self.__graphs[node.graph_id].trace_context, node.broker_options))

        return ready_nodes

    # TODO: Avoid recursion.
    async def mark_graph_cancelled(self: Self, /, id_: UUID) -> list[UUID]:
        graph: Final[_Graph] = self.__graphs[id_]
        graph.status = _GraphStatus.CANCELLED

        dependent_graph_ids: Final[set[UUID]] = {graph.id}
        if graph.dependent_graph_id is not None:
            dependent_graph_ids.update(await self.mark_graph_cancelled(graph.dependent_graph_id))

        dependent_node_id: UUID
        for dependent_node_id in graph.dependent_node_ids:
            dependent_node: _Node = self.__nodes[dependent_node_id]
            dependent_graph_ids.update(await self.mark_graph_cancelled(dependent_node.graph_id))

        return list(dependent_graph_ids)
