from collections.abc import Mapping
from typing import Any, Final, Self, final
from uuid import UUID, uuid4

import logfire_api as logfire

from mycelia.domains.nodes.entities import Node
from mycelia.services.storage.interface import IStorage

__all__: Final[tuple[str, ...]] = ("LocalStorage",)


@final
class LocalStorage(IStorage):
    __slots__: Final[tuple[str, ...]] = (
        "__callbacks",
        "__dependencies",
        "__node_result_associations",
        "__nodes",
        "__parents",
        "__results",
        "__unresolved_dependencies",
    )

    def __init__(self: Self) -> None:
        self.__nodes: Final[dict[UUID, Node]] = {}
        self.__results: Final[dict[UUID, Any]] = {}
        self.__node_result_associations: Final[dict[UUID, UUID]] = {}
        self.__dependencies: Final[dict[UUID, dict[int | str, UUID]]] = {}
        self.__unresolved_dependencies: Final[dict[UUID, set[UUID]]] = {}
        self.__callbacks: Final[dict[UUID, set[UUID]]] = {}
        self.__parents: Final[dict[UUID, UUID]] = {}
        self.__logfire_ctx: Final[dict[UUID, Mapping[str, Any]]] = {}

    async def create_node(  # noqa:PLR0913
        self: Self,
        id_: UUID,
        handler_id: UUID,
        arguments: Mapping[int | str, Any],
        dependencies: Mapping[int | str, UUID],
        broker_options: Any,
        executor_options: Any,
        options: Any,
        logfire_ctx: Mapping[str, Any],
    ) -> None:
        self.__callbacks[id_] = set()
        self.__dependencies[id_] = dict(dependencies)
        self.__unresolved_dependencies[id_] = set(dependencies.values())
        self.__nodes[id_] = Node(
            id=id_,
            handler_id=handler_id,
            arguments=dict(arguments),
            broker_options=broker_options,
            storage_options=options,
            executor_options=executor_options,
        )
        self.__logfire_ctx[id_] = logfire_ctx

    async def save_result(self: Self, node_id: UUID, result: Any, options: Any) -> UUID:  # noqa: ARG002
        result_id: Final[UUID] = uuid4()
        self.__results[result_id] = result

        if node_id in self.__node_result_associations:
            logfire.warning(f"Node {node_id} already has result!")

        self.__node_result_associations[node_id] = result_id
        return result_id

    async def get_node(self: Self, node_id: UUID, options: Any) -> Node:  # noqa: ARG002
        return self.__nodes[node_id]

    async def get_callbacks(self: Self, node_id: UUID) -> set[UUID]:
        return self.__callbacks[node_id]

    async def get_parent(self: Self, node_id: UUID, options: Any) -> UUID | None:  # noqa:ARG002
        return self.__parents.get(node_id)

    async def get_logfire_ctx(self: Self, node_id: UUID, options: Any) -> Mapping[str, Any]:  # noqa:ARG002
        return self.__logfire_ctx[node_id]

    async def set_parent(self: Self, node_id: UUID, parent_id: UUID) -> None:
        self.__parents[node_id] = parent_id

    async def set_dependency_result(self: Self, node_id: UUID, dependency_id: UUID, result_id: UUID) -> bool:
        key: int | str
        value: UUID
        for key, value in self.__dependencies[node_id].items():
            if value == dependency_id:
                self.__nodes[node_id].arguments[key] = self.__results[result_id]

        self.__unresolved_dependencies[node_id].discard(dependency_id)
        return len(self.__unresolved_dependencies[node_id]) == 0

    async def set_callback_or_get_result_id(
        self: Self,
        node_id: UUID,
        dependent_id: UUID,
        options: Any,  # noqa: ARG002
    ) -> UUID | None:
        # Node is finished.
        if node_id in self.__node_result_associations:
            return self.__node_result_associations[node_id]

        self.__callbacks[node_id].add(dependent_id)
        return None
