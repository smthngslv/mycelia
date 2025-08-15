from abc import abstractmethod
from collections.abc import Mapping
from typing import Any, Final, Protocol, Self
from uuid import UUID

from mycelia.domains.nodes.entities import Node

__all__: Final[tuple[str, ...]] = ("IStorage",)


class IStorage(Protocol):
    # TODO: Rewrite for multiple nodes.
    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    async def save_result(self: Self, node_id: UUID, result: Any, options: Any) -> UUID:
        raise NotImplementedError

    @abstractmethod
    async def get_node(self: Self, node_id: UUID, options: Any) -> Node:
        raise NotImplementedError

    @abstractmethod
    async def get_parent(self: Self, node_id: UUID, options: Any) -> UUID | None:
        raise NotImplementedError

    @abstractmethod
    async def get_logfire_ctx(self: Self, node_id: UUID, options: Any) -> Mapping[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_callbacks(self: Self, node_id: UUID) -> set[UUID]:
        raise NotImplementedError

    @abstractmethod
    async def set_parent(self: Self, node_id: UUID, parent_id: UUID) -> None:
        raise NotImplementedError

    # TODO: Rewrite for multiple nodes.
    # Returns `True` if all dependencies are resolved.
    @abstractmethod
    async def set_dependency_result(self: Self, node_id: UUID, dependency_id: UUID, result_id: UUID) -> bool:
        raise NotImplementedError

    # TODO: Rewrite for multiple nodes. Maybe merge with `set_dependency_result`.
    @abstractmethod
    async def set_callback_or_get_result_id(self: Self, node_id: UUID, dependent_id: UUID, options: Any) -> UUID | None:
        pass
