from abc import abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final, Protocol, Self

from mycelia.domains.nodes.entities import NodeCall

__all__: Final[tuple[str, ...]] = ("IExecutor",)


class IExecutor(Protocol):
    @abstractmethod
    async def execute_node(
        self: Self,
        handler_id: Any,
        arguments: Mapping[int | str, Any],
        orchestrate_node_call: Callable[[NodeCall], Awaitable],
        options: Any,
    ) -> Any:
        raise NotImplementedError
