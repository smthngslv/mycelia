from abc import abstractmethod
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final, Protocol, Self

from mycelia.domains.graphs.entities import Call

__all__: Final[tuple[str, ...]] = ("IExecutor",)


class IExecutor(Protocol):
    @abstractmethod
    async def execute_node(
        self: Self,
        /,
        handler_id: Any,
        arguments: Mapping[int | str, Any],
        orchestrate_background_call: Callable[[Call], Awaitable],
        options: Any,
    ) -> Any:
        raise NotImplementedError
