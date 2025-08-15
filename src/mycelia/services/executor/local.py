from collections.abc import Awaitable, Callable, Mapping
from typing import Any, ClassVar, Final, Self, final

from mycelia.domains.nodes.entities import NodeCall
from mycelia.presenters.main import Context
from mycelia.services.executor.interface import IExecutor

__all__: Final[tuple[str, ...]] = ("LocalExecutor",)


@final
class LocalExecutor(IExecutor):
    __slots__: ClassVar[tuple[str, ...]] = ("__handlers",)

    def __init__(self: Self, handlers: Mapping[Any, Callable[..., Awaitable]]) -> None:
        self.__handlers: Final[dict[Any, Callable[..., Awaitable]]] = dict(handlers)

    async def execute_node(
        self: Self,
        handler_id: Any,
        arguments: Mapping[int | str, Any],
        orchestrate_node_call: Callable[[NodeCall], Awaitable],
        options: Any,  # noqa: ARG002
    ) -> Any:
        kwargs: Final[dict[str, Any]] = {key: argument for key, argument in arguments.items() if isinstance(key, str)}
        args: Final[tuple[Any, ...]] = tuple(arguments[index] for index in range(len(arguments) - len(kwargs)))
        ctx: Final[Context] = Context(orchestrate_node_call)
        return await self.__handlers[handler_id](ctx, *args, **kwargs)
