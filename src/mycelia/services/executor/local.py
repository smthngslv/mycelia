import typing
from asyncio import CancelledError
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, ClassVar, Final, Self, final

from mycelia.domains.graphs.entities import Call
from mycelia.domains.graphs.errors import GraphError
from mycelia.presenters.main import Context
from mycelia.services.executor.interface import IExecutor

if TYPE_CHECKING:
    from types import TracebackType

__all__: Final[tuple[str, ...]] = ("LocalExecutor",)


@final
class LocalExecutor(IExecutor):
    __slots__: ClassVar[tuple[str, ...]] = ("__handlers",)

    def __init__(self: Self, /, handlers: Mapping[Any, Callable[..., Awaitable]]) -> None:
        self.__handlers: Final[dict[Any, Callable[..., Awaitable]]] = dict(handlers)

    async def execute_node(
        self: Self,
        /,
        handler_id: Any,
        arguments: Mapping[int | str, Any],
        orchestrate_background_call: Callable[[Call], Awaitable],
        options: Any,  # noqa: ARG002
    ) -> Any:
        kwargs: Final[dict[str, Any]] = {key: argument for key, argument in arguments.items() if isinstance(key, str)}
        args: Final[tuple[Any, ...]] = tuple(arguments[index] for index in range(len(arguments) - len(kwargs)))
        ctx: Final[Context] = Context(orchestrate_background_call)

        try:
            return await self.__handlers[handler_id](ctx, *args, **kwargs)

        except CancelledError:
            return None

        except BaseException as exception:
            # Hide executor's frames from the traceback.
            raise GraphError from exception.with_traceback(
                typing.cast("TracebackType", typing.cast("TracebackType", exception.__traceback__).tb_next)
            )
