import functools
import inspect
import typing
from abc import abstractmethod
from collections.abc import Awaitable, Callable, Set as AbstractSet
from inspect import BoundArguments, Signature
from types import EllipsisType
from typing import Any, ClassVar, Concatenate, Final, Protocol, Self, final, overload
from uuid import uuid4

from mycelia.domains.nodes.entities import NodeCall

__all__: Final[tuple[str, ...]] = ("Context", "Graph", "Node", "SupportsUnion", "node")


class SupportsUnion(Protocol):
    __slots__: ClassVar[tuple[str, ...]] = ()

    @abstractmethod
    def __or__(self: Self, other: Self, /) -> Self:
        raise NotImplementedError


@final
class Context:
    __slots__: ClassVar[tuple[str, ...]] = ("__orchestrate_node_call",)

    def __init__(self: Self, orchestrate_node_call: Callable[[NodeCall], Awaitable], /) -> None:
        self.__orchestrate_node_call: Final[Callable[[NodeCall], Awaitable]] = orchestrate_node_call

    async def submit(self: Self, node_call: Any, /) -> None:
        if not isinstance(node_call, NodeCall):
            message: Final[str] = f"Expected NodeCall object, got {type(node_call)}."
            raise TypeError(message)

        await self.__orchestrate_node_call(node_call)


@final
class Graph:
    __slots__: ClassVar[tuple[str, ...]] = (
        "__nodes",
        "broker_options",
        "executor_options",
        "get_id",
        "storage_options",
    )

    def __init__(
        self: Self,
        /,
        *,
        get_id: Callable[[Callable], Any] | None = None,
        broker_options: SupportsUnion | None = None,
        storage_options: SupportsUnion | None = None,
        executor_options: SupportsUnion | None = None,
    ) -> None:
        self.get_id: Final[Callable[[Callable], Any] | None] = get_id
        self.broker_options: Final[SupportsUnion | None] = broker_options
        self.storage_options: Final[SupportsUnion | None] = storage_options
        self.executor_options: Final[SupportsUnion | None] = executor_options
        self.__nodes: Final[set[Node]] = set()

    @property
    def nodes(self: Self, /) -> AbstractSet["Node"]:
        return self.__nodes

    def add_node(self: Self, node: "Node", /) -> None:
        if node in self.__nodes:
            message: Final[str] = f"Node {node} already in the graph."
            raise RuntimeError(message)

        self.__nodes.add(node)


@final
class Node[**P, R]:
    __slots__: ClassVar[tuple[str, ...]] = (
        "__signature",
        "broker_options",
        "executor_options",
        "function",
        "id",
        "storage_options",
    )

    def __init__(  # noqa: PLR0913
        self: Self,
        /,
        function: Callable[Concatenate[Context, P], Awaitable[R]],
        *,
        id: Any = None,  # noqa: A002
        get_id: Callable[[Callable], Any] | None = None,
        broker_options: SupportsUnion | None = None,
        storage_options: SupportsUnion | None = None,
        executor_options: SupportsUnion | None = None,
    ) -> None:
        if id is not None and get_id is not None:
            message: str = "Use either `id` or `get_id` instead."
            raise RuntimeError(message)

        if get_id is not None:
            id = get_id(function)  # noqa: A001

        elif id is None and hasattr(function, "__qualname__"):
            id = function.__qualname__  # noqa: A001

        if id is None:
            message = f"Cannot generate `id` for the function: `{function}`."
            raise RuntimeError(message)

        self.id: Final[Any] = id
        self.function: Final[Callable[Concatenate[Context, P], Awaitable[R]]] = function
        self.broker_options: Final[SupportsUnion | None] = broker_options
        self.storage_options: Final[SupportsUnion | None] = storage_options
        self.executor_options: Final[SupportsUnion | None] = executor_options
        self.__signature: Final[Signature] = inspect.signature(function)

    def configure(
        self: Self,
        /,
        *,
        broker_options: SupportsUnion | EllipsisType | None = ...,
        storage_options: SupportsUnion | EllipsisType | None = ...,
        executor_options: SupportsUnion | EllipsisType | None = ...,
    ) -> Self:
        if isinstance(broker_options, EllipsisType):
            broker_options = self.broker_options

        elif broker_options is not None and self.broker_options is not None:
            broker_options = self.broker_options | broker_options

        if isinstance(storage_options, EllipsisType):
            storage_options = self.storage_options

        elif storage_options is not None and self.storage_options is not None:
            storage_options = self.storage_options | storage_options

        if isinstance(executor_options, EllipsisType):
            executor_options = self.executor_options

        elif executor_options is not None and self.executor_options is not None:
            executor_options = self.executor_options | executor_options

        return type(self)(
            self.function,
            id=self.id,
            broker_options=broker_options,
            storage_options=storage_options,
            executor_options=executor_options,
        )

    @overload
    def implementation(self: Self, function: Callable[Concatenate[Context, P], Awaitable[R]], /) -> Self:
        raise NotImplementedError

    @overload
    def implementation(
        self: Self,
        graph: Graph | None = None,
        /,
        *,
        broker_options: SupportsUnion | EllipsisType | None = ...,
        storage_options: SupportsUnion | EllipsisType | None = ...,
        executor_options: SupportsUnion | EllipsisType | None = ...,
    ) -> Callable[[Callable[Concatenate[Context, P], Awaitable[R]]], Self]:
        raise NotImplementedError

    def implementation(
        self: Self,
        function_or_graph: Callable[Concatenate[Context, P], Awaitable[R]] | Graph | None = None,
        function: Callable[Concatenate[Context, P], Awaitable[R]] | None = None,
        /,
        **kwargs: Any,
    ) -> Self | Callable[[Callable[Concatenate[Context, P], Awaitable[R]]], Self]:
        if (function_or_graph is not None and not isinstance(function_or_graph, Graph)) or function is not None:
            node_: Final[Self] = type(self)(
                typing.cast(
                    "Callable[Concatenate[Context, P], Awaitable[R]]",
                    function if function is not None else function_or_graph,
                ),
                id=self.id,
                broker_options=self.broker_options,
                storage_options=self.storage_options,
                executor_options=self.executor_options,
            ).configure(**kwargs)

            if isinstance(function_or_graph, Graph):
                function_or_graph.add_node(node_)

            return node_

        return typing.cast(
            "Callable[[Callable[Concatenate[Context, P], Awaitable[R]]], Node[P, R]]",
            functools.partial(self.implementation, function_or_graph, **kwargs),
        )

    def __call__(self: Self, /, *args: P.args, **kwargs: P.kwargs) -> R:
        # Normalize `args` and `kwargs`.
        binding: Final[BoundArguments] = self.__signature.bind(None, *args, **kwargs)
        arguments: Final[dict[int | str, Any]] = {}
        arguments.update(enumerate(binding.args, start=-1))
        arguments.update(binding.kwargs.items())
        # First argument is actually a `Context` object.
        arguments.pop(-1)
        return NodeCall(  # type: ignore[return-value]
            id=uuid4(),
            handler_id=self.id,
            arguments=arguments,
            broker_options=self.broker_options,
            storage_options=self.storage_options,
            executor_options=self.executor_options,
        )

    def __eq__(self: Self, /, other: Any) -> bool:
        return isinstance(other, type(self)) and self.id == other.id

    def __hash__(self: Self, /) -> int:
        return hash(self.id)

    def __repr__(self: Self, /) -> str:
        return (
            f"<Node "
            f"`{self.id}` {self.__signature.__repr__()[11:]} "
            f"broker_options={self.broker_options} "
            f"storage_options={self.storage_options} "
            f"executor_options={self.executor_options}>"
        )


@overload
def node[**P, R](function: Callable[Concatenate[Context, P], Awaitable[R]], /) -> Node[P, R]:
    raise NotImplementedError


@overload
def node[**P, R](
    graph: Graph | None = None, /, **kwargs: Any
) -> Callable[[Callable[Concatenate[Context, P], Awaitable[R]]], Node[P, R]]:
    raise NotImplementedError


def node[**P, R](
    function_or_graph: Callable[Concatenate[Context, P], Awaitable[R]] | Graph | None = None,
    function: Callable[P, Awaitable[R]] | None = None,
    /,
    **kwargs: Any,
) -> Node[P, R] | Callable[[Callable[Concatenate[Context, P], Awaitable[R]]], Node[P, R]]:
    if (function_or_graph is not None and not isinstance(function_or_graph, Graph)) or function is not None:
        node_: Final[Node[P, R]] = Node(
            typing.cast(
                "Callable[Concatenate[Context, P], Awaitable[R]]",
                function if function is not None else function_or_graph,
            ),
            **kwargs,
        )

        if isinstance(function_or_graph, Graph):
            function_or_graph.add_node(node_)

        return node_

    return typing.cast(
        "Callable[[Callable[Concatenate[Context, P], Awaitable[R]]], Node[P, R]]",
        functools.partial(node, function_or_graph, **kwargs),
    )
