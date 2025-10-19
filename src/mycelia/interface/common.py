import inspect
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping, Sequence, Set as AbstractSet
from inspect import Parameter, Signature
from typing import Any, ClassVar, Concatenate, Final, Self, cast, final, overload
from uuid import UUID, uuid4

from pydantic import TypeAdapter

from mycelia.utils import gather

__all__: Final[tuple[str, ...]] = ("Graph", "Node", "NodeCall", "NodeCalls", "RunContext", "group", "node", "pause")


PAUSE_MARKER: Final[object] = object()


@final
class RunContext[SP: Any, BP: Any, EP: Any]:
    __slots__: ClassVar[tuple[str, ...]] = ("__invoke_node", "graph_id", "node_id", "session_id")

    def __init__(
        self: Self,
        /,
        session_id: UUID,
        graph_id: UUID,
        node_id: UUID,
        invoke_node: "Callable[[NodeCall[Any, Any, SP, BP, EP], bool], Coroutine[Any, Any, UUID]]",
    ) -> None:
        self.session_id: Final[UUID] = session_id
        self.graph_id: Final[UUID] = graph_id
        self.node_id: Final[UUID] = node_id
        self.__invoke_node: Final[Callable[[NodeCall[Any, Any, SP, BP, EP], bool], Coroutine[Any, Any, UUID]]] = (
            invoke_node
        )

    async def start_session(self: Self, /, node_call: "NodeCall[Any, Any, SP, BP, EP]") -> UUID:
        return await self.__invoke_node(node_call, True)  # noqa: FBT003

    @overload
    async def run_concurrently[T: NodeCall[Any, Any, SP, BP, EP]](self: Self, call: T, /) -> T:
        raise NotImplementedError

    @overload
    async def run_concurrently[R1: Any, R2: Any](
        self: Self, call_1: "NodeCall[Any, R1, SP, BP, EP]", call_2: "NodeCall[Any, R2, SP, BP, EP]", /
    ) -> "NodeCalls[R1, R2, SP, BP, EP]":
        raise NotImplementedError

    @overload
    async def run_concurrently[R1: Any, R2: Any, R3: Any](
        self: Self,
        call_1: "NodeCall[Any, R1, SP, BP, EP]",
        call_2: "NodeCall[Any, R2, SP, BP, EP]",
        call_3: "NodeCall[Any, R3, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, SP, BP, EP]":
        raise NotImplementedError

    @overload
    async def run_concurrently[R1: Any, R2: Any, R3: Any, R4: Any](
        self: Self,
        call_1: "NodeCall[Any, R1, SP, BP, EP]",
        call_2: "NodeCall[Any, R2, SP, BP, EP]",
        call_3: "NodeCall[Any, R3, SP, BP, EP]",
        call_4: "NodeCall[Any, R4, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, R4, SP, BP, EP]":
        raise NotImplementedError

    @overload
    async def run_concurrently[R1: Any, R2: Any, R3: Any, R4: Any, R5: Any](
        self: Self,
        call_1: "NodeCall[Any, R1, SP, BP, EP]",
        call_2: "NodeCall[Any, R2, SP, BP, EP]",
        call_3: "NodeCall[Any, R3, SP, BP, EP]",
        call_4: "NodeCall[Any, R4, SP, BP, EP]",
        call_5: "NodeCall[Any, R5, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, R4, R5, SP, BP, EP]":
        raise NotImplementedError

    @overload
    async def run_concurrently(
        self: Self, /, *calls: "NodeCall[Any, Any, SP, BP, EP]"
    ) -> "NodeCalls[*tuple[Any, ...], SP, BP, EP]":
        raise NotImplementedError

    async def run_concurrently(
        self: Self, /, *calls: "NodeCall[Any, Any, SP, BP, EP]"
    ) -> "NodeCall[Any, Any, SP, BP, EP] | NodeCalls[*tuple[Any, ...], SP, BP, EP]":
        if len(calls) == 0:
            raise ValueError

        await gather(self.__invoke_node(call, False) for call in calls)  # noqa: FBT003

        if len(calls) == 1:
            return calls[0]

        return NodeCalls[*tuple[Any, ...], SP, BP, EP](calls)


@final
class Graph[SP: Any, BP: Any, EP: Any]:
    __slots__: ClassVar[tuple[str, ...]] = ("__nodes",)

    def __init__(self: Self, /) -> None:
        self.__nodes: Final[set[Node[Any, Any, SP, BP, EP]]] = set()

    @property
    def nodes(self: Self, /) -> AbstractSet["Node[Any, Any, SP, BP,  EP]"]:
        return self.__nodes

    def add_node(self: Self, /, node: "Node[Any, Any, SP, BP,  EP]") -> None:
        if node in self.__nodes:
            message: Final[str] = f"Node {node} already in the graph."
            raise RuntimeError(message)

        self.__nodes.add(node)


@final
class Node[**P, R: Any, SP: Any, BP: Any, EP: Any]:
    __slots__: ClassVar[tuple[str, ...]] = (
        "broker_params",
        "executor_params",
        "function",
        "parameters",
        "return_value_type_adapter",
        "signature",
        "storage_params",
        "type_adapters",
    )

    def __init__(
        self: Self,
        /,
        function_or_signature: Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]] | Signature,
        storage_params: SP,
        broker_params: BP,
        executor_params: EP,
    ) -> None:
        self.function: Final[Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]] | None] = (
            function_or_signature if not isinstance(function_or_signature, Signature) else None
        )
        self.signature: Final[Signature] = (
            function_or_signature
            if isinstance(function_or_signature, Signature)
            else inspect.signature(function_or_signature)
        )
        self.parameters: Final[Mapping[int, tuple[Parameter, TypeAdapter]]] = OrderedDict(
            (index, (parameter, TypeAdapter(parameter.annotation)))
            # It's okay to use it like this, it's ordered and stable.
            for index, parameter in enumerate(self.signature.parameters.values())
            # The first argument is `RunContext`.
            if index != 0
        )
        self.return_value_type_adapter: Final[TypeAdapter] = TypeAdapter(self.signature.return_annotation)

        self.storage_params: Final[SP] = storage_params
        self.broker_params: Final[BP] = broker_params
        self.executor_params: Final[EP] = executor_params

    @overload
    def implementation(self: Self, function: Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]], /) -> Self:
        raise NotImplementedError

    @overload
    def implementation(
        self: Self, graph: Graph[SP, BP, EP], /
    ) -> Callable[[Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]]], Self]:
        raise NotImplementedError

    def implementation(
        self: Self, function_or_graph: Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]] | Graph, /
    ) -> Self | Callable[[Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]]], Self]:
        if not isinstance(function_or_graph, Graph):
            return Node(function_or_graph, self.storage_params, self.broker_params, self.executor_params)

        def decorator(function: Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]]) -> Self:
            node: Final[Node[P, R, SP, BP, EP]] = self.implementation(function)
            function_or_graph.add_node(node)
            return node

        return decorator

    def __call__(self: Self, /, *args: P.args, **kwargs: P.kwargs) -> "NodeCall[P, R, SP, BP, EP]":
        return NodeCall(self, *args, **kwargs)

    def __repr__(self: Self, /) -> str:
        return (
            f"<Node {self.signature.__repr__()[11:]} "
            f"storage_params={self.storage_params} "
            f"broker_params={self.broker_params} "
            f"executor_params={self.executor_params}>"
        )


@final
class NodeCall[**P, R: Any, SP: Any, BP: Any, EP: Any]:
    __slots__: ClassVar[tuple[str, ...]] = ("arguments", "dependencies", "id", "node")

    def __init__(self: Self, /, node: Node[P, R, SP, BP, EP], *args: P.args, **kwargs: P.kwargs) -> None:
        self.id: Final[UUID] = uuid4()
        self.node: Final[Node[P, R, SP, BP, EP]] = node
        # This are non-data dependencies.
        self.dependencies: Final[set[NodeCall[Any, Any, SP, BP, EP]]] = set()
        # Normalize `args` and `kwargs`.
        arguments: Final[dict[str, Any]] = node.signature.bind(None, *args, **kwargs).arguments
        self.arguments: Final[Mapping[int, Any]] = OrderedDict(
            (index, arguments[parameter.name])
            for index, (parameter, _) in node.parameters.items()
            if parameter.name in arguments
        )

    @property
    def value(self: Self, /) -> R:
        return cast("R", self)

    @overload
    def then(self: Self, /) -> Self:
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any](self: Self, call_1: "NodeCall[P1, R1, SP, BP, EP]", /) -> "NodeCall[P1, R1, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any, **P2, R2: Any](
        self: Self, call_1: "NodeCall[P1, R1, SP, BP, EP]", call_2: "NodeCall[P2, R2, SP, BP, EP]", /
    ) -> "NodeCalls[R1, R2, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any, **P2, R2: Any, **P3, R3: Any](
        self: Self,
        call_1: "NodeCall[P1, R1, SP, BP, EP]",
        call_2: "NodeCall[P2, R2, SP, BP, EP]",
        call_3: "NodeCall[P3, R3, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any, **P2, R2: Any, **P3, R3: Any, **P4, R4: Any](
        self: Self,
        call_1: "NodeCall[P1, R1, SP, BP, EP]",
        call_2: "NodeCall[P2, R2, SP, BP, EP]",
        call_3: "NodeCall[P3, R3, SP, BP, EP]",
        call_4: "NodeCall[P4, R4, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, R4, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any, **P2, R2: Any, **P3, R3: Any, **P4, R4: Any, **P5, R5: Any](
        self: Self,
        call_1: "NodeCall[P1, R1, SP, BP, EP]",
        call_2: "NodeCall[P2, R2, SP, BP, EP]",
        call_3: "NodeCall[P3, R3, SP, BP, EP]",
        call_4: "NodeCall[P4, R4, SP, BP, EP]",
        call_5: "NodeCall[P5, R5, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, R4, R5, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then(self: Self, /, *calls: "NodeCall[Any, Any, SP, BP, EP]") -> "NodeCalls[*tuple[Any, ...], SP, BP, EP]":
        raise NotImplementedError

    def then(
        self: Self, /, *calls: "NodeCall[Any, Any, SP, BP, EP]"
    ) -> "NodeCall[Any, Any, SP, BP, EP] | NodeCalls[*tuple[Any, ...], SP, BP, EP]":
        if len(calls) == 0:
            return self

        call: NodeCall[Any, Any, SP, BP, EP]
        for call in calls:
            call.dependencies.add(self)

        if len(calls) == 1:
            return calls[0]

        return NodeCalls[*tuple[Any, ...], SP, BP, EP](calls)

    def __repr__(self: Self, /) -> str:
        return f"<NodeCall `{self.id}` arguments={self.arguments}, dependencies={self.dependencies}, node={self.node}>"


@final
class NodeCalls[*R, SP: Any, BP: Any, EP: Any]:
    __slots__: ClassVar[tuple[str, ...]] = ("calls",)

    def __init__(self: Self, /, calls: Sequence[NodeCall[Any, Any, SP, BP, EP]]) -> None:
        self.calls: Final[Sequence[NodeCall[Any, Any, SP, BP, EP]]] = calls

    @property
    def values(self: Self, /) -> tuple[*R]:
        return cast("tuple[*R]", self)

    @overload
    def then(self: Self, /) -> Self:
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any](self: Self, call_1: "NodeCall[P1, R1, SP, BP, EP]", /) -> "NodeCall[P1, R1, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any, **P2, R2: Any](
        self: Self, call_1: "NodeCall[P1, R1, SP, BP, EP]", call_2: "NodeCall[P2, R2, SP, BP, EP]", /
    ) -> "NodeCalls[R1, R2, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any, **P2, R2: Any, **P3, R3: Any](
        self: Self,
        call_1: "NodeCall[P1, R1, SP, BP, EP]",
        call_2: "NodeCall[P2, R2, SP, BP, EP]",
        call_3: "NodeCall[P3, R3, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any, **P2, R2: Any, **P3, R3: Any, **P4, R4: Any](
        self: Self,
        call_1: "NodeCall[P1, R1, SP, BP, EP]",
        call_2: "NodeCall[P2, R2, SP, BP, EP]",
        call_3: "NodeCall[P3, R3, SP, BP, EP]",
        call_4: "NodeCall[P4, R4, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, R4, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then[**P1, R1: Any, **P2, R2: Any, **P3, R3: Any, **P4, R4: Any, **P5, R5: Any](
        self: Self,
        call_1: "NodeCall[P1, R1, SP, BP, EP]",
        call_2: "NodeCall[P2, R2, SP, BP, EP]",
        call_3: "NodeCall[P3, R3, SP, BP, EP]",
        call_4: "NodeCall[P4, R4, SP, BP, EP]",
        call_5: "NodeCall[P5, R5, SP, BP, EP]",
        /,
    ) -> "NodeCalls[R1, R2, R3, R4, R5, SP, BP, EP]":
        raise NotImplementedError

    @overload
    def then(self: Self, /, *calls: "NodeCall[Any, Any, SP, BP, EP]") -> "NodeCalls[*tuple[Any, ...], SP, BP, EP]":
        raise NotImplementedError

    def then(
        self: Self, /, *calls: "NodeCall[Any, Any, SP, BP, EP]"
    ) -> "NodeCall[Any, Any, SP, BP, EP] | NodeCalls[*tuple[Any, ...], SP, BP, EP]":
        if len(calls) == 0:
            return self

        call: NodeCall[Any, Any, SP, BP, EP]
        for call in calls:
            call.dependencies.update(self.calls)

        if len(calls) == 1:
            return calls[0]

        return NodeCalls[*tuple[Any, ...], SP, BP, EP](calls)

    def __iter__(self: Self, /) -> Iterator[NodeCall[Any, Any, SP, BP, EP]]:
        yield from self.calls


def node[**P, R: Any, SP: Any, BP: Any, EP: Any](
    graph: Graph[SP, BP, EP] | None = None, /, *, storage_params: SP, broker_params: BP, executor_params: EP
) -> Callable[[Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]]], Node[P, R, SP, BP, EP]]:
    def decorator(
        function: Callable[Concatenate[RunContext[SP, BP, EP], P], Awaitable[R]], /
    ) -> Node[P, R, SP, BP, EP]:
        node: Final[Node[P, R, SP, BP, EP]] = Node(function, storage_params, broker_params, executor_params)

        if graph is not None:
            graph.add_node(node)

        return node

    return decorator


def pause[R](_: type[R]) -> R:
    return cast("R", PAUSE_MARKER)


@overload
def group[R1: Any, R2: Any, SP: Any, BP: Any, EP: Any](
    call_1: NodeCall[Any, R1, SP, BP, EP], call_2: NodeCall[Any, R2, SP, BP, EP], /
) -> NodeCalls[R1, R2, SP, BP, EP]:
    raise NotImplementedError


@overload
def group[R1: Any, R2: Any, R3: Any, SP: Any, BP: Any, EP: Any](
    call_1: NodeCall[Any, R1, SP, BP, EP],
    call_2: NodeCall[Any, R2, SP, BP, EP],
    call_3: NodeCall[Any, R3, SP, BP, EP],
    /,
) -> NodeCalls[R1, R2, R3, SP, BP, EP]:
    raise NotImplementedError


@overload
def group[R1: Any, R2: Any, R3: Any, R4: Any, SP: Any, BP: Any, EP: Any](
    call_1: NodeCall[Any, R1, SP, BP, EP],
    call_2: NodeCall[Any, R2, SP, BP, EP],
    call_3: NodeCall[Any, R3, SP, BP, EP],
    call_4: NodeCall[Any, R4, SP, BP, EP],
    /,
) -> NodeCalls[R1, R2, R3, R4, SP, BP, EP]:
    raise NotImplementedError


@overload
def group[R1: Any, R2: Any, R3: Any, R4: Any, R5: Any, SP: Any, BP: Any, EP: Any](
    call_1: NodeCall[Any, R1, SP, BP, EP],
    call_2: NodeCall[Any, R2, SP, BP, EP],
    call_3: NodeCall[Any, R3, SP, BP, EP],
    call_4: NodeCall[Any, R4, SP, BP, EP],
    call_5: NodeCall[Any, R5, SP, BP, EP],
    /,
) -> NodeCalls[R1, R2, R3, R4, R5, SP, BP, EP]:
    raise NotImplementedError


@overload
def group[SP: Any, BP: Any, EP: Any](*calls: NodeCall[Any, Any, SP, BP, EP]) -> NodeCalls[*tuple[Any, ...], SP, BP, EP]:
    raise NotImplementedError


def group[SP: Any, BP: Any, EP: Any](*calls: NodeCall[Any, Any, SP, BP, EP]) -> NodeCalls[*tuple[Any, ...], SP, BP, EP]:
    return NodeCalls[*tuple[Any, ...], SP, BP, EP](calls)
