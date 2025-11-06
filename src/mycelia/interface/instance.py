import functools
from typing import Any, Final, Self, final
from uuid import UUID

from mycelia.core.interactor import Interactor
from mycelia.interface.common import Graph, Node, NodeCall
from mycelia.interface.executor import Executor, ExecutorParams
from mycelia.services.broker.interface import IBroker
from mycelia.services.storage.interface import IStorage
from mycelia.tracing import Tracer
from mycelia.utils import gather

__all__: Final[tuple[str, ...]] = ("Client", "Server")

TRACER: Final[Tracer] = Tracer(__name__)


@final
class Client[SP: Any, BP: Any]:
    __slots__: Final[tuple[str, ...]] = ("__broker", "__storage")
    __TRACER: Final[Tracer] = TRACER.get_child("client")

    def __init__(self: Self, /, storage: IStorage[SP], broker: IBroker[BP]) -> None:
        self.__storage: Final[IStorage[SP]] = storage
        self.__broker: Final[IBroker[BP]] = broker

    @__TRACER.with_span_async(Tracer.INFO, "client.start_session")
    async def start_session(self: Self, node_call: NodeCall[Any, Any, SP, BP, ExecutorParams], /) -> UUID:
        return await Interactor.invoke_node(
            node=Executor.get_invoked_node(node_call),
            storage=self.__storage,
            broker=self.__broker,
            executor=Executor,
            get_node_trace_message=lambda node: f"Node: {node.executor_params.node_id}",
            get_graph_trace_message=lambda node: f"Graph: {node.executor_params.node_id}",
        )

    @__TRACER.with_span_async(Tracer.INFO, "client.cancel_session")
    async def cancel_session(self: Self, id_: UUID, /) -> None:
        return await Interactor.cancel_session(id_, storage=self.__storage, broker=self.__broker)


@final
class Server[SP: Any, BP: Any]:
    __slots__: Final[tuple[str, ...]] = ("__broker", "__executor", "__storage")
    __TRACER: Final[Tracer] = TRACER.get_child("server")

    @classmethod
    @__TRACER.with_span_async(Tracer.INFO, "server.create")
    async def create(cls: type[Self], storage: IStorage[SP], broker: IBroker[BP]) -> Self:
        await broker.add_on_session_cancelled_callback(Interactor.on_session_cancelled)
        return cls(storage, broker)

    def __init__(self: Self, /, storage: IStorage[SP], broker: IBroker[BP]) -> None:
        self.__storage: Final[IStorage[SP]] = storage
        self.__broker: Final[IBroker[BP]] = broker
        self.__executor: Final[Executor] = Executor()

    @__TRACER.with_span_async(Tracer.INFO, "server.serve_node")
    async def serve_node(self: Self, node: Node[Any, Any, SP, BP, ExecutorParams], /) -> None:
        # TODO: Support removing node.
        self.__executor.serve_node(node)
        await self.__broker.add_on_node_enqueued_callback(
            node.broker_params,
            callback=functools.partial(
                Interactor.on_node_enqueued,
                storage=self.__storage,
                broker=self.__broker,
                executor=self.__executor,
                get_node_trace_message=lambda node: f"Node: {node.executor_params.node_id}",
                get_graph_trace_message=lambda node: f"Graph: {node.executor_params.node_id}",
            ),
        )

    @__TRACER.with_span_async(Tracer.INFO, "server.serve_graph")
    async def serve_graph(self: Self, graph: Graph[SP, BP, ExecutorParams], /) -> None:
        # TODO: Support removing graph.
        await gather(map(self.serve_node, graph.nodes))
