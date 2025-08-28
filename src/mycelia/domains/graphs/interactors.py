import asyncio
import functools
import typing
from asyncio import FIRST_COMPLETED, Event as Event_, Task
from collections.abc import Awaitable, Callable, MutableMapping, Sequence
from typing import Any, ClassVar, Final, Self, final
from uuid import UUID, uuid4

from mycelia import tracing
from mycelia.domains.graphs.entities import Call, Node
from mycelia.domains.graphs.errors import GraphError
from mycelia.services.broker.interface import IBroker
from mycelia.services.executor.interface import IExecutor
from mycelia.services.storage.interface import IStorage
from mycelia.tracing import LogfireSpan, Span, TraceContext
from mycelia.utils import Event, to_json

__all__: Final[tuple[str, ...]] = ("GraphsInteractor",)


@final
class GraphsInteractor:
    __slots__: ClassVar[tuple[str, ...]] = ()
    __RUNNING_GRAPHS: ClassVar[dict[UUID, Event_]] = {}

    @classmethod
    async def on_node_enqueued(
        cls: type[Self],
        /,
        node_id: UUID,
        graph_trace_context: TraceContext,
        *,
        broker: IBroker,
        storage: IStorage,
        executor: IExecutor,
    ) -> None:
        span: LogfireSpan
        with graph_trace_context.attach(), tracing.span(tracing.INFO, "node.process", node_id=node_id) as span:
            node: Final[Node] = await storage.get_node(node_id)

            if node.handler_id is None:
                tracing.trace("node.error.empty_handler", node_id=node_id)
                return

            span.message = f"Node: {node.handler_id}"
            span.set_attribute("node_arguments", node.arguments)

            # TODO: Explode.
            await cls.__process_node_callbacks(
                node_id,
                await cls.__process_node_result(
                    node.graph_id,
                    graph_trace_context,
                    *await cls.__execute_node(
                        node, graph_trace_context, broker=broker, storage=storage, executor=executor
                    ),
                    broker=broker,
                    storage=storage,
                ),
                broker=broker,
            )

    @classmethod
    async def on_graph_cancelled(cls: type[Self], /, graph_id: UUID) -> None:
        if graph_id in cls.__RUNNING_GRAPHS:
            cls.__RUNNING_GRAPHS[graph_id].set()

    @classmethod
    async def orchestrate_call(cls: type[Self], /, call: Call, *, broker: IBroker, storage: IStorage) -> UUID:
        if call.handler_id is None:
            message: Final[str] = "You can use pause only as a dependency."
            raise RuntimeError(message)

        graph_id: Final[UUID] = uuid4()
        with tracing.span(
            tracing.INFO, "graph.process", message_=f"Graph: {call.handler_id}", graph_id=graph_id, is_dependency=False
        ):
            graph_trace_context: Final[TraceContext] = TraceContext.get_current()
            await storage.create_graph(graph_id, graph_trace_context)

        return await cls.__orchestrate_call(
            graph_id,
            graph_trace_context,
            call,
            orchestrated_calls={},
            is_dependency=False,
            broker=broker,
            storage=storage,
        )

    @classmethod
    async def cancel_graph(cls: type[Self], /, graph_id: UUID, *, broker: IBroker, storage: IStorage) -> None:
        # TODO: Make it concurrent.
        for graph_id in await storage.mark_graph_cancelled(graph_id):  # noqa: PLR1704, B020
            await broker.publish_graph_cancelled(graph_id)
            tracing.warning("graph.cancelled", graph_id=graph_id)

    @classmethod
    async def submit_graph_result(
        cls: type[Self], /, graph_id: UUID, result: Any, *, broker: IBroker, storage: IStorage
    ) -> None:
        # TODO: Make it concurrent.
        ready_node_or_canceled_graph: tuple[UUID, TraceContext, Any] | UUID
        for ready_node_or_canceled_graph in await storage.mark_graph_completed(graph_id, result):
            if isinstance(ready_node_or_canceled_graph, tuple):
                await broker.publish_node_enqueued(*ready_node_or_canceled_graph)
                tracing.trace("node.enqueued", node_id=ready_node_or_canceled_graph[0])
                continue

            await broker.publish_graph_cancelled(ready_node_or_canceled_graph)
            tracing.warning("graph.cancelled", graph_id=ready_node_or_canceled_graph)

    @classmethod
    @tracing.with_span(tracing.TRACE, "call.orchestrate")
    async def __orchestrate_call(  # noqa: PLR0913
        cls: type[Self],
        /,
        graph_id: UUID,
        graph_trace_context: TraceContext,
        call: Call,
        orchestrated_calls: MutableMapping[UUID, Event[UUID]],
        *,
        broker: IBroker,
        storage: IStorage,
        is_dependency: bool,
    ) -> UUID:
        span: Final[Span] = tracing.get_current_span()
        span.set_attribute("call_id", to_json(call.id))

        if call.id in orchestrated_calls:
            tracing.debug("call.note.already_orchestrated", call_id=call.id)
            return await orchestrated_calls[call.id].wait()

        # TODO: Reduce nesting.
        event: Event[UUID]
        with Event[UUID]() as event:
            # This makes this function friendly to concurrent orchestrating of same calls.
            orchestrated_calls[call.id] = event

            if is_dependency:
                graph_id = uuid4()
                with (
                    graph_trace_context.attach(),
                    tracing.span(
                        tracing.INFO,
                        "graph.process",
                        message_=f"Subgraph: {call.handler_id}" if call.handler_id is not None else "Pause",
                        graph_id=graph_id,
                        is_dependency=True,
                    ),
                ):
                    graph_trace_context = TraceContext.get_current()
                    await storage.create_graph(graph_id, graph_trace_context)

            span.set_attribute("graph_id", to_json(graph_id))
            with tracing.span(tracing.TRACE, "call.process_arguments", call_id=call.id):
                arguments: Final[dict[int | str, Any]] = {}
                dependencies: Final[dict[int | str, UUID]] = {}
                key: int | str
                argument: Any
                for key, argument in call.arguments.items():
                    if not isinstance(argument, Call):
                        arguments[key] = argument
                        tracing.trace("call.note.found_literal_argument", key=key, argument=argument)
                        continue

                    # TODO: Make it concurrent.
                    dependencies[key] = await cls.__orchestrate_call(
                        graph_id,
                        graph_trace_context,
                        argument,
                        orchestrated_calls,
                        is_dependency=True,
                        broker=broker,
                        storage=storage,
                    )

            is_ready: Final[bool] = await storage.create_node(
                call.id,
                graph_id,
                call.handler_id,
                arguments,
                dependencies,
                call.broker_options,
                call.executor_options,
                call.storage_options,
            )

            if call.handler_id is not None and is_ready:
                await broker.publish_node_enqueued(call.id, graph_trace_context, call.broker_options)

            tracing.trace("call.note.orchestrated", call_id=call.id)
            event.set(graph_id)
            return graph_id

    @classmethod
    @tracing.with_span(tracing.TRACE, "node.execute")
    async def __execute_node(
        cls: type[Self],
        /,
        node: Node,
        graph_trace_context: TraceContext,
        *,
        broker: IBroker,
        storage: IStorage,
        executor: IExecutor,
    ) -> tuple[Any, dict[UUID, Event[UUID]]]:
        orchestrated_calls: Final[dict[UUID, Event[UUID]]] = {}
        _orchestrate_background_call: Final[Callable[[Call], Awaitable]] = functools.partial(
            cls.__orchestrate_call,
            node.graph_id,
            graph_trace_context,
            orchestrated_calls=orchestrated_calls,
            is_dependency=True,
            broker=broker,
            storage=storage,
        )

        event: Final[Event_] = cls.__RUNNING_GRAPHS.get(node.graph_id, Event_())
        if node.graph_id not in cls.__RUNNING_GRAPHS:
            cls.__RUNNING_GRAPHS[node.graph_id] = event

        event_task: Final[Task] = asyncio.create_task(event.wait())
        executor_task: Final[Task] = asyncio.create_task(
            executor.execute_node(node.handler_id, node.arguments, _orchestrate_background_call, node.executor_options)
        )

        await asyncio.wait(fs=(event_task, executor_task), return_when=FIRST_COMPLETED)
        cls.__RUNNING_GRAPHS.pop(node.graph_id, None)
        event_task.cancel()
        executor_task.cancel()

        if event_task.done():
            tracing.warning("node.note.cancelled")
            return None, orchestrated_calls

        exception: Final[BaseException | None] = executor_task.exception()
        if isinstance(exception, GraphError):
            # We only need to log cause of the exception.
            tracing.error("node.error.execute", exception_=exception.__cause__)
            return exception.__cause__, orchestrated_calls

        return executor_task.result(), orchestrated_calls

    @classmethod
    @tracing.with_span(tracing.TRACE, "node.process_result")
    async def __process_node_result(  # noqa: PLR0913
        cls: type[Self],
        /,
        graph_id: UUID,
        graph_trace_context: TraceContext,
        result: Any,
        orchestrated_calls: MutableMapping[UUID, Event[UUID]],
        *,
        broker: IBroker,
        storage: IStorage,
    ) -> list[tuple[UUID, TraceContext, Any] | UUID]:
        tracing.get_current_span().set_attribute("graph_id", to_json(graph_id))

        if isinstance(result, BaseException):
            return typing.cast(
                "list[tuple[UUID, TraceContext, Any] | UUID]", await storage.mark_graph_cancelled(graph_id)
            )

        if not isinstance(result, Call):
            return await storage.mark_graph_completed(graph_id, result)

        next_graph_id: Final[UUID] = await cls.__orchestrate_call(
            graph_id,
            graph_trace_context,
            result,
            orchestrated_calls,
            is_dependency=False,
            broker=broker,
            storage=storage,
        )

        return await storage.link_graphs(graph_id, next_graph_id) if graph_id != next_graph_id else []

    @classmethod
    @tracing.with_span(tracing.TRACE, "node.process_callbacks")
    async def __process_node_callbacks(
        cls: type[Self],
        /,
        node_id: UUID,
        ready_nodes_or_canceled_graphs: Sequence[tuple[UUID, TraceContext, Any] | UUID],
        *,
        broker: IBroker,
    ) -> None:
        tracing.get_current_span().set_attribute("node_id", to_json(node_id))

        # TODO: Make it concurrent.
        ready_node_or_canceled_graph: tuple[UUID, TraceContext, Any] | UUID
        for ready_node_or_canceled_graph in ready_nodes_or_canceled_graphs:
            if isinstance(ready_node_or_canceled_graph, tuple):
                await broker.publish_node_enqueued(*ready_node_or_canceled_graph)
                tracing.trace("node.enqueued", node_id=ready_node_or_canceled_graph[0])
                continue

            await broker.publish_graph_cancelled(ready_node_or_canceled_graph)
            tracing.warning("graph.cancelled", graph_id=ready_node_or_canceled_graph)
