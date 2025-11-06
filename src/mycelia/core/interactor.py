import asyncio
from asyncio import Task, TaskGroup
from collections.abc import Callable
from contextlib import ExitStack
from typing import Any, ClassVar, Final, Literal, Self, cast, final, overload
from uuid import UUID, uuid4

from mycelia.core.entities import (
    CompletedNode,
    Context,
    CreatedGraph,
    CreatedNode,
    CreatedSession,
    EnqueuedNode,
    InvokedNode,
    ReadyNode,
    RunningNode,
    StartedNode,
)
from mycelia.services.broker.interface import IBroker
from mycelia.services.executor import IExecutor
from mycelia.services.storage.interface import IStorage
from mycelia.tracing import TraceContext, Tracer
from mycelia.utils import EventWithSubscribers, EventWithValue, SingleUseLockWithValue, gather

__all__: Final[tuple[str, ...]] = ("Interactor",)


@final
class Interactor:
    __TRACER: Final[Tracer] = Tracer(__name__)
    __SESSIONS: ClassVar[dict[UUID, EventWithSubscribers]] = {}

    @overload
    @classmethod
    async def invoke_node[SP: Any, BP: Any, EP: Any](
        cls: type[Self],
        /,
        node: InvokedNode[SP, BP, EP],
        *,
        storage: IStorage[SP],
        broker: IBroker[BP],
        executor: IExecutor[EP] | type[IExecutor[EP]],
        is_first_node: Literal[False] = False,
        context: Context,
        get_node_trace_message: Callable[[InvokedNode[SP, BP, EP]], str] | None = None,
        get_graph_trace_message: Callable[[InvokedNode[SP, BP, EP]], str] | None = None,
    ) -> UUID:
        raise NotImplementedError

    @overload
    @classmethod
    async def invoke_node[SP: Any, BP: Any, EP: Any](
        cls: type[Self],
        /,
        node: InvokedNode[SP, BP, EP],
        *,
        storage: IStorage[SP],
        broker: IBroker[BP],
        executor: IExecutor[EP] | type[IExecutor[EP]],
        is_first_node: Literal[True] = True,
        context: Context | None = None,
        get_node_trace_message: Callable[[InvokedNode[SP, BP, EP]], str] | None = None,
        get_graph_trace_message: Callable[[InvokedNode[SP, BP, EP]], str] | None = None,
    ) -> UUID:
        raise NotImplementedError

    # TODO: Implement via recursion.
    @classmethod
    @__TRACER.with_span_async(Tracer.DEBUG, "interactor.invoke_node")
    async def invoke_node[SP: Any, BP: Any, EP: Any](  # noqa: PLR0913
        cls: type[Self],
        /,
        node: InvokedNode[SP, BP, EP],
        *,
        storage: IStorage[SP],
        broker: IBroker[BP],
        executor: IExecutor[EP] | type[IExecutor[EP]],
        is_first_node: bool = True,
        context: Context | None = None,
        get_node_trace_message: Callable[[InvokedNode[SP, BP, EP]], str] | None = None,
        get_graph_trace_message: Callable[[InvokedNode[SP, BP, EP]], str] | None = None,
    ) -> UUID:
        is_created: EventWithValue[UUID] | None = context.created_nodes.get(node.id) if context is not None else None
        if is_created is not None:
            return await is_created.wait()

        is_created = EventWithValue()
        if context is not None:
            context.created_nodes[node.id] = is_created

        exit_stack: ExitStack
        with ExitStack() as exit_stack:
            # This is needed to propagate exceptions back to dependent nodes.
            exit_stack.enter_context(is_created)

            if is_first_node:
                # TODO: Support proper user mapper to add attributes to the graph.
                exit_stack.enter_context(
                    cls.__TRACER.span(
                        Tracer.INFO,
                        "graph.process",
                        message_=get_graph_trace_message(node) if get_graph_trace_message is not None else None,
                        id=node.id,
                    )
                )

            elif context is not None:
                # Restore graph context, in case we are handling not very first node.
                exit_stack.enter_context(context.graph_trace_context.attach(cls.__TRACER))

            else:
                raise RuntimeError

            # Ether propagated or newly created.
            graph_trace_context: Final[bytes] = TraceContext.get_current().to_bytes()

            # TODO: Support proper user mapper to add attributes to the node.
            exit_stack.enter_context(
                cls.__TRACER.span(
                    Tracer.INFO,
                    "node.process",
                    message_=get_node_trace_message(node) if get_node_trace_message is not None else None,
                    id=node.id,
                )
            )
            node_trace_context: Final[bytes] = TraceContext.get_current().to_bytes()

            if context is None:
                context = Context(
                    node_id=node.id,
                    graph_id=node.id,
                    session_id=SingleUseLockWithValue(uuid4()),
                    graph_trace_context=TraceContext.get_current(),
                    created_nodes={node.id: is_created},
                )

            await gather(
                cls.invoke_node(
                    dependency,
                    context=context,
                    storage=storage,
                    broker=broker,
                    executor=executor,
                    get_node_trace_message=get_node_trace_message,
                    get_graph_trace_message=get_graph_trace_message,
                )
                if dependency.id not in context.created_nodes
                else context.created_nodes[dependency.id].wait()
                for dependency in node.dependencies
            )

            is_session_created: bool
            async with context.session_id as is_session_created:
                # Three cases:
                #  1. Just a node, when we are processing node for existing graph.
                #  2. Node and graph, when we are processing new graph for existing session.
                #  3. Node, graph and session, when we start without any context.
                create_node_args: Final[list[Any]] = []

                if not is_session_created:
                    create_node_args.append(CreatedSession(id=context.session_id.value))

                if is_first_node:
                    create_node_args.append(
                        CreatedGraph(id=node.id, session_id=context.session_id.value, trace_context=graph_trace_context)
                    )

                create_node_args.append(
                    CreatedNode(
                        id=node.id,
                        parent_id=None if is_first_node else context.node_id,
                        graph_id=node.id if is_first_node else context.graph_id,
                        arguments=node.arguments,
                        dependencies={dependency.id: is_data for dependency, is_data in node.dependencies.items()},
                        trace_context=node_trace_context,
                        broker_params=broker.get_bytes_from_params(node.broker_params),
                        executor_params=executor.get_bytes_from_params(node.executor_params),
                    )
                )

                is_ready: Final[bool] = await storage.create_node(node.storage_params, *reversed(create_node_args))

            is_created.set(context.session_id.value)
            if is_ready:
                await broker.publish_node_enqueued(
                    node.broker_params,
                    node=EnqueuedNode(
                        id=node.id, session_id=context.session_id.value, trace_context=node_trace_context
                    ),
                )

        return context.session_id.value

    @classmethod
    @__TRACER.with_span_async(Tracer.DEBUG, "interactor.complete_node")
    async def complete_node(cls: type[Self], /, node: CompletedNode, *, storage: IStorage, broker: IBroker) -> None:
        async def publish_node_enqueued(node: ReadyNode) -> None:
            await broker.publish_node_enqueued(
                params=broker.get_params_from_bytes(node.broker_params), node=cast("EnqueuedNode", node)
            )

        ready_nodes: Final[list[ReadyNode]] = await storage.complete_node(node)
        await gather(map(publish_node_enqueued, ready_nodes))
        # TODO: Support user mapper to add attributes to the node.
        cls.__TRACER.info("node.completed", id=node.id)

    @classmethod
    @__TRACER.with_span_async(Tracer.DEBUG, "interactor.cancel_session")
    async def cancel_session(cls: type[Self], id_: UUID, *, storage: IStorage, broker: IBroker) -> None:
        await gather(storage.cancel_session(id_), broker.publish_session_cancelled(id_))
        cls.__TRACER.info("session.canceled", id=id_)

    @classmethod
    async def on_node_enqueued(  # noqa: PLR0913
        cls: type[Self],
        /,
        node: EnqueuedNode,
        *,
        storage: IStorage,
        broker: IBroker,
        executor: IExecutor,
        get_node_trace_message: Callable[[InvokedNode], str] | None = None,
        get_graph_trace_message: Callable[[InvokedNode], str] | None = None,
    ) -> None:
        session_cancelled_event: Final[EventWithSubscribers] = cls.__SESSIONS.get(
            node.session_id, EventWithSubscribers()
        )
        cls.__SESSIONS[node.session_id] = session_cancelled_event

        try:
            with TraceContext.from_bytes(node.trace_context).attach(cls.__TRACER), session_cancelled_event:
                task_group: TaskGroup
                async with TaskGroup() as task_group:
                    session_cancelled_event_task: Final[Task] = task_group.create_task(session_cancelled_event.wait())
                    on_node_enqueued_task: Final[Task] = task_group.create_task(
                        cls.__execute_node(
                            node,
                            storage=storage,
                            broker=broker,
                            executor=executor,
                            get_node_trace_message=get_node_trace_message,
                            get_graph_trace_message=get_graph_trace_message,
                        )
                    )

                    await asyncio.wait(
                        fs=(session_cancelled_event_task, on_node_enqueued_task), return_when=asyncio.FIRST_COMPLETED
                    )

                    if session_cancelled_event.is_set:
                        on_node_enqueued_task.cancel(msg="Session cancelled.")
                        cls.__TRACER.info("session.cancelled")
                        return

                    session_cancelled_event_task.cancel()

                    try:
                        await on_node_enqueued_task

                    except Exception as exception:
                        cls.__TRACER.error("node.failed", exception_=exception)
                        await cls.cancel_session(node.session_id, storage=storage, broker=broker)

        finally:
            if session_cancelled_event.subscriber_count == 0:
                cls.__SESSIONS.pop(node.session_id)

    @classmethod
    async def on_session_cancelled(cls: type[Self], /, id_: UUID) -> None:
        if id_ in cls.__SESSIONS:
            cls.__SESSIONS[id_].set()

    @classmethod
    async def __execute_node[SP: Any, BP: Any, EP: Any](  # noqa: PLR0913
        cls: type[Self],
        /,
        enqueued_node: EnqueuedNode,
        *,
        storage: IStorage[SP],
        broker: IBroker[BP],
        executor: IExecutor[EP],
        get_node_trace_message: Callable[[InvokedNode[SP, BP, EP]], str] | None = None,
        get_graph_trace_message: Callable[[InvokedNode[SP, BP, EP]], str] | None = None,
    ) -> None:
        started_node: Final[StartedNode] = await storage.start_node(enqueued_node.id)
        cls.__TRACER.set_attributes_to_current_span(Tracer.INFO, node=started_node)

        context: Final[Context] = Context(
            node_id=started_node.id,
            graph_id=started_node.graph_id,
            # Session already created, since current node is already executing.
            session_id=SingleUseLockWithValue(enqueued_node.session_id, is_used=True),
            graph_trace_context=TraceContext.from_bytes(started_node.graph_trace_context),
            created_nodes={},
        )

        async def invoke_node(node: InvokedNode[SP, BP, EP], is_new_session: bool) -> UUID:  # noqa: FBT001
            return await cls.invoke_node(
                node,
                context=None if is_new_session else context,
                storage=storage,
                broker=broker,
                executor=executor,
                get_node_trace_message=get_node_trace_message,
                get_graph_trace_message=get_graph_trace_message,
            )

        result: Final[InvokedNode | CompletedNode | None] = await executor.execute_node(
            params=executor.get_params_from_bytes(started_node.executor_params),
            node=RunningNode(
                id=started_node.id,
                graph_id=started_node.graph_id,
                session_id=enqueued_node.session_id,
                arguments=started_node.arguments,
                dependencies=started_node.dependencies,
            ),
            invoke_node_callback=invoke_node,
        )

        if result is None:
            cls.__TRACER.info("node.paused")
            return

        if isinstance(result, CompletedNode):
            await cls.complete_node(result, storage=storage, broker=broker)
            return

        await cls.invoke_node(
            result,
            is_first_node=False,
            context=context,
            storage=storage,
            broker=broker,
            executor=executor,
            get_node_trace_message=get_node_trace_message,
            get_graph_trace_message=get_graph_trace_message,
        )
