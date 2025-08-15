import functools
from collections.abc import Mapping, MutableSet
from typing import Any, Final, Self, final
from uuid import UUID

import logfire_api as logfire

from mycelia.domains.nodes.entities import Node, NodeCall, NodeCompletedEvent, NodeEnqueuedEvent
from mycelia.services.broker.interface import IBroker
from mycelia.services.executor.interface import IExecutor
from mycelia.services.storage.interface import IStorage

__all__: Final[tuple[str, ...]] = ("NodesInteractor",)


@final
class NodesInteractor:
    @classmethod
    async def on_node_enqueued(
        cls: type[Self], event: NodeEnqueuedEvent, *, broker: IBroker, storage: IStorage, executor: IExecutor
    ) -> None:
        with logfire.propagate.attach_context(event.logfire_ctx), logfire.span(f"Node {event.node_id}.") as span:
            node: Final[Node] = await storage.get_node(event.node_id, event.storage_options)
            span.message = f"Invoked `{node.handler_id}` with {node.arguments}"
            span.set_attributes(attributes={"node": node})
            orchestrated_node_call_ids: Final[set[UUID]] = set()

            logfire_ctx = logfire.propagate.get_context()
            # TODO: Cancel entire graph in case of exception.
            with logfire.span("Execution."):
                result: Final[Any] = await executor.execute_node(
                    node.handler_id,
                    node.arguments,
                    functools.partial(
                        cls._orchestrate_node_call,
                        orchestrated_node_call_ids=orchestrated_node_call_ids,
                        broker=broker,
                        storage=storage,
                        logfire_ctx=logfire_ctx,
                    ),
                    node.executor_options,
                )

            if isinstance(result, NodeCall):
                logfire.trace(f"Node finished with another call: {result}.")
                await storage.set_parent(result.id, node.id)
                # TODO: Handle case when node is first created and then returned. We cannot change parent, but we
                #  can add link. NOT POSSIBLE.
                await cls._orchestrate_node_call(
                    result, orchestrated_node_call_ids, broker=broker, storage=storage, logfire_ctx=event.logfire_ctx
                )
                return

            result_id: Final[UUID] = await storage.save_result(node.id, result, node.storage_options)
            logfire.trace(f"Node finished with result: `{result}`, saved as {result_id}.")
            await broker.publish(
                event=NodeCompletedEvent(node_id=node.id, storage_options=node.storage_options, result_id=result_id),
                options=node.broker_options,
            )

    @classmethod
    async def on_node_completed(
        cls: type[Self], event: NodeCompletedEvent, *, broker: IBroker, storage: IStorage
    ) -> None:
        for node_id in await storage.get_callbacks(event.node_id):
            if await storage.set_dependency_result(node_id, event.node_id, event.result_id):
                await broker.publish(
                    event=NodeEnqueuedEvent(
                        node_id=node_id,
                        # TODO: Fetch storage options.
                        logfire_ctx=await storage.get_logfire_ctx(node_id, None),
                        storage_options=None,
                    ),
                    # TODO: Fetch storage options.
                    options=None,
                )

        parent_id: Final[UUID | None] = await storage.get_parent(event.node_id, event.storage_options)
        if parent_id is not None:
            await cls.on_node_completed(
                event=NodeCompletedEvent(node_id=parent_id, storage_options=None, result_id=event.result_id),
                broker=broker,
                storage=storage,
            )

    # TODO: Rewrite it without recursion, and it can be done with only 2 requests to storage.
    @classmethod
    async def _orchestrate_node_call(  # noqa: C901
        cls: type[Self],
        node_call: NodeCall,
        orchestrated_node_call_ids: MutableSet[UUID],
        *,
        broker: IBroker,
        storage: IStorage,
        logfire_ctx: Mapping[str, Any] | None = None,
    ) -> None:
        if node_call.id in orchestrated_node_call_ids:
            return

        if logfire_ctx is None:
            with logfire.span(f"Graph {node_call.handler_id} ({node_call.id})"):
                logfire_ctx = logfire.propagate.get_context()

        arguments: Final[dict[int | str, Any]] = {}
        dependencies: Final[dict[int | str, UUID]] = {}
        key: int | str
        argument: Any
        for key, argument in node_call.arguments.items():
            if isinstance(argument, NodeCall):
                dependencies[key] = argument.id

            else:
                arguments[key] = argument

        orchestrated_node_call_ids.add(node_call.id)
        await storage.create_node(
            node_call.id,
            node_call.handler_id,
            arguments,
            dependencies,
            node_call.broker_options,
            node_call.executor_options,
            node_call.storage_options,
            logfire_ctx,
        )

        is_dependencies_resolved: bool = True
        for argument in node_call.arguments.values():
            if not isinstance(argument, NodeCall):
                continue

            if argument.id not in orchestrated_node_call_ids:
                # Make sure, node is orchestrated.
                with (
                    logfire.propagate.attach_context(logfire_ctx),
                    logfire.span(f"Subgraph {argument.handler_id} ({argument.id})"),
                ):
                    child_logfire_ctx = logfire.propagate.get_context()

                await cls._orchestrate_node_call(
                    argument, orchestrated_node_call_ids, broker=broker, storage=storage, logfire_ctx=child_logfire_ctx
                )

            result_id: UUID | None = await storage.set_callback_or_get_result_id(
                argument.id, node_call.id, argument.storage_options
            )

            # Dependency node is not finished yet, but callback set.
            if result_id is None:
                is_dependencies_resolved = False
                continue

            # The method returns `True` when all dependencies resolved.
            if await storage.set_dependency_result(node_call.id, argument.id, result_id):
                is_dependencies_resolved = True
                break

        if is_dependencies_resolved:
            await broker.publish(
                event=NodeEnqueuedEvent(
                    node_id=node_call.id, logfire_ctx=logfire_ctx, storage_options=node_call.storage_options
                ),
                options=node_call.broker_options,
            )
