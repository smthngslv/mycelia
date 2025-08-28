import contextlib
from collections.abc import AsyncIterator
from typing import Any, Final
from uuid import UUID

from mycelia.domains.graphs.interactors import GraphsInteractor
from mycelia.presenters.main import Context, Graph, Node, node, pause
from mycelia.services.broker.local import LocalBroker
from mycelia.services.executor.local import LocalExecutor
from mycelia.services.storage.local import LocalStorage
from mycelia.tracing import TraceContext

__all__: Final[tuple[str, ...]] = (
    "Context",
    "Graph",
    "Node",
    "cancel",
    "execute",
    "node",
    "pause",
    "resume",
    "resume",
    "session",
)


_SESSION: Final[dict[str, Any]] = {"broker": None, "storage": None, "executor": None}


@contextlib.asynccontextmanager
async def session(*graphs: Graph) -> AsyncIterator[None]:
    if _SESSION["broker"] is not None or _SESSION["storage"] is not None:
        message: Final[str] = "You can start only one session."
        raise RuntimeError(message)

    storage: Final[LocalStorage] = LocalStorage()
    executor: Final[LocalExecutor] = LocalExecutor(
        handlers={node.id: node.function for graph in graphs for node in graph.nodes}
    )

    async def _on_node_enqueued_callback(node_id: UUID, tract_context: TraceContext) -> None:
        await GraphsInteractor.on_node_enqueued(
            node_id,
            tract_context,
            broker=_SESSION["broker"],
            storage=_SESSION["storage"],
            executor=_SESSION["executor"],
        )

    broker: LocalBroker
    async with LocalBroker(_on_node_enqueued_callback, GraphsInteractor.on_graph_cancelled) as broker:
        _SESSION["broker"] = broker
        _SESSION["storage"] = storage
        _SESSION["executor"] = executor
        yield


async def execute[**P, R](node: Node[P, R], *args: P.args, **kwargs: P.kwargs) -> UUID:
    if _SESSION["broker"] is None or _SESSION["storage"] is None:
        message: Final[str] = "Start session first."
        raise RuntimeError(message)

    return await GraphsInteractor.orchestrate_call(
        node(*args, **kwargs),  # type: ignore[arg-type]
        broker=_SESSION["broker"],
        storage=_SESSION["storage"],
    )


async def cancel(graph_id: UUID) -> None:
    await GraphsInteractor.cancel_graph(graph_id, broker=_SESSION["broker"], storage=_SESSION["storage"])


async def resume(graph_id: UUID, result: Any = None) -> None:
    await GraphsInteractor.submit_graph_result(graph_id, result, broker=_SESSION["broker"], storage=_SESSION["storage"])
