import contextlib
import functools
from collections.abc import AsyncIterator
from typing import Any, Final

from mycelia.domains.nodes.interactors import NodesInteractor
from mycelia.presenters.main import Context, Graph, Node, node
from mycelia.services.broker.local import LocalBroker
from mycelia.services.executor.local import LocalExecutor
from mycelia.services.storage.local import LocalStorage

__all__: Final[tuple[str, ...]] = ("Context", "Graph", "Node", "execute", "node", "session")

_SESSION: Final[dict[str, Any]] = {"broker": None, "storage": None}


@contextlib.asynccontextmanager
async def session(*graphs: Graph) -> AsyncIterator[None]:
    if _SESSION["broker"] is not None or _SESSION["storage"] is not None:
        message: Final[str] = "You can start only one session."
        raise RuntimeError(message)

    storage: Final[LocalStorage] = LocalStorage()
    executor: Final[LocalExecutor] = LocalExecutor(
        handlers={node.id: node.function for graph in graphs for node in graph.nodes}
    )

    broker: LocalBroker
    async with LocalBroker() as broker:
        broker.set_on_node_enqueued_callback(
            function=functools.partial(
                NodesInteractor.on_node_enqueued, broker=broker, storage=storage, executor=executor
            )
        )

        broker.set_on_node_completed_callback(
            function=functools.partial(NodesInteractor.on_node_completed, broker=broker, storage=storage)
        )

        _SESSION["broker"] = broker
        _SESSION["storage"] = storage
        yield


async def execute[**P, R](node: Node[P, R], *args: P.args, **kwargs: P.kwargs) -> None:
    if _SESSION["broker"] is None or _SESSION["storage"] is None:
        message: Final[str] = "Start session first."
        raise RuntimeError(message)

    await NodesInteractor._orchestrate_node_call(  # noqa: SLF001
        node(*args, **kwargs),  # type: ignore[arg-type]
        set(),
        broker=_SESSION["broker"],
        storage=_SESSION["storage"],
    )
