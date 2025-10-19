import asyncio
import importlib.metadata
import random
import sys
from collections.abc import Sequence
from typing import Final

import logfire

import mycelia
from mycelia import Client, Graph, RunContext
from mycelia.interface.executor import ExecutorParams
from mycelia.interface.instance import Server
from mycelia.services.broker.rabbitmq import RabbitMQBroker, RabbitMQBrokerParams
from mycelia.services.storage.postgres import PostgresStorage
from mycelia.services.storage.postgres.instance import PostgresStorageParams

__all__: Final[tuple[str, ...]] = ()

graph: Final[Graph[PostgresStorageParams, RabbitMQBrokerParams, ExecutorParams]] = Graph()


@mycelia.node(
    graph,
    storage_params=PostgresStorageParams(),
    broker_params=RabbitMQBrokerParams(queue_name="get_random_number"),
    executor_params=ExecutorParams(node_id="get_random_number"),
)
async def get_random_number(_: RunContext, /, minimum: int, maximum: int, label: str) -> int:
    number: Final[int] = random.randint(minimum, maximum)  # noqa: S311
    print(f"Hello from `get_random_number#{label}`, number is {number}!")  # noqa: T201
    return number


@mycelia.node(
    graph,
    storage_params=PostgresStorageParams(),
    broker_params=RabbitMQBrokerParams(queue_name="print_numbers"),
    executor_params=ExecutorParams(node_id="print_numbers"),
)
async def print_numbers(_: RunContext, /, numbers: Sequence[int]) -> None:
    print(f"Hello from `print_numbers`, numbers are {numbers}!")  # noqa: T201


async def server() -> None:
    storage: Final[PostgresStorage] = PostgresStorage(url="postgresql+asyncpg://mycelia:mycelia@localhost/mycelia")
    broker: Final[RabbitMQBroker] = await RabbitMQBroker.create(url="amqp://mycelia:mycelia@localhost")
    mycelia_server: Final[Server] = await Server.create(storage, broker)
    await mycelia_server.serve_graph(graph)

    try:
        await asyncio.Future()

    finally:
        await storage.shutdown()
        await broker.shutdown()


async def client() -> None:
    storage: Final[PostgresStorage] = PostgresStorage(url="postgresql+asyncpg://mycelia:mycelia@localhost/mycelia")
    broker: Final[RabbitMQBroker] = await RabbitMQBroker.create(url="amqp://mycelia:mycelia@localhost")
    mycelia_client: Final[Client] = Client(storage, broker)

    try:
        await mycelia_client.start_session(
            # This node will be executed in first group, with B, C, G, H, I.
            get_random_number(1, 2, "A")
            .then(
                # This node will be executed in second group.
                get_random_number(1, 2, "D"),
                get_random_number(1, 2, "E"),
                get_random_number(1, 2, "F"),
            )
            .then(
                # This node will be executed in third group.
                get_random_number(
                    # These nodes will be executed in first group, with A, G, H, I.
                    get_random_number(1, 2, "B").value,
                    get_random_number(1, 2, "C").value,
                    label="Meow",
                )
            )
            .then(
                # This node will be executed in forth group.
                print_numbers(
                    numbers=mycelia.group(
                        # These nodes will be executed in first group, with A, B, C.
                        get_random_number(1, 2, "G"),
                        get_random_number(1, 2, "H"),
                        get_random_number(1, 2, "I"),
                    ).values
                )
            )
        )

    finally:
        await storage.shutdown()
        await broker.shutdown()


if __name__ == "__main__":
    logfire.configure(
        console=False,
        service_name="mycelia.example",
        service_version=importlib.metadata.version("mycelia"),
        distributed_tracing=True,
        min_level="trace",
        scrubbing=False,
    )
    asyncio.run(server() if sys.argv[1] == "server" else client())
