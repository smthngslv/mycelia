import asyncio
from typing import Final

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> int:
    print("Hello, 1!")
    return echo_node(echo_node(echo_node(echo_node(echo_node(echo_node(1))))))


@mycelia.node(my_graph)
async def echo_node(_: Context, value: int) -> int:
    print(f"Hello, from echo_node: {value}!")
    return value


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node_1)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
