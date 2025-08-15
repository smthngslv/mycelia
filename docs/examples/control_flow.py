import asyncio
from typing import Final

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> None:
    print("Hello, 1!")
    return my_node_2()


@mycelia.node(my_graph)
async def my_node_2(_: Context, /) -> None:
    print("Hello, 2!")


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node_1)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
