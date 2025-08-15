import asyncio
import random
from typing import Final

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> None:
    print("Hello, 1!")
    return my_node_2(left=get_random_integer(minimum=0, maximum=10), right=get_random_integer(minimum=0, maximum=10))


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    return random.randint(minimum, maximum)


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, left: int, right: int) -> None:
    print(f"Sum is {left + right}.")


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node_1)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
