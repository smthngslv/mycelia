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
    print("Hello, 1!")  # noqa: T201
    return my_node_2(*(get_random_integer(minimum=0, maximum=10) for _ in range(10)))


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    await asyncio.sleep(delay=5.0)
    return random.randint(minimum, maximum)


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, *values: int) -> None:
    print(f"Sum is {sum(values)}.")


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node_1)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
