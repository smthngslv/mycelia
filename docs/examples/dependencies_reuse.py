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
    # We save delayed execution of a node to a variable.
    value: Final[int] = get_random_integer(minimum=0, maximum=10)
    # However, since we actually called `get_random_integer` only once - all nodes here are actually the same.
    return my_node_2(*(value for _ in range(10)))


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    await asyncio.sleep(delay=5.0)
    value: Final[int] = random.randint(minimum, maximum)  # noqa: S311
    print(f"Got {value} as random integer.")
    return value


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, *values: int) -> None:
    print(f"Values are {values}.")


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node_1)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
