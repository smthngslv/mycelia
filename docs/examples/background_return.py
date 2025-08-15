import asyncio
import random
from typing import Final

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node_1(ctx: Context, /) -> None:
    print("Hello, 1!")
    value: Final[int] = get_random_integer(minimum=0, maximum=10)
    await ctx.submit(value)
    # Some heavy stuff.
    await asyncio.sleep(delay=5.0)
    # Use as dependency.
    return my_node_2(value)


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    await asyncio.sleep(delay=3.0)
    return random.randint(minimum, maximum)


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, value: int) -> None:
    print(f"Value is {value}.")


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node_1)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
