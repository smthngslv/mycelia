import asyncio
from typing import Final

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node_1(ctx: Context, /) -> None:
    print("Hello, 1!")

    i: int
    for i in range(5):
        await ctx.submit(print_value(i))

    print("1 is finished.")

@mycelia.node(my_graph)
async def print_value(_: Context, /, value: int) -> None:
    await asyncio.sleep(delay=1.0)
    print(f"Value is {value}.")


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node_1)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
