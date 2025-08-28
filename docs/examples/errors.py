import asyncio
from typing import Final

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node(_: Context, /) -> None:
    message: Final[str] = "Hello, world!"
    raise Exception(message)


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
