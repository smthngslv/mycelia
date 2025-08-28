import asyncio
from typing import Final
from uuid import UUID

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node(_: Context, /) -> None:
    await asyncio.sleep(delay=100.0)


async def main() -> None:
    async with mycelia.session(my_graph):
        graph_id: Final[UUID] = await mycelia.execute(my_node)
        await asyncio.sleep(delay=1.0)
        await mycelia.cancel(graph_id)



if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
