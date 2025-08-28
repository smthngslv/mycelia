import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Final
from uuid import UUID

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()

_STATE: Final[dict[str, Any]] = {}

@mycelia.node(my_graph)
async def my_node_1(ctx: Context, /) -> None:
    print("Hello, 1!")
    user_input: Final[int] = mycelia.pause(int)
    _STATE["graph_id"] = await ctx.submit(user_input)
    return my_node_2(get_random_integer(minimum=0, maximum=10), user_input)


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    return random.randint(minimum, maximum)


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, value: int, user_input: str) -> None:
    print(f"Value is {value}.")
    print(f"User input is {user_input}.")


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node_1)

        pool: ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=1) as pool:
            user_input: Final[str] = await asyncio.get_event_loop().run_in_executor(pool, input, ">>> ")

        await mycelia.resume(_STATE["graph_id"], user_input)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
