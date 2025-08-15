import asyncio
from typing import Any, Final

import logfire

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node(
    _: Context,
    # Position only argument.
    arg_1: int,
    # Position only argument with default value.
    arg_2: int = 1,
    /,
    # Usual argument, default value is optional.
    arg_3: int = 2,
    # Variable position arguments.
    *args: int,
    # Keyword argument, default value is optional.
    kwarg: int | None = None,
    # Variable keywords arguments.
    **kwargs: Any,
) -> None:
    print(f"ARGS: {arg_1=}, {arg_2=}, {arg_3=}, {args=}, {kwarg=}, {kwargs=}")  # noqa: T201


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node, 1, 2, 3, 4, 5, 6, kwarg=7, a=8)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
