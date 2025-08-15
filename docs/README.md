# Installation

Right now you can install `mycelia` with `.whl` and `uv`, just do following:

```shell
uv add "path/to/mycelia-0.1.0-py3-none-any.whl[logfire]"
```

After that you need to create account in [Logfire](https://logfire.pydantic.dev/login) and run

```shell
logfire auth
```

It will open browser window and you can authenticate there. Then on very first run with you will be asked to
select/create project.

# Concepts

Mycelia is a task based interservice (interagent) communication system. It solves problem of creating and executing
multi steps, interconnected and very dependency heavy workflows along with data management in these workflows and
interservice (interagent) communication.

# Examples

## Hello, World!

Let's start with `Hello, word!` example. Each workflow in mycelia is a graph that consists of nodes (tasks). Let us
define a node:

```python
import mycelia
from mycelia import Context

__all__: Final[tuple[str, ...]] = ("main",)


@mycelia.node
async def my_node(_: Context, /) -> None:
    print("Hello, world!")
```

Nice, all you need is to take any async function and decorate it with `mycelia.node` decorator. First argument to the
function is always a context, but since we do not need it right now, just name arg as `_`. Now, we want to run it, to
do so we need to define a graph, and assign node `my_node` to that graph. It's simple:

```python
import asyncio
from typing import Final

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node(_: Context, /) -> None:
    print("Hello, world!")
```

And now we can run with a few lines of code:

```python
import asyncio
from typing import Final

import mycelia
from mycelia import Context, Graph

__all__: Final[tuple[str, ...]] = ("main",)

my_graph: Final[Graph] = Graph()


@mycelia.node(my_graph)
async def my_node(_: Context, /) -> None:
    print("Hello, world!")  # noqa: T201


async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node)


if __name__ == "__main__":
    logfire.configure()
    asyncio.run(main())
```

Notice, that we called `logfire.configure()` before execution to initialize `Logfire`, to see traces in the UI.

To run it, just type

```shell
python docs/examples/hello_world.py
```

You will see:

```
python docs/examples/hello_world.py
16:49:28.853 Graph my_node (6db61517-940d-44c6-9d97-63ee6d8d562e)
16:49:28.854 Node 6db61517-940d-44c6-9d97-63ee6d8d562e.
16:49:28.855   Execution.
Hello, world!
Logfire project URL: https://logfire-us.pydantic.dev/some-user/some-project
```

In `Logfire` UI you can see

![](images/logfire_1.png)

To stop session, just press `CTRL+C`.

## Control Flow

Now lets deal with execution flow. In `mycelia` you can pass control flow to next node but simply return its call:

```python
@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> None:
    print("Hello, 1!")  # noqa: T201
    return my_node_2()


@mycelia.node(my_graph)
async def my_node_2(_: Context, /) -> None:
    print("Hello, 2!")  # noqa: T201
```

Here we defined two nodes and then first node passed control flow to the second one. In this way we just created linear
graph `A -> B`! You can run this example and see `Logfire` traces:

```
python docs/examples/control_flow.py
17:00:54.831 Graph my_node_1 (26dcaebe-3acf-4d83-9dd2-6147fcde7806)
17:00:54.833 Node 26dcaebe-3acf-4d83-9dd2-6147fcde7806.
17:00:54.833   Execution.
Hello, 1!
17:00:54.834 Node 74a469f0-9ee4-4911-b317-8c4974e052e3.
17:00:54.834   Execution.
Hello, 2!
```

And in UI you will see two nodes on the same level.

![](images/logfire_2.png)

## Arguments

`Mycelia` does not limit you on arguments, you can pass whatever you want as long as it serializable by configured
storage (for current storage it's any python object, however it's better to stick with JSON serializable objects):

```python
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
```

To invoke this, simple pass arguments to `mycelia.execute`:

```python
async def main() -> None:
    async with mycelia.session(my_graph):
        await mycelia.execute(my_node, 1, 2, 3, 4, 5, 6, kwarg=7, a=8)
```

You will see:

```
python docs/examples/arguments.py
17:19:46.906 Graph my_node (62cae96a-cdfc-4aed-a515-81a24e86b26c)
17:19:46.907 Node 62cae96a-cdfc-4aed-a515-81a24e86b26c.
17:19:46.908   Execution.
ARGS: arg_1=1, arg_2=2, arg_3=3, args=(4, 5, 6), kwarg=7, kwargs={'a': 8}
```

And arguments also will be displayed in `Logfire`:

![](images/logfire_3.png)

In `Logfire` positional arguments has index as key and keyword arguments a string. Similar to this you can pass
arguments not just to the entrypoint node, but for any node:

```python
@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> None:
    print("Hello, 1!")
    return my_node_2(1, "2")
    
@mycelia.node(my_graph)
async def my_node_2(_: Context, a: int, b: str) -> None:
    print(a, b)
```

## Dependencies (Subgraphes)

And now, when we know how to pass arguments to the nodes, it's time to get results from them. Let's say, that we need to
pass a random number to the second node, and, for example, a random number must be generated in separate node. To do so,
you have to define nodes like following:

```python
@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> None:
    print("Hello, 1!")
    return my_node_2(get_random_integer(minimum=0, maximum=10))


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    return random.randint(minimum, maximum)


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, value: int) -> None:
    print(f"Value is {value}.")
```

Here first (`my_node_1`) node passes control flow to the second one (`my_node_2`), however before that we need to
execute `get_random_integer` since it's a __dependency__ of the second node. Let's run this example:

```
python docs/examples/dependencies.py
17:36:29.694 Graph my_node_1 (107ae6c5-4c32-4cdb-83de-78167cbe23a5)
17:36:29.696 Node 107ae6c5-4c32-4cdb-83de-78167cbe23a5.
17:36:29.696   Execution.
Hello, 1!
17:36:29.697 Subgraph get_random_integer (70f5bcb1-1424-4b1a-810f-f80b6e3fc72c)
17:36:29.697 Node 70f5bcb1-1424-4b1a-810f-f80b6e3fc72c.
17:36:29.698   Execution.
17:36:29.698 Node da6b0c63-c953-4019-abc4-259938ace774.
17:36:29.699   Execution.
Value is 6.
```

And it's better to see traces in `Logfire`:

![](images/logfire_4.png)

As you can see, to be able to invoke `my_node_2` system firstly invoke a subgraph with single node `get_random_integer`,
and only then resume the execution of `my_node_2`. Node can have any number of dependencies, and these dependencies
will be resolved as parallel as possible (meaning if two dependency are independent, they will be resolved in parallel).

Let's modify our example and make `my_node_2` sum two arguments:

```python
@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> None:
    print("Hello, 1!")
    return my_node_2(left=get_random_integer(minimum=0, maximum=10), right=get_random_integer(minimum=0, maximum=10))

...

@mycelia.node(my_graph)
async def my_node_2(_: Context, /, left: int, right: int) -> None:
    print(f"Sum is {left + right}.")
```

And execution trace:

![](images/logfire_5.png)

Moreover, since you can make `my_node_2` accept not just two arguments but `*args`, you can make universal function,
and all dependencies still will be resolved in parallel (let's also add some sleep in `get_random_integer` and see that
it does not affect execution).

```python
@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> None:
    print("Hello, 1!")
    return my_node_2(*(get_random_integer(minimum=0, maximum=10) for _ in range(10)))


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    await asyncio.sleep(delay=5.0)
    return random.randint(minimum, maximum)


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, *values: int) -> None:
    print(f"Sum is {sum(values)}.")
```

And trace:

![](images/logfire_6.png)

As you can see, it only took 5 seconds, because we were running 10 nodes in parallel, not sequentially. Also, there
is no limit on nesting, so you can do following things. However, notice that we have to change return value of
`my_node_1` to match it with `echo_node` even if we do not use it later.

```python
@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> int:
    print("Hello, 1!")  # noqa: T201
    return echo_node(echo_node(echo_node(echo_node(echo_node(echo_node(1))))))


@mycelia.node(my_graph)
async def echo_node(_: Context, value: int) -> int:
    print(f"Hello, from echo_node: {value}!")  # noqa: T201
    return value

```

Trace in this case will be also nested, except for very first `echo_node` node:

![](images/logfire_7.png)

And the last example will show you, that after some node was invoked, it actually can be used multiple times (its
result), however it will be exact same node:

```python
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
    value: Final[int] = random.randint(minimum, maximum)
    print(f"Got {value} as random integer.")
    return value


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, *values: int) -> None:
    print(f"Values are {values}.")
```

Trace will contain only single call to `get_random_integer`:

![](images/logfire_8.png)

Due to limitation of python typing, I have to leave same type of return values when node is called, so make sure you do
not try to do something with it:

```python
@mycelia.node(my_graph)
async def my_node_1(_: Context, /) -> None:
    # This is not actually an integer, but some object.
    value: Final[int] = get_random_integer(minimum=0, maximum=10)
    # So this is invalid will throw exception:
    value += 1
    # However you can return it or pass as argument to another node:
    
    # Works.
    return value
    # Also works.
    return get_random_integer(1, value)


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    return random.randint(minimum, maximum)
```

## Background Execution

Sometimes you need to just run some node in background and do not need to even return result from it (i.e. submit some
event during execution). Or, in case of very long workflows you may want to start some node execution in background and
then use it as dependency or return value. For this you are provided with `Context` object that is always passed as
first argument to any node.

```python
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
```

Here all `print_value` nodes will be executed in background and will not block:

```
python docs/examples/background.py
18:58:21.172 Graph my_node_1 (6d0cba7d-7142-4110-b736-69cdebcc3dca)
18:58:21.173 Node 6d0cba7d-7142-4110-b736-69cdebcc3dca.
18:58:21.173   Execution.
Hello, 1!
1 is finished.
18:58:21.174 Node 08b47749-534d-44db-b558-47f00eab8f3d.
18:58:21.174   Execution.
18:58:21.175 Node f4c30fc7-be5c-4135-94ef-4ae6f00831c6.
18:58:21.175   Execution.
18:58:21.175 Node 2359133f-ef5f-4f59-b611-923fc6b97758.
18:58:21.175   Execution.
18:58:21.175 Node de450af5-c07f-420d-9ae7-8750b74e4036.
18:58:21.175   Execution.
18:58:21.176 Node 3eedfd1e-ec88-4414-9d07-415db2cffd04.
18:58:21.176   Execution.
Logfire project URL: https://logfire-us.pydantic.dev/smthngslv/mycelia
Value is 0.
Value is 1.
Value is 2.
Value is 3.
Value is 4.
```

And the trace:

![](images/logfire_9.png)

And here example how you can speed up things, in case of complex workflows:

```python
@mycelia.node(my_graph)
async def my_node_1(ctx: Context, /) -> None:
    print("Hello, 1!")  # noqa: T201
    value: Final[int] = get_random_integer(minimum=0, maximum=10)
    await ctx.submit(value)
    # Some heavy stuff.
    await asyncio.sleep(delay=5.0)
    # Use as dependency.
    return my_node_2(value)


@mycelia.node(my_graph)
async def get_random_integer(_: Context, /, minimum: int, maximum: int) -> int:
    await asyncio.sleep(delay=3.0)
    return random.randint(minimum, maximum)  # noqa: S311


@mycelia.node(my_graph)
async def my_node_2(_: Context, /, value: int) -> None:
    print(f"Value is {value}.")  # noqa: T201
```

If we look on trace, we see that it took only 5 seconds, because `get_random_integer` was executed in background and
`return my_node_2(value)` just use result. In case node is not finished when used as argument, it still will work, in
this case it will be usual dependency.

![](images/logfire_10.png)

## Logfire

`Mycelia` uses [Logfire](https://pydantic.dev/logfire) to enable enhanced monitoring. And since `Logfire` already
support multiple integrations, you can experience full its power. See
integrations [here](https://logfire.pydantic.dev/docs/integrations/#documented-integrations) and manual tracing
[here](https://logfire.pydantic.dev/docs/guides/onboarding-checklist/add-manual-tracing/).

i.e. with just adding `logfire.instrument_openai()` line after `logfire.configure()` you already see all LLMs calls:

![img_1.png](images/logfire_11.png)

Or filter to see only LLM calls (also as tree structure).

![img.png](images/logfire_12.png)
