import asyncio
import functools
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from inspect import BoundArguments, Parameter
from typing import Any, ClassVar, Final, Self, cast, final
from uuid import UUID

from pydantic import Field, TypeAdapter

from mycelia import RunContext
from mycelia.core.entities import CompletedNode, InvokedNode, RunningNode
from mycelia.interface.common import PAUSE_MARKER, Node, NodeCall, NodeCalls
from mycelia.services.executor import IExecutor
from mycelia.tracing import TRACER, Tracer
from mycelia.utils import Codec, Entity

__all__: Final[tuple[str, ...]] = ("Executor", "ExecutorParams")


@final
@dataclass(frozen=True)
class DependencyReference:
    dependency_id: UUID

    @classmethod
    def from_bytes(cls: type[Self], /, packed: bytes) -> Self:
        return cls(dependency_id=UUID(bytes=packed))

    def to_bytes(self: Self, /) -> bytes:
        return self.dependency_id.bytes


@final
@dataclass(frozen=True)
class DependencyReferences:
    dependency_ids: tuple[UUID, ...]

    @classmethod
    def from_bytes(cls: type[Self], /, packed: bytes) -> Self:
        return cls(dependency_ids=tuple(UUID(bytes=packed[index : index + 16]) for index in range(0, len(packed), 16)))

    def to_bytes(self: Self, /) -> bytes:
        return b"".join(dependency_id.bytes for dependency_id in self.dependency_ids)


@final
class ExecutorParams(Entity):
    node_id: str
    # TODO: Annotated syntax is not supported by mypy yet.
    timeout: timedelta | None = Field(default=None)


@final
class Executor(IExecutor[ExecutorParams]):
    __slots__: ClassVar[tuple[str, ...]] = ("__nodes",)
    __TRACER: Final[Tracer] = TRACER.get_child("interface.executor")
    # TODO: Support passing `Node` object somehow.
    __CODEC: ClassVar[Codec] = Codec(
        serializers={
            DependencyReference: (2, DependencyReference.to_bytes),
            DependencyReferences: (3, DependencyReferences.to_bytes),
        },
        deserializers={2: DependencyReference.from_bytes, 3: DependencyReferences.from_bytes},
    )

    @classmethod
    def get_bytes_from_params(cls: type[Self], /, params: ExecutorParams) -> bytes:
        return params.to_bytes()

    @classmethod
    def get_params_from_bytes(cls: type[Self], /, packed: bytes) -> ExecutorParams:
        return ExecutorParams.from_bytes(packed)

    @classmethod
    @functools.lru_cache(maxsize=32, typed=True)
    @__TRACER.with_span_sync(Tracer.DEBUG, "executor.get_invoked_node")
    def get_invoked_node[SP: Any, BP: Any](
        cls: type[Self], /, call: NodeCall[Any, Any, SP, BP, ExecutorParams]
    ) -> InvokedNode[SP, BP, ExecutorParams]:
        cls.__TRACER.set_attributes_to_current_span(
            id=call.id,
            arguments=call.arguments,
            storage_params=call.node.storage_params,
            broker_params=call.node.broker_params,
            executor_params=call.node.executor_params,
        )

        arguments: Final[dict[int, Any]] = {}
        dependencies: Final[dict[NodeCall[Any, Any, SP, BP, ExecutorParams], bool]] = dict.fromkeys(
            # Non-data dependencies.
            call.dependencies,
            False,
        )

        def handle_argument(
            type_adapter: TypeAdapter, argument: Any
        ) -> DependencyReference | DependencyReferences | bytes:
            if isinstance(argument, NodeCall):
                dependencies[argument] = True
                return DependencyReference(dependency_id=argument.id)

            if isinstance(argument, NodeCalls):
                dependencies.update(dict.fromkeys(argument.calls, True))
                return DependencyReferences(dependency_ids=tuple(dependency.id for dependency in argument.calls))

            return type_adapter.dump_python(argument)

        index: int
        argument: Any
        for index, argument in call.arguments.items():
            parameter: Parameter
            type_adapter: TypeAdapter
            parameter, type_adapter = call.node.parameters[index]

            if parameter.kind == Parameter.VAR_POSITIONAL:
                arguments[index] = tuple(handle_argument(type_adapter, argument) for argument in argument)

            elif parameter.kind == Parameter.VAR_KEYWORD:
                arguments[index] = {key: handle_argument(type_adapter, argument) for key, argument in argument.items()}

            else:
                arguments[index] = handle_argument(type_adapter, argument)

        invoked_dependencies: Final[dict[InvokedNode[SP, BP, ExecutorParams], bool]] = {
            cls.get_invoked_node(dependency): is_data for dependency, is_data in dependencies.items()
        }
        cls.__TRACER.set_attributes_to_current_span(dependencies=invoked_dependencies)
        return InvokedNode(
            id=call.id,
            arguments=cls.__CODEC.to_bytes(arguments),
            dependencies=invoked_dependencies,
            storage_params=call.node.storage_params,
            broker_params=call.node.broker_params,
            executor_params=call.node.executor_params,
        )

    def __init__(self: Self, /) -> None:
        self.__nodes: Final[dict[str, Node[Any, Any, Any, Any, ExecutorParams]]] = {}

    def serve_node(self: Self, /, node: Node[Any, Any, Any, Any, ExecutorParams]) -> None:
        self.__TRACER.info(
            "executor.serve_node",
            node={
                "signature": node.signature,
                "storage_params": node.storage_params,
                "broker_params": node.broker_params,
                "executor_params": node.executor_params,
            },
        )
        self.__nodes[node.executor_params.node_id] = node

    @__TRACER.with_span_async(Tracer.DEBUG, "executor.execute_node")
    async def execute_node(
        self: Self,
        /,
        params: ExecutorParams,
        node: RunningNode,
        invoke_node_callback: Callable[[InvokedNode[Any, Any, ExecutorParams], bool], Awaitable[UUID]],
    ) -> InvokedNode | CompletedNode | None:
        self.__TRACER.set_attributes_to_current_span(node=node)
        arguments: Final[dict[int, Any]] = self.__CODEC.from_bytes(node.arguments)
        self.__TRACER.set_attributes_to_current_span(raw_arguments=arguments)
        node_handler: Final[Node[Any, Any, Any, Any, ExecutorParams]] = self.__nodes[params.node_id]
        self.__TRACER.set_attributes_to_current_span(
            node_handler={
                "signature": node_handler.signature,
                "storage_params": node_handler.storage_params,
                "broker_params": node_handler.broker_params,
                "executor_params": node_handler.executor_params,
            }
        )
        node_function: Final[Callable[[RunContext[Any, Any, ExecutorParams], *tuple[Any, ...]], Awaitable]] = cast(
            "Callable[[RunContext[Any, Any, ExecutorParams], *tuple[Any, ...]], Awaitable]", node_handler.function
        )

        def handle_argument(type_adapter: TypeAdapter, argument: Any) -> Any:
            if isinstance(argument, DependencyReference):
                argument = self.__CODEC.from_bytes(node.dependencies[argument.dependency_id])

            elif isinstance(argument, DependencyReferences):
                argument = tuple(
                    self.__CODEC.from_bytes(node.dependencies[dependency_id])
                    for dependency_id in argument.dependency_ids
                )

            return type_adapter.validate_python(argument)

        index: int
        argument: Any
        for index, argument in arguments.items():
            parameter: Parameter
            type_adapter: TypeAdapter
            parameter, type_adapter = node_handler.parameters[index]

            if parameter.kind == Parameter.VAR_POSITIONAL:
                arguments[index] = tuple(handle_argument(type_adapter, argument) for argument in argument)

            elif parameter.kind == Parameter.VAR_KEYWORD:
                arguments[index] = {key: handle_argument(type_adapter, argument) for key, argument in argument.items()}

            else:
                arguments[index] = handle_argument(type_adapter, argument)

        bound_arguments: Final[BoundArguments] = BoundArguments(
            node_handler.signature,
            arguments=OrderedDict(
                (parameter.name, arguments[index])
                # It's okay to use it like this, it's ordered and stable.
                for index, (parameter, _) in node_handler.parameters.items()
                if index in arguments
            ),
        )
        self.__TRACER.set_attributes_to_current_span(resolved_arguments=bound_arguments.arguments)

        async def invoke_node(
            node_call: NodeCall[Any, Any, Any, Any, ExecutorParams],
            is_new_session: bool,  # noqa: FBT001
        ) -> UUID:
            return await invoke_node_callback(self.get_invoked_node(node_call), is_new_session)

        run_context: Final[RunContext[Any, Any, ExecutorParams]] = RunContext(
            node.session_id, node.graph_id, node.id, invoke_node
        )

        return_value: Final[Any] = await asyncio.wait_for(
            fut=node_function(run_context, *bound_arguments.args, **bound_arguments.kwargs),
            timeout=params.timeout.total_seconds() if params.timeout is not None else None,
        )

        if return_value == PAUSE_MARKER:
            self.__TRACER.set_attributes_to_current_span(is_paused=True)
            return None

        if isinstance(return_value, NodeCall):
            return self.get_invoked_node(return_value)

        self.__TRACER.set_attributes_to_current_span(return_value=return_value)
        serialized_return_value: Final[Any] = node_handler.return_value_type_adapter.dump_python(return_value)
        return CompletedNode(id=node.id, result=self.__CODEC.to_bytes(serialized_return_value))
