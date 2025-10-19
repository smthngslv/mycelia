from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, final
from uuid import UUID

from mycelia.tracing import TraceContext
from mycelia.utils import EventWithValue, SingleUseLockWithValue

__all__: Final[tuple[str, ...]] = (
    "CompletedNode",
    "Context",
    "CreatedGraph",
    "CreatedNode",
    "CreatedSession",
    "EnqueuedNode",
    "InvokedNode",
    "ReadyNode",
    "RunningNode",
    "StartedNode",
)


@final
@dataclass(frozen=True, eq=False)
class InvokedNode[SP: Any, BP: Any, EP: Any]:
    id: UUID
    arguments: bytes
    dependencies: Mapping["InvokedNode[SP, BP, EP]", bool]
    storage_params: SP
    broker_params: BP
    executor_params: EP


@final
@dataclass(frozen=True, eq=False)
class CreatedNode:
    id: UUID
    parent_id: UUID | None
    graph_id: UUID
    arguments: bytes
    dependencies: Mapping[UUID, bool]
    trace_context: bytes
    broker_params: bytes
    executor_params: bytes


@final
@dataclass(frozen=True, eq=False)
class CreatedGraph:
    id: UUID
    session_id: UUID
    trace_context: bytes


@final
@dataclass(frozen=True, eq=False)
class CreatedSession:
    id: UUID


@final
@dataclass(frozen=True, eq=False)
class ReadyNode:
    id: UUID
    session_id: UUID
    broker_params: bytes
    trace_context: bytes


@final
@dataclass(frozen=True, eq=False)
class EnqueuedNode:
    id: UUID
    session_id: UUID
    trace_context: bytes


@final
@dataclass(frozen=True, eq=False)
class StartedNode:
    id: UUID
    graph_id: UUID
    arguments: bytes
    dependencies: Mapping[UUID, bytes]
    graph_trace_context: bytes
    executor_params: bytes


@final
@dataclass(frozen=True, eq=False)
class RunningNode:
    id: UUID
    graph_id: UUID
    session_id: UUID
    arguments: bytes
    dependencies: Mapping[UUID, bytes]


@final
@dataclass(frozen=True, eq=False)
class CompletedNode:
    id: UUID
    result: bytes


@final
@dataclass(frozen=True, eq=False)
class Context:
    node_id: UUID
    graph_id: UUID
    session_id: SingleUseLockWithValue[UUID]
    graph_trace_context: TraceContext
    created_nodes: dict[UUID, EventWithValue[UUID]]
