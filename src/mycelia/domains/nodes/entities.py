from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, final
from uuid import UUID

__all__: Final[tuple[str, ...]] = ("Event", "Node", "NodeCall", "NodeCompletedEvent", "NodeEnqueuedEvent")

type Event = NodeCompletedEvent | NodeEnqueuedEvent


@final
@dataclass
class Node:
    id: UUID
    handler_id: Any
    arguments: dict[int | str, Any]
    broker_options: Any
    storage_options: Any
    executor_options: Any


@final
@dataclass
class NodeCall:
    id: UUID
    handler_id: Any
    arguments: dict[int | str, Any]
    broker_options: Any
    storage_options: Any
    executor_options: Any


@final
@dataclass
class NodeEnqueuedEvent:
    node_id: UUID
    logfire_ctx: Mapping[str, Any]
    storage_options: Any


@final
@dataclass
class NodeCompletedEvent:
    node_id: UUID
    result_id: UUID
    storage_options: Any
