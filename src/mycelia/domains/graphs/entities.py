from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, final
from uuid import UUID

__all__: Final[tuple[str, ...]] = ("Call", "Node")


@final
@dataclass(frozen=True)
class Node:
    graph_id: UUID
    handler_id: Any
    arguments: Mapping[int | str, Any]
    broker_options: Any
    executor_options: Any


@final
@dataclass(frozen=True)
class Call:
    id: UUID
    handler_id: Any
    arguments: Mapping[int | str, Any]
    broker_options: Any
    storage_options: Any
    executor_options: Any
