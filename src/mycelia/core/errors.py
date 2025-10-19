from typing import ClassVar, Final, Self, final
from uuid import UUID

__all__: Final[tuple[str, ...]] = (
    "Error",
    "NodeNodeFoundError",
    "SessionCancelledError",
    "SessionFinishedError",
    "SessionFinishedError",
)


class Error(Exception):
    __slots__: ClassVar[tuple[str, ...]] = ()


@final
class NodeNodeFoundError(Error):
    __slots__: ClassVar[tuple[str, ...]] = ()

    def __init__(self: Self, /, node_id: UUID) -> None:
        super().__init__(f"Node `{node_id}` not found.")


@final
class SessionNotFoundError(Error):
    __slots__: ClassVar[tuple[str, ...]] = ()

    def __init__(self: Self, /, session_id: UUID) -> None:
        super().__init__(f"Session `{session_id}` not found.")


@final
class SessionFinishedError(Error):
    __slots__: ClassVar[tuple[str, ...]] = ()

    def __init__(self: Self, /, session_id: UUID) -> None:
        super().__init__(f"Session `{session_id}` finished.")


@final
class SessionCancelledError(Error):
    __slots__: ClassVar[tuple[str, ...]] = ()

    def __init__(self: Self, /, session_id: UUID) -> None:
        super().__init__(f"Session `{session_id}` cancelled.")
