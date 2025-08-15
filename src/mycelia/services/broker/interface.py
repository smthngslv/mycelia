from abc import abstractmethod
from typing import Any, Final, Protocol, Self

from mycelia.domains.nodes.entities import Event

__all__: Final[tuple[str, ...]] = ("IBroker",)


class IBroker(Protocol):
    @abstractmethod
    async def publish(self: Self, event: Event, options: Any) -> None:
        raise NotImplementedError
