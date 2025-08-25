from typing import ClassVar, Final

__all__: Final[tuple[str, ...]] = ("GraphError",)


class GraphError(Exception):
    __slots__: ClassVar[tuple[str, ...]] = ()
