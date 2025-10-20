from typing import Final

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase

__all__: Final[tuple[str, ...]] = ("Table",)


class Table(AsyncAttrs, DeclarativeBase):
    pass
