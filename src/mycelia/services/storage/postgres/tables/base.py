from typing import ClassVar, Final

from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql.schema import MetaData

__all__: Final[tuple[str, ...]] = ("Table",)


class Table(AsyncAttrs, DeclarativeBase):
    metadata: ClassVar[MetaData] = MetaData(schema="mycelia")
