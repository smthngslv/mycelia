from datetime import datetime, timedelta
from typing import ClassVar, Final, final
from uuid import UUID

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import INTERVAL, UUID as PostgresUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from mycelia.services.storage.postgres.tables.base import Table
from mycelia.services.storage.postgres.types import UTCDateTime

__all__: Final[tuple[str, ...]] = ("SessionsTable",)


@final
class SessionsTable(Table):
    # TODO: This must be a ClassVar, but sqlalchemy has invalid typings.
    __tablename__: ClassVar[str] = "sessions"  # type: ignore[misc]

    id: Mapped[UUID] = mapped_column(PostgresUUID, primary_key=True)
    retention: Mapped[timedelta | None] = mapped_column(INTERVAL)
    cancelled_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    __table_args__: ClassVar[tuple[Index, ...]] = (  # type: ignore[misc]
        Index("ix_sessions_not_cancelled", id, postgresql_where=cancelled_at.is_(other=None)),
    )
