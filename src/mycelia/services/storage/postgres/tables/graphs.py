from typing import ClassVar, Final, final
from uuid import UUID

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import BYTEA, UUID as PostgresUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.schema import ForeignKey

from mycelia.services.storage.postgres.tables.base import Table

__all__: Final[tuple[str, ...]] = ("GraphsTable",)


@final
class GraphsTable(Table):
    # TODO: This must be a ClassVar, but sqlalchemy has invalid typings.
    __tablename__: ClassVar[str] = "graphs"  # type: ignore[misc]

    id: Mapped[UUID] = mapped_column(PostgresUUID, primary_key=True)
    session_id: Mapped[UUID] = mapped_column(ForeignKey(column="sessions.id", ondelete="CASCADE"))
    trace_context: Mapped[bytes] = mapped_column(BYTEA)
    result: Mapped[bytes | None] = mapped_column(BYTEA)

    __table_args__: ClassVar[tuple[Index, ...]] = (  # type: ignore[misc]
        Index("ix_graphs_session_id_not_finished", session_id, postgresql_where=result.is_(other=None)),
        Index("ix_graphs_result_not_finished", id, postgresql_where=result.is_(other=None)),
    )
