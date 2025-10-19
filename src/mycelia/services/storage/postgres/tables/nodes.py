from datetime import datetime
from typing import ClassVar, Final, final
from uuid import UUID

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import BYTEA, SMALLINT, UUID as PostgresUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.schema import ForeignKey

from mycelia.services.storage.postgres.tables.base import Table
from mycelia.services.storage.postgres.types import UTCDateTime

__all__: Final[tuple[str, ...]] = ("NodesTable",)


@final
class NodesTable(Table):
    # TODO: This must be a ClassVar, but sqlalchemy has invalid typings.
    __tablename__: ClassVar[str] = "nodes"  # type: ignore[misc]

    id: Mapped[UUID] = mapped_column(PostgresUUID, primary_key=True)
    graph_id: Mapped[UUID] = mapped_column(ForeignKey(column="graphs.id", ondelete="CASCADE"))
    arguments: Mapped[bytes] = mapped_column(BYTEA)
    trace_context: Mapped[bytes] = mapped_column(BYTEA)
    broker_params: Mapped[bytes] = mapped_column(BYTEA)
    executor_params: Mapped[bytes] = mapped_column(BYTEA)
    pending_dependency_count: Mapped[int] = mapped_column(SMALLINT)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime)

    __table_args__: ClassVar[tuple[Index, ...]] = (  # type: ignore[misc]
        Index("ix_nodes_graph_id", graph_id),
        Index("ix_nodes_pending_dependency_count", pending_dependency_count),
    )
