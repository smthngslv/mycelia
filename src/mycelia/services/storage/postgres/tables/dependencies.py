from typing import ClassVar, Final, final
from uuid import UUID

from sqlalchemy import Index
from sqlalchemy.dialects.postgresql import BOOLEAN
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.schema import ForeignKey

from mycelia.services.storage.postgres.tables.base import Table

__all__: Final[tuple[str, ...]] = ("DependenciesTable",)


@final
class DependenciesTable(Table):
    # TODO: This must be a ClassVar, but sqlalchemy has invalid typings.
    __tablename__: ClassVar[str] = "dependencies"  # type: ignore[misc]

    node_id: Mapped[UUID] = mapped_column(ForeignKey(column="nodes.id", ondelete="CASCADE"), primary_key=True)
    graph_id: Mapped[UUID] = mapped_column(ForeignKey(column="graphs.id", ondelete="CASCADE"), primary_key=True)
    is_data: Mapped[bool] = mapped_column(BOOLEAN)

    __table_args__: ClassVar[tuple[Index, ...]] = (  # type: ignore[misc]
        Index("ix_mycelia_dependencies_node_id_is_data", node_id, is_data),
        Index("ix_mycelia_dependencies_graph_id", graph_id),
        Index("ix_mycelia_dependencies_node_id_graph_id", node_id, graph_id),
    )
