"""Messqlalchemyge: Indices.

Revision ID: 036e6694cae0
Revises: 797c81c201d2
Create Date: 2025-10-02 18:25:02.425348
"""

from collections.abc import Sequence
from typing import Final

import sqlalchemy
from alembic import op

__all__: Final[tuple[str, ...]] = ("branch_labels", "depends_on", "down_revision", "downgrade", "revision", "upgrade")

# Revision identifiers, used by Alembic.
revision: Final[str] = "036e6694cae0"
down_revision: Final[str | None] = "797c81c201d2"
branch_labels: Final[str | Sequence[str] | None] = None
depends_on: Final[str | Sequence[str] | None] = None

SCHEMA_NAME: Final[str] = "mycelia"


def upgrade() -> None:
    op.create_index(
        index_name="ix_sessions_not_cancelled",
        table_name="sessions",
        columns=("id",),
        unique=False,
        schema=SCHEMA_NAME,
        postgresql_where=sqlalchemy.text("cancelled_at IS NULL"),
    )
    op.create_index(
        index_name="ix_graphs_result_not_finished",
        table_name="graphs",
        columns=("id",),
        unique=False,
        schema=SCHEMA_NAME,
        postgresql_where=sqlalchemy.text("result IS NULL"),
    )
    op.create_index(
        index_name="ix_graphs_session_id_not_finished",
        table_name="graphs",
        columns=("session_id",),
        unique=False,
        schema=SCHEMA_NAME,
        postgresql_where=sqlalchemy.text("result IS NULL"),
    )
    op.create_index(
        index_name="ix_nodes_graph_id", table_name="nodes", columns=("graph_id",), unique=False, schema=SCHEMA_NAME
    )
    op.create_index(
        index_name="ix_nodes_pending_dependency_count",
        table_name="nodes",
        columns=("pending_dependency_count",),
        unique=False,
        schema=SCHEMA_NAME,
    )
    op.create_index(
        index_name="ix_dependencies_graph_id",
        table_name="dependencies",
        columns=("graph_id",),
        unique=False,
        schema=SCHEMA_NAME,
    )
    op.create_index(
        index_name="ix_dependencies_node_id_graph_id",
        table_name="dependencies",
        columns=("node_id", "graph_id"),
        unique=False,
        schema=SCHEMA_NAME,
    )
    op.create_index(
        index_name="ix_dependencies_node_id_is_data",
        table_name="dependencies",
        columns=("node_id", "is_data"),
        unique=False,
        schema=SCHEMA_NAME,
    )


def downgrade() -> None:
    op.drop_index(index_name="ix_dependencies_node_id_is_data", table_name="dependencies", schema=SCHEMA_NAME)
    op.drop_index(index_name="ix_dependencies_node_id_graph_id", table_name="dependencies", schema=SCHEMA_NAME)
    op.drop_index(index_name="ix_dependencies_graph_id", table_name="dependencies", schema=SCHEMA_NAME)
    op.drop_index(index_name="ix_nodes_pending_dependency_count", table_name="nodes", schema=SCHEMA_NAME)
    op.drop_index(index_name="ix_nodes_graph_id", table_name="nodes", schema=SCHEMA_NAME)
    op.drop_index(
        index_name="ix_graphs_session_id_not_finished",
        table_name="graphs",
        schema=SCHEMA_NAME,
        postgresql_where=sqlalchemy.text("result IS NULL"),
    )
    op.drop_index(
        index_name="ix_graphs_result_not_finished",
        table_name="graphs",
        schema=SCHEMA_NAME,
        postgresql_where=sqlalchemy.text("result IS NULL"),
    )
    op.drop_index(
        index_name="ix_sessions_not_cancelled",
        table_name="sessions",
        schema=SCHEMA_NAME,
        postgresql_where=sqlalchemy.text("cancelled_at IS NULL"),
    )
