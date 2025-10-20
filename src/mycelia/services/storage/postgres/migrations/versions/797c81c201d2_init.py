"""Message: Init.

Revision ID: 797c81c201d2
Revises: -
Create Date: 2025-10-01 18:26:41.403747
"""

from collections.abc import Sequence
from typing import Final

from alembic import op
from sqlalchemy import Column, ForeignKeyConstraint, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import BOOLEAN, BYTEA, INTERVAL, SMALLINT, UUID

from mycelia.services.storage.postgres.types import UTCDateTime

__all__: Final[tuple[str, ...]] = ("branch_labels", "depends_on", "down_revision", "downgrade", "revision", "upgrade")

# Revision identifiers, used by Alembic.
revision: Final[str] = "797c81c201d2"
down_revision: Final[str | None] = None
branch_labels: Final[str | Sequence[str] | None] = None
depends_on: Final[str | Sequence[str] | None] = None


def upgrade() -> None:
    op.create_table(
        "sessions",
        Column("id", UUID(), nullable=False),
        Column("retention", INTERVAL(), nullable=True),
        Column("cancelled_at", UTCDateTime(), nullable=True),
        PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "graphs",
        Column("id", UUID(), nullable=False),
        Column("session_id", UUID(), nullable=False),
        Column("trace_context", BYTEA(), nullable=False),
        Column("result", BYTEA(), nullable=True),
        ForeignKeyConstraint(columns=("session_id",), refcolumns=("sessions.id",), ondelete="CASCADE"),
        PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "nodes",
        Column("id", UUID(), nullable=False),
        Column("graph_id", UUID(), nullable=False),
        Column("arguments", BYTEA(), nullable=False),
        Column("trace_context", BYTEA(), nullable=False),
        Column("broker_params", BYTEA(), nullable=False),
        Column("executor_params", BYTEA(), nullable=False),
        Column("pending_dependency_count", SMALLINT(), nullable=False),
        Column("created_at", UTCDateTime(), nullable=False),
        Column("started_at", UTCDateTime(), nullable=True),
        Column("finished_at", UTCDateTime(), nullable=True),
        ForeignKeyConstraint(columns=("graph_id",), refcolumns=("graphs.id",), ondelete="CASCADE"),
        PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "dependencies",
        Column("node_id", UUID(), nullable=False),
        Column("graph_id", UUID(), nullable=False),
        Column("is_data", BOOLEAN(), nullable=False),
        ForeignKeyConstraint(columns=("graph_id",), refcolumns=("graphs.id",), ondelete="CASCADE"),
        ForeignKeyConstraint(columns=("node_id",), refcolumns=("nodes.id",), ondelete="CASCADE"),
        PrimaryKeyConstraint("node_id", "graph_id"),
    )

    op.execute(
        sqltext="""\
CREATE FUNCTION
    complete_node(
        completed_node_id uuid,
        completed_graph_result bytea,
        completed_node_finished_at timestamp without time zone
    )

RETURNS
    TABLE(id uuid, trace_context bytea, broker_params bytea, session_id uuid)

LANGUAGE
    plpgsql
AS
    $func$

DECLARE
    completed_graph_id uuid;

BEGIN
    -- Update node.
    UPDATE
        nodes

    SET
        finished_at = completed_node_finished_at

    WHERE
        nodes.id = completed_node_id

    RETURNING
        nodes.graph_id

    INTO
        completed_graph_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'node.not_found';
    END IF;

    -- Update graph, only if this is the first update.
    UPDATE
        graphs

    SET
        result = completed_graph_result

    WHERE
        graphs.id = completed_graph_id AND graphs.result IS NULL;

    -- Update dependencies, if this is the first execution (result was NULL previously).
    IF FOUND THEN
        -- Return ready nodes.
        RETURN QUERY WITH updated_nodes AS (
            UPDATE
                nodes

            SET
                pending_dependency_count = pending_dependency_count - 1

            FROM
                dependencies

            WHERE
                nodes.id = dependencies.node_id
                AND dependencies.graph_id = completed_graph_id

            RETURNING
                nodes.id,
                nodes.graph_id,
                nodes.trace_context,
                nodes.broker_params,
                nodes.pending_dependency_count
        )

        SELECT
            updated_nodes.id,
            updated_nodes.trace_context,
            updated_nodes.broker_params,
            graphs.session_id

        FROM
            updated_nodes

        JOIN
            graphs ON updated_nodes.graph_id = graphs.id

        WHERE
            updated_nodes.pending_dependency_count = 0;

        RETURN;
    END IF;

    -- Otherwise still update the graph, but do not handle dependencies, since they are already handled.
    UPDATE
        graphs

    SET
        result = completed_graph_result

    WHERE
        graphs.id = completed_graph_id;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- Return ready nodes.
    RETURN QUERY SELECT
        nodes.id,
        nodes.trace_context,
        nodes.broker_params,
        graphs.session_id

    FROM
        nodes

    JOIN
        graphs ON nodes.graph_id = graphs.id

    JOIN
        dependencies ON nodes.id = dependencies.node_id

    WHERE
        dependencies.graph_id = completed_graph_id AND nodes.pending_dependency_count = 0;

    RETURN;
END;
$func$;"""
    )


def downgrade() -> None:
    op.execute(sqltext="DROP FUNCTION complete_node(uuid, bytea, timestamp without time zone);")
    op.drop_table(table_name="dependencies")
    op.drop_table(table_name="nodes")
    op.drop_table(table_name="graphs")
    op.drop_table(table_name="sessions")
