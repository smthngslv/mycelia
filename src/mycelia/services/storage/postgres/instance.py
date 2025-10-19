import contextlib
from datetime import UTC, datetime, timedelta
from traceback import format_exception
from typing import TYPE_CHECKING, ClassVar, Final, Self, cast, final, overload
from uuid import UUID

import sqlalchemy
from pydantic import Field
from sqlalchemy import CTE, CompoundSelect, Exists, Result, RowMapping, Select
from sqlalchemy.exc import CompileError, DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql.elements import CompilerElement

from mycelia.core.entities import CompletedNode, CreatedGraph, CreatedNode, CreatedSession, ReadyNode, StartedNode
from mycelia.core.errors import NodeNodeFoundError, SessionCancelledError, SessionFinishedError, SessionNotFoundError
from mycelia.services.storage.interface import IStorage
from mycelia.services.storage.postgres.tables import DependenciesTable, GraphsTable, NodesTable, SessionsTable
from mycelia.services.storage.postgres.types import UTCDateTime
from mycelia.tracing import TRACER, Tracer
from mycelia.utils import Entity

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__: Final[tuple[str, ...]] = ("PostgresStorage", "PostgresStorageParams")


@final
class PostgresStorageParams(Entity):
    # TODO: Annotated syntax is not supported by mypy yet.
    session_retention: timedelta | None = Field(default=timedelta(days=7))


@final
class PostgresStorage(IStorage[PostgresStorageParams]):
    __slots__: ClassVar[tuple[str, ...]] = ("__engine", "__session_maker")

    __TRACER: Final[Tracer] = TRACER.get_child("services.storage.postgres")

    @classmethod
    def get_bytes_from_params(cls: type[Self], /, params: PostgresStorageParams) -> bytes:
        return params.to_bytes()

    @classmethod
    def get_params_from_bytes(cls: type[Self], /, packed: bytes) -> PostgresStorageParams:
        return PostgresStorageParams.from_bytes(packed)

    @__TRACER.with_span_sync(Tracer.INFO, "postgres_storage.create")
    def __init__(self, /, url: str) -> None:
        self.__engine: Final[AsyncEngine] = create_async_engine(
            url, isolation_level="AUTOCOMMIT", skip_autocommit_rollback=True
        )
        self.__session_maker: Final[async_sessionmaker[AsyncSession]] = async_sessionmaker(self.__engine)

    @overload
    async def create_node(self: Self, /, params: PostgresStorageParams, node: CreatedNode) -> bool:
        raise NotImplementedError

    @overload
    async def create_node(self: Self, /, params: PostgresStorageParams, node: CreatedNode, graph: CreatedGraph) -> bool:
        raise NotImplementedError

    @overload
    async def create_node(
        self: Self, /, params: PostgresStorageParams, node: CreatedNode, graph: CreatedGraph, session: CreatedSession
    ) -> bool:
        raise NotImplementedError

    @__TRACER.with_span_async(Tracer.DEBUG, "postgres_storage.create_node")
    async def create_node(
        self: Self,
        /,
        params: PostgresStorageParams,
        node: CreatedNode,
        graph: CreatedGraph | None = None,
        session: CreatedSession | None = None,
    ) -> bool:
        self.__TRACER.set_attributes_to_current_span(params=params, node=node, graph=graph, session=session)
        ctes: Final[list[CTE]] = []
        updated_parent_node_cte: CTE | None = None
        pending_dependencies_cte: CTE | None = None

        if session is not None:
            self.__TRACER.trace("postgres_storage.create_node.insert_session")
            ctes.append(
                sqlalchemy.insert(SessionsTable)
                .values(
                    {
                        SessionsTable.id: session.id,
                        SessionsTable.retention: params.session_retention,
                        SessionsTable.cancelled_at: None,
                    }
                )
                .cte(name="inserted_session")
            )

        if graph is not None:
            self.__TRACER.trace("postgres_storage.create_node.insert_graph")
            ctes.append(
                sqlalchemy.insert(GraphsTable)
                .values(
                    {
                        GraphsTable.id: graph.id,
                        GraphsTable.session_id: graph.session_id,
                        GraphsTable.trace_context: graph.trace_context,
                        GraphsTable.result: None,
                    }
                )
                .cte(name="inserted_graph")
            )

        if node.parent_id is not None:
            self.__TRACER.trace("postgres_storage.create_node.update_parent_node")
            updated_parent_node_cte = (
                sqlalchemy.update(NodesTable)
                .where(NodesTable.id == node.parent_id)
                .values({NodesTable.finished_at: datetime.now(UTC)})
                .returning(sqlalchemy.literal(value=True).label(name="is_parent_node_updated"))
                .cte(name="updated_parent_node")
            )

        if len(node.dependencies) != 0:
            inserted_dependencies_cte: Final[CTE] = (
                sqlalchemy.insert(DependenciesTable)
                .values(
                    tuple(
                        {
                            DependenciesTable.node_id: node.id,
                            DependenciesTable.graph_id: graph_id,
                            DependenciesTable.is_data: is_data,
                        }
                        for graph_id, is_data in node.dependencies.items()
                    )
                )
                .returning(DependenciesTable.graph_id)
                .cte(name="inserted_dependencies")
            )
            pending_dependencies_cte = (
                sqlalchemy.select(1)
                .select_from(GraphsTable)
                .join(inserted_dependencies_cte, onclause=GraphsTable.id == inserted_dependencies_cte.columns.graph_id)
                .where(GraphsTable.result.is_(other=None))
                # Ordering handles deadlocks.
                .order_by(GraphsTable.id.asc())
                .with_for_update(of=GraphsTable)
                .cte(name="pending_dependencies")
            )

        inserted_node_cte: Final[CTE] = (
            sqlalchemy.insert(NodesTable)
            .values(
                {
                    NodesTable.id: node.id,
                    NodesTable.graph_id: node.id,
                    NodesTable.arguments: node.arguments,
                    NodesTable.trace_context: node.trace_context,
                    NodesTable.broker_params: node.broker_params,
                    NodesTable.executor_params: node.executor_params,
                    NodesTable.pending_dependency_count: (
                        sqlalchemy.select(sqlalchemy.func.count())
                        .select_from(pending_dependencies_cte)
                        .scalar_subquery()
                        if pending_dependencies_cte is not None
                        else 0
                    ),
                    NodesTable.created_at: datetime.now(UTC),
                    NodesTable.finished_at: None,
                }
            )
            .returning((NodesTable.pending_dependency_count == 0).label(name="is_ready"))
            .cte("inserted_node")
        )

        statement: Final[CompoundSelect | Select] = (
            sqlalchemy.union_all(sqlalchemy.select(inserted_node_cte), sqlalchemy.select(updated_parent_node_cte))
            if updated_parent_node_cte is not None
            else sqlalchemy.select(inserted_node_cte)
        ).add_cte(*ctes)

        if self.__TRACER.level == Tracer.TRACE:
            self.__TRACER.set_attributes_to_current_span(sql=self.__get_sql(statement))

        db_session: AsyncSession
        async with self.__session_maker() as db_session:
            result: Final[Result] = await db_session.execute(statement)

        flags: Final[Sequence[bool]] = result.scalars().all()
        if updated_parent_node_cte is not None and len(flags) == 1:
            raise NodeNodeFoundError(cast("UUID", node.parent_id))

        self.__TRACER.set_attributes_to_current_span(is_ready=flags[0])
        return flags[0]

    @__TRACER.with_span_async(Tracer.DEBUG, "postgres_storage.start_node")
    async def start_node(self: Self, /, id_: UUID) -> StartedNode:
        self.__TRACER.set_attributes_to_current_span(id=id_)

        original_started_node_cte: Final[CTE] = (
            sqlalchemy.select(
                NodesTable.id,
                NodesTable.graph_id,
                NodesTable.arguments,
                NodesTable.executor_params,
                GraphsTable.trace_context.label(name="graph_trace_context"),
                SessionsTable.cancelled_at.isnot(other=None).label(name="session_is_cancelled"),
            )
            .select_from(NodesTable)
            .join(GraphsTable, onclause=NodesTable.graph_id == GraphsTable.id)
            .join(SessionsTable, onclause=GraphsTable.session_id == SessionsTable.id)
            .where(NodesTable.id == id_)
            .cte(name="original_started_node")
        )

        updated_started_node_cte: Final[CTE] = (
            sqlalchemy.update(NodesTable)
            .where(NodesTable.id == id_)
            .values({NodesTable.started_at: datetime.now(UTC)})
            .cte(name="updated_started_node")
        )

        dependencies_cte: Final[CTE] = (
            sqlalchemy.select(
                DependenciesTable.node_id,
                sqlalchemy.func.array_agg(GraphsTable.id).label(name="graph_ids"),
                sqlalchemy.func.array_agg(GraphsTable.result).label(name="graph_results"),
            )
            .select_from(DependenciesTable)
            .join(GraphsTable, onclause=DependenciesTable.graph_id == GraphsTable.id)
            .where(DependenciesTable.node_id == id_)
            .where(DependenciesTable.is_data.is_(other=True))
            .group_by(DependenciesTable.node_id)
            .cte(name="dependencies")
        )

        statement: Final[Select] = (
            sqlalchemy.select(
                original_started_node_cte.columns.graph_id,
                original_started_node_cte.columns.arguments,
                original_started_node_cte.columns.executor_params,
                original_started_node_cte.columns.graph_trace_context,
                original_started_node_cte.columns.session_is_cancelled,
                sqlalchemy.func.coalesce(dependencies_cte.columns.graph_ids, []).label(name="dependency_graph_ids"),
                sqlalchemy.func.coalesce(dependencies_cte.columns.graph_results, []).label(
                    name="dependency_graph_results"
                ),
            )
            .select_from(original_started_node_cte)
            .join(
                dependencies_cte,
                onclause=original_started_node_cte.columns.id == dependencies_cte.columns.node_id,
                isouter=True,
            )
            .add_cte(updated_started_node_cte)
        )

        if self.__TRACER.level == Tracer.TRACE:
            self.__TRACER.set_attributes_to_current_span(sql=self.__get_sql(statement))

        db_session: AsyncSession
        async with self.__session_maker() as db_session:
            result: Final[Result] = await db_session.execute(statement)

        nodes: Final[Sequence[RowMapping]] = result.mappings().all()
        if len(nodes) == 0:
            raise NodeNodeFoundError(id_)

        if nodes[0]["session_is_cancelled"]:
            raise SessionCancelledError(id_)

        node: Final[StartedNode] = StartedNode(
            id=id_,
            graph_id=nodes[0]["graph_id"],
            arguments=nodes[0]["arguments"],
            graph_trace_context=nodes[0]["graph_trace_context"],
            dependencies=dict(zip(nodes[0]["dependency_graph_ids"], nodes[0]["dependency_graph_results"], strict=True)),
            executor_params=nodes[0]["executor_params"],
        )
        self.__TRACER.set_attributes_to_current_span(node=node)
        return node

    @__TRACER.with_span_async(Tracer.DEBUG, "postgres_storage.complete_node")
    async def complete_node(self: Self, /, node: CompletedNode) -> list[ReadyNode]:
        self.__TRACER.set_attributes_to_current_span(node=node)

        statement: Final[Select] = sqlalchemy.select(
            sqlalchemy.func.mycelia.complete_node(
                node.id, node.result, sqlalchemy.literal(datetime.now(UTC), UTCDateTime)
            )
            .table_valued("id", "trace_context", "broker_params", "session_id")
            .alias(name="ready_nodes")
        )

        if self.__TRACER.level == Tracer.TRACE:
            self.__TRACER.set_attributes_to_current_span(sql=self.__get_sql(statement))

        db_session: AsyncSession
        async with self.__session_maker() as db_session:
            try:
                result: Final[Result] = await db_session.execute(statement)

            except DBAPIError as exception:
                if "node.not_found" in str(exception):
                    raise NodeNodeFoundError(node.id) from exception

                raise

        ready_nodes: Final[list[ReadyNode]] = [ReadyNode(**ready_node) for ready_node in result.mappings().all()]
        self.__TRACER.set_attributes_to_current_span(ready_nodes=ready_nodes)
        return ready_nodes

    @__TRACER.with_span_async(Tracer.DEBUG, "postgres_storage.cancel_session")
    async def cancel_session(self: Self, /, id_: UUID) -> bool:
        self.__TRACER.set_attributes_to_current_span(id=id_)

        is_running: Final[Exists] = (
            sqlalchemy.select(1)
            .where(GraphsTable.session_id == id_)
            .where(GraphsTable.result.is_(other=None))
            .limit(1)
            .exists()
        )

        statement: Final[CompoundSelect] = sqlalchemy.union_all(
            sqlalchemy.select("*").select_from(
                sqlalchemy.update(SessionsTable)
                .where(SessionsTable.id == id_)
                .where(SessionsTable.cancelled_at.is_(other=None))
                .where(is_running)
                .values({SessionsTable.cancelled_at: datetime.now(UTC)})
                .returning(sqlalchemy.literal(value=True).label(name="is_updated"))
                .cte(name="updated_session")
            ),
            sqlalchemy.select("*").select_from(
                sqlalchemy.select(SessionsTable.cancelled_at.isnot(None).label(name="is_cancelled"))
                .where(SessionsTable.id == id_)
                .cte(name="session_exists")
            ),
        )

        if self.__TRACER.level == Tracer.TRACE:
            self.__TRACER.set_attributes_to_current_span(sql=self.__get_sql(statement))

        db_session: AsyncSession
        async with self.__session_maker() as db_session:
            result: Final[Result] = await db_session.execute(statement)

        results: Final[Sequence[bool]] = result.scalars().all()

        if len(results) == 2:  # noqa: PLR2004
            return True

        if len(results) == 0:
            raise SessionNotFoundError(id_)

        if results[0]:
            raise SessionCancelledError(id_)

        raise SessionFinishedError(id_)

    @__TRACER.with_span_async(Tracer.INFO, "postgres_storage.shutdown")
    async def shutdown(self: Self, /) -> None:
        await self.__engine.dispose()

    def __get_sql(self: Self, /, statement: CompilerElement) -> str:
        try:
            with contextlib.suppress(CompileError):
                return str(statement.compile(self.__engine, compile_kwargs={"literal_binds": True}))

            return str(statement.compile(self.__engine))

        except Exception as exception:
            return "".join(format_exception(type(exception), exception, exception.__traceback__))
