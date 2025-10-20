import asyncio
from typing import TYPE_CHECKING, Any, Final, cast

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncConnection, async_engine_from_config
from sqlalchemy.sql.ddl import CreateSchema

from mycelia.services.storage.postgres.tables.base import Table

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio.engine import AsyncEngine

__all__: Final[tuple[str, ...]] = ()


def run_migrations_offline() -> None:
    raise NotImplementedError


def do_run_migrations_online(connection: Connection, /) -> None:
    context.configure(connection, target_metadata=Table.metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    cli_arguments: Final[dict[str, str]] = context.get_x_argument(as_dictionary=True)
    if "url" not in cli_arguments:
        message: Final[str] = "Please specify the database URL."
        raise RuntimeError(message)

    schema: Final[str] = cli_arguments.get("schema", "mycelia")
    engine: Final[AsyncEngine] = async_engine_from_config(
        cast("dict[str, Any]", context.config.toml_alembic_config),
        url=cli_arguments["url"],
        connect_args={"server_settings": {"search_path": schema}},
    )

    try:
        connection: AsyncConnection
        async with engine.connect() as connection:
            await connection.execute(CreateSchema(schema, if_not_exists=True))
            await connection.run_sync(do_run_migrations_online)
            await connection.commit()

    finally:
        await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()

else:
    asyncio.run(run_migrations_online())
