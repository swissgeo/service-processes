import asyncio
import logging

import app.models  # noqa: F401  # registers all ORM models on Base.metadata
from alembic import context
from app.models.base import Base
from app.settings import get_settings
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

settings = get_settings()

# There is no alembic.ini, so logging is configured here instead of via fileConfig().
logging.basicConfig(level=logging.WARNING)
logging.getLogger("alembic").setLevel(logging.INFO)
if settings.logging_handlers_level is not None:
    logging.getLogger().setLevel(settings.logging_handlers_level)

# MetaData of all ORM models, used for 'autogenerate' support
target_metadata = Base.metadata

# Single source of truth for the DB URL: reuse the app's own settings. The URL is passed directly
# to the engine instead of through config.set_main_option(), because the alembic config is
# ConfigParser based and would choke on the '%' of percent-encoded passwords.
database_url = settings.async_database_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    connectable = create_async_engine(
        database_url,
        poolclass=pool.NullPool,
        connect_args={"timeout": settings.db_connect_timeout},
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
