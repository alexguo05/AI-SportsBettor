"""Alembic environment using the application's Cloud SQL connector."""

from logging.config import fileConfig
from pathlib import Path

from alembic import context

from src.db.engine import create_database_resources
from src.db.models import metadata

config = context.config
if config.config_file_name:
    fileConfig(config.config_file_name)
target_metadata = metadata


def run_migrations_offline() -> None:
    context.configure(
        url="postgresql://",
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    src_dir = Path(__file__).resolve().parents[1] / "src"
    resources = create_database_resources(src_dir)
    try:
        with resources.engine.connect() as connection:
            context.configure(
                connection=connection,
                target_metadata=target_metadata,
                compare_type=True,
            )
            with context.begin_transaction():
                context.run_migrations()
    finally:
        resources.close()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
