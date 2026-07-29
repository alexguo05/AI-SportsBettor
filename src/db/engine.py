"""Create SQLAlchemy engines for local PostgreSQL or Cloud SQL."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import google.auth
from dotenv import dotenv_values
from google.cloud.sql.connector import Connector, IPTypes
from google.oauth2 import service_account
from sqlalchemy import Engine, create_engine


@dataclass(frozen=True)
class DatabaseConfig:
    database_name: str
    instance_connection_name: str | None
    iam_user: str | None
    postgres_dsn: str | None
    ip_type: str
    service_account_file: Path | None


@dataclass
class DatabaseResources:
    engine: Engine
    connector: Connector | None = None

    def close(self) -> None:
        self.engine.dispose()
        if self.connector:
            self.connector.close()


def _environment(src_dir: Path) -> dict[str, str]:
    dotenv_path = src_dir / ".env"
    file_values = dotenv_values(dotenv_path) if dotenv_path.exists() else {}
    values = {key: str(value) for key, value in file_values.items() if value is not None}
    values.update(os.environ)
    return values


def load_database_config(src_dir: Path) -> DatabaseConfig:
    values = _environment(src_dir)
    configured_credentials = values.get("GOOGLE_APPLICATION_CREDENTIALS")
    fallback_credentials = src_dir / "ai-sports-bettor-559e8837739f.json"
    service_account_file = (
        Path(configured_credentials).expanduser()
        if configured_credentials
        else fallback_credentials
        if fallback_credentials.exists()
        else None
    )
    return DatabaseConfig(
        database_name=values.get("POSTGRES_DB", "sportsbettor"),
        instance_connection_name=values.get("CLOUD_SQL_INSTANCE_CONNECTION_NAME"),
        iam_user=values.get("CLOUD_SQL_IAM_USER"),
        postgres_dsn=values.get("POSTGRES_DSN"),
        ip_type=values.get("CLOUD_SQL_IP_TYPE", "PUBLIC").upper(),
        service_account_file=service_account_file,
    )


def _load_google_credentials(config: DatabaseConfig) -> tuple[Any, str | None]:
    if config.service_account_file:
        credentials = service_account.Credentials.from_service_account_file(
            str(config.service_account_file)
        )
        return credentials, credentials.service_account_email
    credentials, _project = google.auth.default()
    return credentials, getattr(credentials, "service_account_email", None)


def _postgres_iam_username(email: str) -> str:
    return email.removesuffix(".gserviceaccount.com")


def create_database_resources(src_dir: Path) -> DatabaseResources:
    config = load_database_config(src_dir)
    if not config.instance_connection_name:
        dsn = config.postgres_dsn or (
            "postgresql+psycopg://sportsbettor:sportsbettor@localhost:5432/sportsbettor"
        )
        return DatabaseResources(engine=create_engine(dsn, pool_pre_ping=True, pool_recycle=1800))

    credentials, credential_email = _load_google_credentials(config)
    iam_user = config.iam_user or (
        _postgres_iam_username(credential_email) if credential_email else None
    )
    if not iam_user:
        raise ValueError(
            "CLOUD_SQL_IAM_USER is required when the active credentials do not "
            "expose a service-account email"
        )
    try:
        ip_type = IPTypes[config.ip_type]
    except KeyError as exc:
        raise ValueError("CLOUD_SQL_IP_TYPE must be PUBLIC, PRIVATE, or PSC") from exc

    connector = Connector(credentials=credentials, refresh_strategy="LAZY")

    def connect() -> Any:
        return connector.connect(
            config.instance_connection_name,
            "pg8000",
            user=iam_user,
            db=config.database_name,
            enable_iam_auth=True,
            ip_type=ip_type,
        )

    engine = create_engine(
        "postgresql+pg8000://",
        creator=connect,
        pool_pre_ping=True,
        pool_recycle=1800,
    )
    return DatabaseResources(engine=engine, connector=connector)
