from __future__ import annotations

import os

import psycopg

from wildfire_smoke.settings import Settings


def psycopg_conninfo(settings: Settings | None = None) -> str:
    explicit = os.environ.get("PSYCOPG_CONNINFO")
    if explicit:
        return explicit

    s = settings or Settings.from_env()
    return (
        f"host={s.postgres_host} "
        f"port={s.postgres_port} "
        f"dbname={s.postgres_db} "
        f"user={s.postgres_user} "
        f"password={s.postgres_password}"
    )


def connect(settings: Settings | None = None):
    return psycopg.connect(psycopg_conninfo(settings))


def jdbc_properties() -> dict[str, str]:
    return {
        "user": os.environ.get("JDBC_USER", os.environ.get("POSTGRES_USER", "smoke")),
        "password": os.environ.get("JDBC_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "smoke")),
        "driver": "org.postgresql.Driver",
    }


def jdbc_url() -> str:
    return os.environ.get("JDBC_URL", "jdbc:postgresql://postgres:5432/smoke")
