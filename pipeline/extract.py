"""Extract: pooled read-only connection to Supabase Postgres (psycopg3).

This module will grow across issues #2-#6: connection (#2), watermark +
SQL builder (#3), table extraction (#4), bronze staging (#5), and the
deterministic demo sample generator (#6). No cleaning or transformation
lives here.
"""
from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from pipeline import config as C

log = logging.getLogger(__name__)


def get_database_url() -> str | None:
    """Return the DB URL from env (DATABASE_URL preferred, DIRECT_URL fallback)."""
    url = os.getenv("DATABASE_URL") or os.getenv("DIRECT_URL")
    if not url:
        return None
    url = url.strip().strip("'").strip('"')
    # SQLAlchemy needs an explicit psycopg3 dialect.
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://") and "+psycopg" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def create_db_engine(url: str | None = None) -> Engine:
    """Create a pooled SQLAlchemy 2.x engine over psycopg3 with SSL."""
    url = url or get_database_url()
    if not url:
        raise RuntimeError(
            "No DATABASE_URL / DIRECT_URL set. "
            "Run with --demo for the file-only demo, or export a connection string."
        )
    return create_engine(
        url,
        pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={
            "sslmode": os.getenv("DB_SSLMODE", "require"),
            "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
            "application_name": "rentech-batch-pipeline",
        },
    )


def ping(engine: Engine) -> bool:
    """V1: prove the source is reachable before any table read."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    log.info("Source connection OK")
    return True
