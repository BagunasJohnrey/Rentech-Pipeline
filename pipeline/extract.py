"""Extract: pooled read-only connection to Supabase Postgres (psycopg3).

This module will grow across issues #2-#6: connection (#2), watermark +
SQL builder (#3), table extraction (#4), bronze staging (#5), and the
deterministic demo sample generator (#6). No cleaning or transformation
lives here.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import polars as pl

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


def read_last_run(data_dir: Path = C.DEFAULT_DATA_DIR) -> datetime | None:
    """Return the watermark, or None (=> full extract) if missing/corrupt."""
    state_path = Path(data_dir) / C.STATE_FILE
    if not state_path.exists():
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        raw = payload.get("last_run")
        return datetime.fromisoformat(raw) if raw else None
    except (ValueError, OSError) as exc:
        log.warning("Could not read watermark %s: %s (full extract)", state_path, exc)
        return None


def write_last_run(run_at: datetime, data_dir: Path = C.DEFAULT_DATA_DIR) -> Path:
    """Persist the watermark. Caller must invoke ONLY after a successful run."""
    state_path = Path(data_dir) / C.STATE_FILE
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps({"last_run": run_at.isoformat()}, indent=2), encoding="utf-8"
    )
    return state_path


def _select_sql(table: str, has_watermark: bool) -> str:
    """Explicit-column SELECT with physical AS canonical aliases + window."""
    mapping: dict[str, str] = C.TABLES[table]["columns"]
    cols = ", ".join(f'"{phys}" AS "{canon}"' for canon, phys in mapping.items())
    sql = f'SELECT {cols} FROM "{table}"'
    if has_watermark:
        sql += f' WHERE {C.TABLES[table]["watermark_sql"]}'
        sql += f' ORDER BY {C.TABLES[table]["watermark_order"]}'
    return sql


def extract_table(
    engine, table: str, last_run: datetime | None
) -> pl.DataFrame:
    """Extract one table incrementally into a Polars frame (canonical names)."""
    has_watermark = last_run is not None
    sql = _select_sql(table, has_watermark)
    log.info(
        "Extracting %s (%s)",
        table,
        f"incremental since {last_run.isoformat()}" if has_watermark
        else "initial load (no watermark)",
    )
    # DB timestamps are naive; bind a naive UTC watermark to match.
    bound = last_run.replace(tzinfo=None) if has_watermark else None
    with engine.connect() as conn:
        params = {"last_run": bound} if bound is not None else {}
        result = conn.execute(text(sql), params)
        rows = result.mappings().all()
    if not rows:
        log.info("Extracted %s: 0 rows", table)
        return pl.DataFrame(
            schema={c: pl.String for c in C.TABLES[table]["columns"]}
        )
    df = pl.from_dicts([dict(r) for r in rows])
    log.info("Extracted %s: %d rows", table, df.height)
    return df


def extract_all(
    engine, data_dir: Path = C.DEFAULT_DATA_DIR
) -> dict[str, pl.DataFrame]:
    """Extract every source table using the stored watermark."""
    last_run = read_last_run(data_dir)
    log.info("Watermark last_run=%s", last_run.isoformat() if last_run else "none")
    return {t: extract_table(engine, t, last_run) for t in C.TABLES}
