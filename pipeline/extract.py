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
import random
from datetime import date, datetime, timedelta, timezone
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


def stage_raw(
    frames: dict[str, pl.DataFrame],
    run_date: date,
    data_dir: Path = C.DEFAULT_DATA_DIR,
) -> Path:
    """Write bronze snapshots: data/raw/dt=YYYY-MM-DD/<table>.parquet (zstd)."""
    partition = Path(data_dir) / "raw" / f"dt={run_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for table, df in frames.items():
        out = partition / f"{table}.parquet"
        df.write_parquet(str(out), compression="zstd")
        counts[table] = df.height
        log.info("Staged raw %s: %d rows -> %s", table, df.height, out)
    (partition / "_counts.json").write_text(
        json.dumps(counts, indent=2), encoding="utf-8"
    )
    return partition


def generate_sample_data(
    n_customers: int = 30,
    n_items: int = 25,
    n_transactions: int = 120,
    seed: int = 42,
    run_at: datetime | None = None,
    include_bad_rows: bool = True,
) -> dict[str, pl.DataFrame]:
    """Deterministic single-store sample mimicking the Prisma schema (no DB)."""
    rng = random.Random(seed)
    run_at = run_at or datetime.now(timezone.utc)
    base = (run_at - timedelta(days=90)).date()

    profiles = [
        {
            "id": f"cust-{i:03d}",
            "name": f"Customer {i}",
            "phone": f"+63917{i:07d}",
            "address": "Balayan, Batangas",
            "preferences": rng.choice(C.CATEGORIES + [""]),
            "createdAt": datetime.combine(
                base + timedelta(days=i % 90), datetime.min.time(), tzinfo=timezone.utc
            ),
            "updatedAt": datetime.combine(
                base + timedelta(days=i % 90), datetime.min.time(), tzinfo=timezone.utc
            ) + timedelta(hours=1),
        }
        for i in range(1, n_customers + 1)
    ]

    items = []
    for i in range(1, n_items + 1):
        price = float(rng.choice([1500, 2000, 2500, 3000, 3500, 5000]))
        on_sale = rng.random() < 0.3
        discount = float(rng.choice([0, 10, 15, 20])) if on_sale else 0.0
        sale_price = round(price * 0.85, 2) if on_sale and rng.random() < 0.4 else None
        sale_ends = None
        if on_sale:
            sale_ends = datetime.combine(
                base + timedelta(days=rng.randint(0, 120)), datetime.min.time()
            )
        items.append({
            "id": f"item-{i:03d}",
            "name": f"{rng.choice(C.CATEGORIES)} #{i}",
            "category": C.CATEGORIES[(i - 1) % len(C.CATEGORIES)],
            "price": price,
            "status": "Available",
            "onSale": on_sale,
            "discountPct": discount,
            "salePrice": sale_price,
            "saleEndsAt": sale_ends,
            "createdAt": datetime.combine(
                base + timedelta(days=i % 90), datetime.min.time(), tzinfo=timezone.utc
            ),
            "updatedAt": datetime.combine(
                base + timedelta(days=i % 90), datetime.min.time(), tzinfo=timezone.utc
            ) + timedelta(hours=1),
        })

    transactions, payments = [], []
    pay_id = 1
    for n in range(1, n_transactions + 1):
        cust = rng.choice(profiles)["id"]
        item = rng.choice(items)
        rental = base + timedelta(days=rng.randint(0, 89))
        due = rental + timedelta(days=rng.randint(1, 7))
        status = rng.choice(C.STATUSES)
        returned = (
            rental + timedelta(days=rng.randint(1, 9)) if status == "Returned" else None
        )
        fee = round(item["price"] * rng.choice([1.0, 1.0, 1.0, 0.9]), 2)
        down = round(fee * 0.5, 2)
        paid = fee if status == "Returned" else (down if status != "Reserved" else 0.0)
        created = datetime.combine(rental, datetime.min.time(), tzinfo=timezone.utc)
        txn_id = f"txn-{n:04d}"
        transactions.append({
            "id": txn_id,
            "customerId": cust,
            "itemId": item["id"],
            "rentalDate": rental,
            "dueDate": due,
            "returnDate": returned,
            "status": status,
            "rentalFee": fee,
            "downpayment": down,
            "totalPaid": paid,
            "balance": round(fee - paid, 2),
            "createdAt": created,
            "updatedAt": created + timedelta(hours=1),
        })
        n_pays = 2 if status == "Returned" and rng.random() < 0.5 else 1
        if paid <= 0:
            amounts: list[float] = []
        elif n_pays == 2 and paid > down:
            amounts = [down, round(paid - down, 2)]
        else:
            amounts = [paid]
        for amt in amounts:
            if amt <= 0:
                continue
            payments.append({
                "id": f"pay-{pay_id:04d}",
                "transactionId": txn_id,
                "amount": amt,
                "method": rng.choice(["cash", "gcash", "cash"]),
                "status": "confirmed",
                "createdAt": created + timedelta(hours=1),
            })
            pay_id += 1

    if include_bad_rows:
        # Kept in bronze for audit; flagged in a later stage, never cleaned here.
        bad_fee = dict(transactions[0])
        bad_fee["id"] = "txn-BAD1"
        bad_fee["rentalFee"] = -500.0
        transactions.append(bad_fee)
        bad_due = dict(transactions[1])
        bad_due["id"] = "txn-BAD2"
        bad_due["dueDate"] = bad_due["rentalDate"] - timedelta(days=3)
        transactions.append(bad_due)

    return {
        "profiles": pl.from_dicts(profiles),
        "inventory_items": pl.from_dicts(items),
        "transactions": pl.from_dicts(transactions),
        "payments": pl.from_dicts(payments),
    }
