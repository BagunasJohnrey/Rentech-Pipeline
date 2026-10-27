"""RenTech Stage 2: Data Transformation (Parquet output).

Reads bronze Parquet from Stage 1, applies cleaning/standardization/business
rules, and writes silver Parquet files under data/transformed/dt=YYYY-MM-DD/.
"""
from __future__ import annotations

import json
import logging
import hashlib
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from .config import DEFAULT_DATA_DIR, CATEGORIES

logger = logging.getLogger("rentech.transform")
RULES_VERSION = "RULES-1.0.0"
UTC = timezone.utc

def read_bronze_partition(data_dir: Path, run_date: date) -> dict[str, pl.DataFrame]:
    partition_dir = data_dir / "raw" / f"dt={run_date.isoformat()}"
    if not partition_dir.exists():
        raise FileNotFoundError(f"Bronze partition not found: {partition_dir}")

    counts_file = partition_dir / "_counts.json"
    expected_counts = {}
    if counts_file.exists():
        expected_counts = json.loads(counts_file.read_text())

    frames = {}
    for table in ["profiles", "inventory_items", "transactions", "payments"]:
        file_path = partition_dir / f"{table}.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"Missing bronze file: {file_path}")
        df = pl.read_parquet(file_path)
        expected = expected_counts.get(table)
        if expected is not None and expected != df.height:
            raise ValueError(
                f"Row count mismatch for {table}: expected {expected}, got {df.height}"
            )
        frames[table] = df
        logger.info("Read bronze %s: %d rows", table, df.height)
    return frames

def xxhash64_positive(key: str) -> int:
    h = hashlib.shake_256(key.encode()).digest(8)
    return int.from_bytes(h, "little", signed=False) & 0x7FFFFFFFFFFFFFFF

def utc_to_manila_date(ts: Any) -> date | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return ts.astimezone(timezone(timedelta(hours=8))).date()
    if isinstance(ts, date):
        return ts
    return None

def normalize_name(name: Any) -> tuple[str | None, bool]:
    if name is None:
        return None, False
    s = str(name).strip()
    if not s:
        return None, False
    return " ".join(s.split()).title(), True

def normalize_phone(phone: Any) -> tuple[str | None, bool]:
    if phone is None:
        return None, False
    s = str(phone).strip()
    if not s:
        return None, False
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 10 and digits.startswith("9"):
        return f"+63{digits}", True
    if len(digits) == 11 and digits.startswith("639"):
        return f"+{digits}", True
    if len(digits) == 12 and digits.startswith("639"):
        return f"+{digits}", True
    return None, False

def normalize_address(address: Any) -> tuple[str | None, bool]:
    if address is None:
        return "UNKNOWN", False
    s = str(address).strip()
    if not s:
        return "UNKNOWN", False
    return " ".join(s.split()), True

def parse_preferences(prefs: Any) -> tuple[list[str], int]:
    if prefs is None:
        return [], 0
    if isinstance(prefs, list):
        items = [str(p).strip() for p in prefs if str(p).strip()]
        return items, len(items)
    if isinstance(prefs, str):
        items = [p.strip() for p in prefs.split(",") if p.strip()]
        return items, len(items)
    return [], 0

def standardize_category(cat: Any) -> tuple[str, str | None]:
    if cat is None:
        return "Other", None
    s = str(cat).strip()
    if not s:
        return "Other", None
    for c in CATEGORIES:
        if c.lower() == s.lower():
            return c, s
    return "Other", s

def standardize_payment_status(status: Any) -> str:
    if status is None:
        return "UNKNOWN"
    s = str(status).strip().lower()
    if s in ("completed", "succeeded", "confirmed"):
        return "COMPLETED"
    if s in ("pending", "processing"):
        return "PENDING"
    if s in ("failed", "declined", "cancelled"):
        return "FAILED"
    if s in ("refunded", "reversed"):
        return "REFUNDED"
    return "UNKNOWN"

def standardize_payment_method(method: Any) -> str:
    if method is None:
        return "UNKNOWN"
    s = str(method).strip().lower()
    if s in ("cash", "cash on delivery", "cod"):
        return "CASH"
    if s in ("gcash", "g cash"):
        return "GCASH"
    if s in ("maya", "paymaya"):
        return "MAYA"
    if s in ("card", "credit", "debit", "visa", "mastercard"):
        return "CARD"
    if s in ("bank", "transfer", "bank transfer"):
        return "BANK_TRANSFER"
    return "OTHER"

def standardize_transaction_status(status: Any) -> str:
    if status is None:
        return "UNKNOWN"
    s = str(status).strip().title()
    if s in ("Active", "Ongoing", "Rented"):
        return "ACTIVE"
    if s in ("Returned", "Completed", "Done"):
        return "RETURNED"
    if s in ("Overdue", "Late"):
        return "OVERDUE"
    if s in ("Reserved", "Booked", "Pending"):
        return "RESERVED"
    if s in ("Cancelled", "Canceled", "Void"):
        return "CANCELLED"
    return "UNKNOWN"

def standardize_item_status(status: Any) -> str:
    if status is None:
        return "UNKNOWN"
    s = str(status).strip().title()
    if s in ("Available", "In Stock", "Active"):
        return "AVAILABLE"
    if s in ("Rented", "Out", "On Rent"):
        return "RENTED"
    if s in ("Maintenance", "Repair", "Cleaning"):
        return "MAINTENANCE"
    if s in ("Deleted", "Removed", "Archived"):
        return "DELETED"
    return "UNKNOWN"