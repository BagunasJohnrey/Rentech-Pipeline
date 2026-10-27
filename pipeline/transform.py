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

def _to_utc(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    exprs = []
    for column in columns:
        if column not in df.columns:
            continue
        dtype = df.schema[column]
        if isinstance(dtype, pl.Datetime):
            if dtype.time_zone is not None:
                exprs.append(pl.col(column).dt.convert_time_zone("UTC"))
            else:
                exprs.append(pl.col(column).dt.replace_time_zone("UTC"))
    return df.with_columns(exprs) if exprs else df

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

QUARANTINE_SCHEMA = {
    "quarantine_id": pl.Int64,
    "source_table": pl.String,
    "source_id": pl.String,
    "rule_id": pl.String,
    "reason_code": pl.String,
    "reason_detail": pl.String,
    "raw_record": pl.String,
    "detected_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "transformation_run_id": pl.String,
    "batch_id": pl.String,
}

def clean_transactions(transactions: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    quarantine_rows = []
    clean_rows = []

    for row in transactions.iter_rows(named=True):
        tid = row["id"]
        rental_date = utc_to_manila_date(row.get("rentalDate"))
        return_date = utc_to_manila_date(row.get("returnDate"))

        if return_date and rental_date and return_date < rental_date:
            quarantine_rows.append({
                "quarantine_id": xxhash64_positive(f"transactions:{tid}:C-11"),
                "source_table": "transactions",
                "source_id": tid,
                "rule_id": "C-11",
                "reason_code": "INVALID_RETURN_DATE",
                "reason_detail": f"returnDate {return_date} < rentalDate {rental_date}",
                "raw_record": json.dumps({k: str(v) for k, v in row.items()}),
                "detected_at": datetime.now(UTC),
                "transformation_run_id": "",
                "batch_id": "",
            })
            continue
        clean_rows.append(row)

    grouped: dict[tuple, list] = {}
    for row in clean_rows:
        key = (row["customerId"], row["itemId"], utc_to_manila_date(row.get("rentalDate")))
        grouped.setdefault(key, []).append(row)

    kept = []
    for key, group in grouped.items():
        if len(group) == 1:
            kept.append(group[0])
            continue
        group.sort(
            key=lambda r: (
                r.get("updatedAt") or "",
                r.get("createdAt") or "",
                r.get("id") or "",
            ),
            reverse=True,
        )
        kept.append(group[0])
        for duplicate in group[1:]:
            quarantine_rows.append({
                "quarantine_id": xxhash64_positive(f"transactions:{duplicate['id']}:C-12"),
                "source_table": "transactions",
                "source_id": duplicate["id"],
                "rule_id": "C-12",
                "reason_code": "DUPLICATE_TRANSACTION",
                "reason_detail": f"Duplicate natural key {key}, older updatedAt",
                "raw_record": json.dumps({k: str(v) for k, v in duplicate.items()}),
                "detected_at": datetime.now(UTC),
                "transformation_run_id": "",
                "batch_id": "",
            })

    clean_df = pl.DataFrame(kept) if kept else transactions.head(0)
    quarantine_df = (
        pl.DataFrame(quarantine_rows, schema=QUARANTINE_SCHEMA)
        if quarantine_rows else pl.DataFrame(schema=QUARANTINE_SCHEMA)
    )
    return clean_df, quarantine_df

def clean_payments(
    payments: pl.DataFrame, valid_transaction_ids: set[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    quarantine_rows = []
    clean_rows = []
    for row in payments.iter_rows(named=True):
        if row["transactionId"] not in valid_transaction_ids:
            quarantine_rows.append({
                "quarantine_id": xxhash64_positive(f"payments:{row['id']}:C-16"),
                "source_table": "payments",
                "source_id": row["id"],
                "rule_id": "C-16",
                "reason_code": "INVALID_PARENT_TRANSACTION",
                "reason_detail": f"Parent transaction {row['transactionId']} not found or quarantined",
                "raw_record": json.dumps({k: str(v) for k, v in row.items()}),
                "detected_at": datetime.now(UTC),
                "transformation_run_id": "",
                "batch_id": "",
            })
            continue
        clean_rows.append(row)
    clean_df = pl.DataFrame(clean_rows) if clean_rows else payments.head(0)
    quarantine_df = (
        pl.DataFrame(quarantine_rows, schema=QUARANTINE_SCHEMA)
        if quarantine_rows else pl.DataFrame(schema=QUARANTINE_SCHEMA)
    )
    return clean_df, quarantine_df

def build_dim_date(min_date: date, max_date: date) -> pl.DataFrame:
    rows = []
    current = min_date
    while current <= max_date:
        _, iso_week, iso_day = current.isocalendar()
        rows.append({
            "date_key": int(current.strftime("%Y%m%d")),
            "date": current,
            "year": current.year,
            "quarter": (current.month - 1) // 3 + 1,
            "month": current.month,
            "month_name": current.strftime("%B"),
            "day": current.day,
            "day_of_week": iso_day,
            "day_name": current.strftime("%A"),
            "week_of_year": iso_week,
            "is_weekend": iso_day >= 6,
            "is_holiday": False,
        })
        current += timedelta(days=1)
    return pl.DataFrame(rows)

def build_dim_customer(profiles: pl.DataFrame) -> pl.DataFrame:
    rows = []
    name_phone_counts: dict[tuple[str, str], int] = {}
    for row in profiles.iter_rows(named=True):
        name_clean, name_valid = normalize_name(row.get("name"))
        phone_clean, phone_valid = normalize_phone(row.get("phone"))
        addr_clean, addr_valid = normalize_address(row.get("address"))
        prefs_list, pref_count = parse_preferences(row.get("preferences"))
        key = (name_clean or "", phone_clean or "")
        name_phone_counts[key] = name_phone_counts.get(key, 0) + 1
        rows.append({
            "customer_key": xxhash64_positive(row["id"]),
            "customer_id": row["id"],
            "name": name_clean,
            "name_valid": name_valid,
            "phone": phone_clean,
            "phone_valid": phone_valid,
            "has_phone": phone_clean is not None,
            "address": addr_clean,
            "address_valid": addr_valid,
            "preferences": prefs_list,
            "preference_count": pref_count,
            "potential_duplicate_customer": False,
            "created_at": row.get("createdAt"),
            "updated_at": row.get("updatedAt"),
            "is_current": True,
        })
    for r in rows:
        r["potential_duplicate_customer"] = name_phone_counts[
            (r["name"] or "", r["phone"] or "")
        ] > 1
    df = pl.DataFrame(rows)
    df = df.with_columns(pl.col("preferences").cast(pl.List(pl.String)))
    return _to_utc(df, ["created_at", "updated_at"])

def build_dim_item(items: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for row in items.iter_rows(named=True):
        name_clean = " ".join((row.get("name") or "").strip().split()).title()
        category, source_category = standardize_category(row.get("category"))
        price = float(row.get("price") or 0)
        on_sale = bool(row.get("onSale"))
        discount_pct = float(row.get("discountPct") or 0)
        inconsistent = (not on_sale) and discount_pct > 0
        effective_price = (
            round(price * (1 - discount_pct / 100), 2)
            if on_sale and not inconsistent else price
        )
        status = standardize_item_status(row.get("status"))
        rows.append({
            "item_key": xxhash64_positive(str(row["id"])),
            "item_id": str(row["id"]),
            "name": name_clean,
            "category": category,
            "source_category": source_category,
            "price": price,
            "effective_price": effective_price,
            "on_sale": on_sale,
            "discount_pct": 0.0 if inconsistent else discount_pct,
            "sale_price": float(row["salePrice"]) if row.get("salePrice") is not None else None,
            "sale_ends_at": utc_to_manila_date(row.get("saleEndsAt")),
            "status": status,
            "is_deleted": status == "DELETED",
            "is_test_item": category == "Other" and (
                (source_category or "").lower() == "test" or "test" in name_clean.lower()
            ),
            "sale_flag_inconsistent": inconsistent,
            "created_at": row.get("createdAt"),
            "updated_at": row.get("updatedAt"),
            "is_current": True,
        })
    df = pl.DataFrame(rows)
    df = df.with_columns([
        pl.col("sale_ends_at").cast(pl.Date),
        pl.col("sale_price").cast(pl.Float64),
    ])
    return _to_utc(df, ["created_at", "updated_at"])