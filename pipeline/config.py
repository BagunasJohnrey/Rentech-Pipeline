"""Single source of truth: tables, column maps, watermark SQL, settings.

Physical (Supabase) columns are snake_case; canonical pipeline names are
camelCase (Prisma parity) and are what bronze Parquet holds. Every SELECT
uses an explicit column list (never SELECT *) so a renamed or dropped
column fails fast instead of shifting positions silently.

TABLES maps each source table to:
  - columns: {canonical: physical}
  - incremental_col: canonical watermark field
  - watermark_sql / watermark_order: raw SQL fragment (physical names)

payments has no updated_at on the live DB, so it can only window on
created_at; the other three use GREATEST(created_at, updated_at) so edits
and returns (which bump updated_at only) are re-extracted.
"""
from __future__ import annotations

import os
from pathlib import Path

DEFAULT_DATA_DIR = Path(os.getenv("RENTECH_DATA_DIR", "data"))
STATE_FILE = "state/last_run.json"

# Stage 2 is file-only: no target database and no Stage 2 credentials.
CORE_TABLES = [
    "dim_customer",
    "dim_item",
    "dim_date",
    "fact_sales",
    "agg_daily_category_sales",
]

# Match the boutique catalog + Postgres enums (case-sensitive).
CATEGORIES = ["Gown", "Barong", "Suit", "Costume", "Filipiniana"]
# Weighted like the live TEST seed; Cancelled excluded downstream.
STATUSES = ["Returned", "Returned", "Returned", "Active", "Overdue", "Reserved"]

TABLES: dict[str, dict] = {
    "transactions": {
        "columns": {
            "id": "id",
            "customerId": "customer_id",
            "itemId": "item_id",
            "rentalDate": "rental_date",
            "dueDate": "due_date",
            "returnDate": "return_date",
            "status": "status",
            "rentalFee": "rental_fee",
            "downpayment": "downpayment",
            "totalPaid": "total_paid",
            "balance": "balance",
            "createdAt": "created_at",
            "updatedAt": "updated_at",
        },
        "incremental_col": "createdAt",
        # Catch edits too (Active -> Returned changes updated_at, not created_at).
        "watermark_sql": 'GREATEST("created_at", "updated_at") > :last_run',
        "watermark_order": 'GREATEST("created_at", "updated_at")',
    },
    "payments": {
        "columns": {
            "id": "id",
            "transactionId": "transaction_id",
            "amount": "amount",
            "method": "method",
            "status": "status",
            "createdAt": "created_at",
        },
        "incremental_col": "createdAt",
        # Live payments table has NO updated_at (see plan 1.5 L2).
        "watermark_sql": '"created_at" > :last_run',
        "watermark_order": '"created_at"',
    },
    "inventory_items": {
        "columns": {
            "id": "id",
            "name": "name",
            "category": "category",
            "price": "price",
            "status": "status",
            "onSale": "on_sale",
            "discountPct": "discount_pct",
            "salePrice": "sale_price",
            "saleEndsAt": "sale_ends_at",
            "createdAt": "created_at",
            "updatedAt": "updated_at",
        },
        "incremental_col": "createdAt",
        "watermark_sql": 'GREATEST("created_at", "updated_at") > :last_run',
        "watermark_order": 'GREATEST("created_at", "updated_at")',
    },
    "profiles": {
        "columns": {
            "id": "id",
            "name": "name",
            "phone": "phone",
            "address": "address",
            "preferences": "preferences",
            "createdAt": "created_at",
            "updatedAt": "updated_at",
        },
        "incremental_col": "createdAt",
        "watermark_sql": 'GREATEST("created_at", "updated_at") > :last_run',
        "watermark_order": 'GREATEST("created_at", "updated_at")',
    },
}

# Columns that must never be NULL in bronze (checked by V4 in validate_extract).
REQUIRED_IDS: dict[str, list[str]] = {
    "profiles": ["id"],
    "inventory_items": ["id"],
    "transactions": ["id", "customerId", "itemId"],
    "payments": ["id", "transactionId"],
}

if __name__ == "__main__":  # quick contract self-check
    assert set(TABLES) == {"transactions", "payments", "inventory_items", "profiles"}
    counts = [len(TABLES[t]["columns"]) for t in TABLES]
    assert counts == [13, 6, 11, 7], counts
    print("config contract OK:", counts)