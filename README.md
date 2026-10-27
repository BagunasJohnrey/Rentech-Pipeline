# RenTech Pipeline (Stage 1: Extraction, Stage 2: Transformation)

Stage 1 (Extraction) and Stage 2 (Transformation) of **RenTech: Rental Management System with Demand Forecasting for Mylene's Boutique** (single store, Balayan, Batangas).

Pipeline name: `rentech-nightly-rental-mart`. Batch only, nightly at 00:00 Asia/Manila. Stage 1 reads Supabase Postgres incrementally and stages dated bronze Parquet. Stage 2 reads bronze, applies cleaning/standardization/business rules, and writes transformed Parquet files to `data/transformed/dt=YYYY-MM-DD/`.

```
Scheduler -> Extract (psycopg3/SQLAlchemy) -> Stage raw Parquet -> Validate V1-V7
      -> Transform (Polars) -> Write Parquet -> Validate transformation checks
```

## Architecture Overview

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Supabase       │     │  Stage 1:        │     │  data/raw/          │
│  PostgreSQL     │────▶│  Extract         │────▶│  dt=YYYY-MM-DD/     │
│  (Source)       │     │  (Bronze)        │     │  profiles.parquet   │
└─────────────────┘     └──────────────────┘     │  inventory_items.   │
                              │                  │  transactions.parquet│
                              ▼                  │  payments.parquet   │
                       ┌──────────────────┐      │  _counts.json       │
                       │  Validate V1-V7  │      └─────────────────────┘
                       └────────┬─────────┘
                                │
                                ▼
                       ┌──────────────────┐     ┌─────────────────────┐
                       │  Stage 2:        │────▶│  data/transformed/  │
                       │  Transform       │     │  dt=YYYY-MM-DD/     │
                       │  (Silver)        │     │  dim_customer       │
                       └────────┬─────────┘     │  dim_item           │
                                │               │  dim_date           │
                                ▼               │  fact_sales         │
                       ┌──────────────────┐     │  agg_daily_category_│
                       │  Validate V1-V24 │     │  audit/             │
                       └────────┬─────────┘     │    excluded_sales   │
                                │               │    excluded_payments│
                                ▼               │  _manifest.json     │
                       ┌──────────────────┐     │  _reconciliation.   │
                       │  Quarantine/     │     │  _rules_applied.json│
                       │  Audit Output    │     │  _validation.json   │
                       └──────────────────┘     └─────────────────────┘
```

## Layout

```
pipeline/
  config.py               # TABLES map + settings (single source of truth)
  extract.py              # connection, watermark, extract, stage_raw, sample data
  validate_extract.py     # V1-V7 extraction checks only
  run_extract.py          # logging, CLI, nightly scheduler (Stage 1)
  transform.py            # cleaning, standardization, business rules (Stage 2)
  validate_transform.py   # V1-V24 transformation checks (Stage 2)
  run_transform.py        # logging, CLI, nightly scheduler (Stage 2)
data/
  raw/dt=YYYY-MM-DD/*.parquet           # bronze: 4 files per run (Stage 1 output)
  transformed/dt=YYYY-MM-DD/*.parquet   # silver: 5 tables + metadata (Stage 2 output)
  transformed/dt=YYYY-MM-DD/audit/      # excluded_sales, excluded_payments
  quarantine/transform/dt=YYYY-MM-DD/   # quarantined transactions and payments
  logs/pipeline_YYYY-MM-DD.log          # Stage 1 per-run log
  logs/transform_YYYY-MM-DD.log         # Stage 2 per-run log
  state/last_run.json                   # incremental watermark
```

## Data Model (Silver Layer)

### Dimension Tables

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `dim_customer` | `customer_key` | Customer master with normalized name, phone, address, preferences |
| `dim_item` | `item_key` | Inventory items with standardized category, pricing, sale flags |
| `dim_date` | `date_key` | Calendar dimension (YYYYMMDD), ISO week, holidays, weekends |

### Fact Table

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `fact_sales` | `sales_key` | Rental transactions with payment reconciliation, overdue tracking, balance |

### Aggregate Table

| Table | Primary Key | Description |
|-------|-------------|-------------|
| `agg_daily_category_sales` | (`date_key`, `category_key`) | Daily revenue/volume by category |

## Business Rules (Stage 2)

### Data Quality Rules (Quarantine - C-xx)
| Rule | Code | Description |
|------|------|-------------|
| Invalid return date | C-11 | `returnDate < rentalDate` → quarantine |
| Duplicate transaction | C-12 | Same `(customerId, itemId, rentalDate)` → keep latest `updatedAt` |
| Invalid parent transaction | C-16 | Payment references non-existent/quarantined transaction → quarantine |

### Business Exclusion Rules (Audit - B-xx)
| Rule | Code | Description |
|------|------|-------------|
| Cancelled transaction | B-07 | Status = CANCELLED → exclude from fact, audit with payments |

### Standardization
- **Names**: Title case, trimmed
- **Phones**: PH format (+639xxxxxxxxx)
- **Addresses**: Trimmed, "UNKNOWN" if empty
- **Categories**: Mapped to canonical list (Electronics, Fashion, etc.)
- **Payment status**: COMPLETED / PENDING / FAILED / REFUNDED / UNKNOWN
- **Payment method**: CASH / GCASH / MAYA / CARD / BANK_TRANSFER / OTHER
- **Transaction status**: ACTIVE / RETURNED / OVERDUE / RESERVED / CANCELLED / UNKNOWN
- **Item status**: AVAILABLE / RENTED / MAINTENANCE / DELETED / UNKNOWN

### Derived Metrics (fact_sales)
- `balance_due = max(rental_fee - payment_total, 0)`
- `overpayment_amount = max(payment_total - rental_fee, 0)`
- `ledger_mismatch = |source_balance - balance_due| > 0.01`
- `overdue_days` calculated from due/return dates
- `is_future_booking = rental_date > batch_date`

## Validation Checks

### Stage 1 (V1-V7)
| Check | Description |
|-------|-------------|
| V1-V4 | Each bronze file exists |
| V5-V8 | Row counts > 0 |
| V9-V12 | Row counts match `_counts.json` |
| V13 | No null primary keys |
| V14 | Watermark advances |

### Stage 2 (V1-V24)
| Check | Description |
|-------|-------------|
| V1-V5 | Silver files exist |
| V6-V10 | Row counts > 0 |
| V11-V15 | Primary key uniqueness |
| V16-V20 | Foreign key integrity |
| V21 | Payment reconciliation (fee = payment + balance - overpayment) |
| V22 | Future bookings retained |
| V23 | Business exclusion rate ≤ 15% |
| V24 | Quarantine rate ≤ 1% |

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env   # fill in DATABASE_URL for Stage 1
```

**Environment Variables:**
- `DATABASE_URL` (preferred) / `DIRECT_URL` — Stage 1 source database
- `RENTECH_DATA_DIR` — override `data/` root (default: `./data`)
- `DB_POOL_SIZE` (5), `DB_MAX_OVERFLOW` (10), `DB_SSLMODE` (require), `DB_CONNECT_TIMEOUT` (10)

Stage 2 needs no target database; it reads `data/raw/` and writes `data/transformed/`.

## Run Commands

### Stage 1: Extraction
```powershell
# File-only demo, no DB (defense demo)
python -m pipeline.run_extract --demo --date 2026-09-15

# Live nightly extract (needs DATABASE_URL)
python -m pipeline.run_extract

# Specific date
python -m pipeline.run_extract --date 2026-10-18

# Nightly scheduler (00:00 Asia/Manila)
python -m pipeline.run_extract --schedule

# Validation only
python -m pipeline.run_extract --date 2026-10-18 --validate-only
```

### Stage 2: Transformation
```powershell
# Transform bronze to silver Parquet
python -m pipeline.run_transform --date 2026-10-18

# Validation only (no re-transform)
python -m pipeline.run_transform --date 2026-10-18 --validate-only

# Nightly scheduler (00:30 Asia/Manila, after extraction)
python -m pipeline.run_transform --schedule
```

## Output Artifacts (Stage 2)

### Core Tables
- `dim_customer.parquet` — 30+ columns including normalized fields
- `dim_item.parquet` — standardized categories, effective pricing, sale flags
- `dim_date.parquet` — full calendar covering all event dates in batch
- `fact_sales.parquet` — rental transactions with full reconciliation
- `agg_daily_category_sales.parquet` — daily rollups by category

### Metadata
| File | Purpose |
|------|---------|
| `_manifest.json` | Run manifest: counts, checksums, amounts, status |
| `_reconciliation.json` | Input/output/quarantine/audit row balances |
| `_rules_applied.json` | Rules triggered with examples |
| `_validation.json` | Validation report (V1-V24) |

### Audit & Quarantine
- `audit/excluded_sales.parquet` — Cancelled transactions (B-07)
- `audit/excluded_payments.parquet` — Payments on cancelled transactions
- `quarantine/transform/dt=.../quarantine_transactions.parquet` — C-11, C-12
- `quarantine/transform/dt=.../quarantine_payments.parquet` — C-16

## Scheduling

| Stage | Cron (Asia/Manila) | Command |
|-------|-------------------|---------|
| Extract | `0 0 * * *` | `python -m pipeline.run_extract --schedule` |
| Transform | `30 0 * * *` | `python -m pipeline.run_transform --schedule` |

Both runners use APScheduler with timezone-aware scheduling. They daemonize and run forever.

## Sample Log Output

### Stage 1 (`--demo`, seed=42)
```text
2026-09-16 17:27:21,703 | INFO    | rentech.extract | === RenTech nightly extract dt=2026-09-16 (mode=demo) ===
2026-09-16 17:27:21,705 | INFO    | rentech.extract | Staged raw profiles: 30 rows -> data\raw\dt=2026-09-16\profiles.parquet
2026-09-16 17:27:21,709 | INFO    | rentech.extract | Staged raw inventory_items: 25 rows -> data\raw\dt=2026-09-16\inventory_items.parquet
2026-09-16 17:27:21,712 | INFO    | rentech.extract | Staged raw transactions: 122 rows -> data\raw\dt=2026-09-16\transactions.parquet
2026-09-16 17:27:21,713 | INFO    | rentech.extract | Staged raw payments: 132 rows -> data\raw\dt=2026-09-16\payments.parquet
2026-09-16 17:27:21,713 | INFO    | rentech.extract | Raw bronze staged: data\raw\dt=2026-09-16 (309 rows total)
2026-09-16 17:27:21,714 | INFO    | pipeline.validate_extract | All extraction checks PASS (4 tables, 309 rows)
2026-09-16 17:27:21,714 | INFO    | rentech.extract | === Extract SUCCESS dt=2026-09-16 raw=data\raw\dt=2026-09-16 rows=309 status=APPROVED log=data\logs\pipeline_2026-09-16.log ===
```

### Stage 2
```text
2026-10-27 22:08:56,811 | INFO    | rentech.transform | Read bronze profiles: 30 rows
2026-10-27 22:08:56,813 | INFO    | rentech.transform | Read bronze inventory_items: 25 rows
2026-10-27 22:08:56,816 | INFO    | rentech.transform | Read bronze transactions: 122 rows
2026-10-27 22:08:56,820 | INFO    | rentech.transform | Read bronze payments: 132 rows
2026-10-27 22:08:56,871 | INFO    | rentech.transform | Transformation complete: {'dim_customer': 30, 'dim_item': 25, 'dim_date': 119, 'fact_sales': 119, 'agg_daily_category_sales': 108, 'quarantine_quarantine_transactions': 3, 'quarantine_quarantine_payments': 5, 'audit_excluded_sales': 0, 'audit_excluded_payments': 0}
2026-10-27 22:08:56,871 | INFO    | rentech.run_transform | Output partition: data\transformed\dt=2026-09-15
2026-10-27 22:08:56,894 | INFO    | rentech.run_transform | Validation APPROVED (24/24 PASS)
```

## Constraints

- Batch only; no Pandas, no Spark/Airflow/Kafka
- Single machine execution
- Read-only on shop tables (never writes back)
- Stage 1 has no transformation logic
- All timestamps in UTC; business dates in Asia/Manila (UTC+8)

## Troubleshooting

| Issue | Resolution |
|-------|------------|
| `FileNotFoundError: Bronze partition not found` | Run Stage 1 first for the date, or check `data/raw/dt=YYYY-MM-DD/` |
| `FK fact_sales.due_date_key -> dim_date.date_key` FAIL | Ensure `dim_date` spans all `dueDate` values (fixed in transform.py) |
| Quarantine rate > 1% | Check `_rules_applied.json` for C-11/C-12/C-16 patterns |
| Business exclusion rate > 15% | Review cancelled transaction volume in source |
| Validation REJECTED | Check `_validation.json` for failing check IDs |

## Requirements

- Python 3.11+
- `polars`, `psycopg3`, `sqlalchemy`, `apscheduler`, `python-dotenv`, `pydantic`
- See `requirements.txt` for pinned versions