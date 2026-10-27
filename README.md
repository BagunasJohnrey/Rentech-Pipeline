# RenTech Pipeline (Stage 1: Extraction, Stage 2: Transformation)

Stage 1 (Extraction) of **RenTech: Rental Management System with Demand
Forecasting for Mylene's Boutique** (single store, Balayan, Batangas).

Pipeline name: `rentech-nightly-rental-mart`. Batch only, nightly at
00:00 Asia/Manila. Stage 1 reads Supabase Postgres incrementally and
stages dated bronze Parquet. Stage 2 reads bronze, applies cleaning/standardization/business rules, and writes transformed Parquet files to `data/transformed/dt=YYYY-MM-DD/`.

```
Scheduler -> Extract (psycopg3/SQLAlchemy) -> Stage raw Parquet -> Validate V1-V7
      -> Transform (Polars) -> Write Parquet -> Validate transformation checks
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

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env   # fill in DATABASE_URL for Stage 1
```

Env: `DATABASE_URL` preferred, `DIRECT_URL` fallback (Stage 1 source).
Stage 2 needs no target database; it reads `data/raw/` and writes `data/transformed/`.
Optional: `RENTECH_DATA_DIR`, `DB_POOL_SIZE` (5), `DB_MAX_OVERFLOW` (10),
`DB_SSLMODE` (require), `DB_CONNECT_TIMEOUT` (10).

## Run

```powershell
# Stage 1: File-only demo, no DB (defense demo)
python -m pipeline.run_extract --demo --date 2026-09-15

# Stage 1: Live nightly extract (needs DATABASE_URL)
python -m pipeline.run_extract

# Stage 1: Nightly scheduler (00:00 Asia/Manila)
python -m pipeline.run_extract --schedule

# Stage 2: Transform bronze to Parquet (file-only, no target DB)
python -m pipeline.run_transform --date 2026-10-18

# Stage 2: Validation only
python -m pipeline.run_transform --date 2026-10-18 --validate-only

# Stage 2: Nightly scheduler (00:30 Asia/Manila, after extraction)
python -m pipeline.run_transform --schedule
```

## Constraints

Batch only; no Pandas, no Spark/Airflow/Kafka; single machine; read-only on
shop tables (never writes back); Stage 1 has no transformation.

## Sample log output (`--demo`, seed=42)

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