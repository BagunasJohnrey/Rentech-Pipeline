# RenTech Extraction-Only Pipeline

Stage 1 (Extraction) of **RenTech: Rental Management System with Demand
Forecasting for Mylene's Boutique** (single store, Balayan, Batangas).

Pipeline name: `rentech-nightly-rental-mart`. Batch only, nightly at
00:00 Asia/Manila. This milestone reads Supabase Postgres incrementally and
stages dated bronze Parquet. Cleaning, joins, aggregation, mart load, and
forecasting are **out of scope** here.

```
Scheduler -> Extract (psycopg3/SQLAlchemy) -> Stage raw Parquet -> Validate V1-V7
```

## Layout

```
pipeline/
  config.py           # TABLES map + settings (single source of truth)
  extract.py          # connection, watermark, extract, stage_raw, sample data
  validate_extract.py # V1-V7 extraction checks only
  run_extract.py      # logging, CLI, nightly scheduler
data/
  raw/dt=YYYY-MM-DD/*.parquet   # bronze: 4 files per run
  logs/pipeline_YYYY-MM-DD.log  # per-run log
  state/last_run.json           # incremental watermark
  quarantine/                   # referenced only (no cleaning here)
```

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env   # fill in DATABASE_URL
```

Env: `DATABASE_URL` preferred, `DIRECT_URL` fallback. Optional:
`RENTECH_DATA_DIR`, `DB_POOL_SIZE` (5), `DB_MAX_OVERFLOW` (10),
`DB_SSLMODE` (require), `DB_CONNECT_TIMEOUT` (10).

## Run

```powershell
# File-only demo, no DB (defense demo)
python -m pipeline.run_extract --demo --date 2026-09-15

# Live nightly extract (needs DATABASE_URL)
python -m pipeline.run_extract

# Nightly scheduler (00:00 Asia/Manila)
python -m pipeline.run_extract --schedule
```

## Constraints

Batch only; no Pandas, no Spark/Airflow/Kafka; single machine; read-only on
shop tables (never writes back); no transformation in this stage.

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

## Members

1. Noilee Anne Pagkaliwagan - Documentation
2. Baby Joy Dayao - GitHub Issues Manager
3. John Rey Bagunas - Programmer Coder
