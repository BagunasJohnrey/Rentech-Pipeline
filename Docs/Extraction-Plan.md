# Lab 3 — Stage 1: Data Extraction Documentation

RenTech: Rental Management System with Demand Forecasting for Mylene's Boutique (Balayan, Batangas)

## Objective

The goal of this stage is to extract data from an identified source system and prepare a documented, traceable, and validated dataset for the next stage of the data pipeline.

Planned pipeline name: rentech-nightly-rental-mart. Planned run: nightly batch, once per day, not live.

---

## 1. Data Source and Extraction Specification

### 1.1 Source System

- Name and purpose of the source system.
- Source: RenTech shop system for Mylene's Boutique, a single store in Balayan, Batangas. Records clothes for rent (gowns, barongs, suits, costumes, Filipiniana), customer bookings, and payments. Purpose is daily and monthly rental reporting and demand forecasting.

### 1.2 Source Database or File

- Database management system: PostgreSQL.
- Platform: Supabase, schema public.
- File format, if applicable: planned output will be Parquet files in data/raw/dt=YYYY-MM-DD/, one dated folder per run with 4 files. No CSV, JSON, or Excel in the live process.

### 1.3 Extraction Method

- Full or incremental: planned as incremental extraction.
- Field and logic for extraction window: each table has created_at and updated_at. Last copy time will be saved in data/state/last_run.json. Tables with updated_at will use WHERE GREATEST(created_at, updated_at) > :last_run (transactions, inventory_items, profiles). payments has no updated_at, so will use WHERE created_at > :last_run.
- How data will be extracted: read-only queries for new or changed rows only. First run with no saved time will copy everything. After that, only newer rows.
- Extraction tool, script, or technology: planned Python scripts with psycopg3 + SQLAlchemy, pooled, SSL required, DATABASE_URL preferred with DIRECT_URL fallback, plus file-only demo mode that never touches the live database.
- Extraction schedule: planned daily at 00:00 Asia/Manila, plus manual run by date for demos and missed days.

### 1.4 Extraction Scope

- Data that will be extracted: only columns needed for sales and forecast.
- Relevant tables, files, or date ranges: public.profiles, public.inventory_items, public.transactions, public.payments. Date range is everything newer than the saved watermark. Cancelled bookings and NULLs will be retained in raw for audit.

### 1.5 Source Limitations and Assumptions

- Read-only on shop tables; write-only on final mart_ tables.
- payments has no updated_at, so late fixes to old payments can be missed. Planned weekly full re-check of payments.
- Dates are timezone-naive. One date style must be used to avoid skipped rows.
- No internet, wrong password, or asleep database will block the run. Plan is to stop safely, keep old checkpoint, and retry.
- Schema changes (renamed or added columns) will stop the run with an error. Column list and this doc must be updated together.
- Restricted access can block reads. Read access on the 4 tables will be required.
- Small volume (hundreds of rows). One computer, nightly batch is enough.

---

## 2. Source Tables and Column Specification

Only columns relevant to the pipeline are listed. Every row of the source table is not copied here.

### profiles

- Name: public.profiles
- Purpose: customer and staff reference, to know who rented.
- Important columns and types: id TEXT, name TEXT, phone TEXT NULL, address TEXT NULL, preferences TEXT NULL, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ.
- Primary key: id.
- Relationships: referenced by transactions.customer_id.
- Reason for inclusion: without it, bookings cannot be linked to customers.

### inventory_items

- Name: public.inventory_items
- Purpose: clothes reference, to know what was rented.
- Important columns and types: id INTEGER, name TEXT, category TEXT, price NUMERIC, status TEXT, on_sale BOOLEAN, discount_pct NUMERIC NULL, sale_price NUMERIC NULL, sale_ends_at TIMESTAMPTZ NULL, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ.
- Primary key: id.
- Relationships: referenced by transactions.item_id.
- Reason for inclusion: category and sale info are needed for category sales and correct revenue.

### transactions

- Name: public.transactions
- Purpose: rental bookings, the main fact source.
- Important columns and types: id TEXT, customer_id TEXT NULL, item_id INTEGER NULL, rental_date DATE NULL, due_date DATE NULL, return_date DATE NULL, status TEXT, rental_fee NUMERIC, downpayment NUMERIC, total_paid NUMERIC, balance NUMERIC, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ.
- Primary key: id.
- Relationships: customer_id references profiles.id, item_id references inventory_items.id.
- Reason for inclusion: all revenue counts and demand reports come from this table. Cancelled rows stay in raw, removed from sales later.

### payments

- Name: public.payments
- Purpose: money log to double-check bookings.
- Important columns and types: id TEXT, transaction_id TEXT, amount NUMERIC, method TEXT, status TEXT, created_at TIMESTAMPTZ.
- Primary key: id.
- Relationships: transaction_id references transactions.id.
- Reason for inclusion: money in payments must match money in bookings.

---

## 3. Extraction Validation and Data Quality Checks

Scope limitation: focus on extraction-level validation. Data cleaning, standardization, and business transformations will be documented in the Transformation Stage.

| Check Name | Target | Purpose | Validation Criteria |
| ---------- | ------ | ------- | ------------------- |
| Source connection | PostgreSQL connection | Confirm source is reachable. | PASS: read-only connection succeeds within timeout with SSL. FAIL: connection or auth fails; stop run, keep old checkpoint. |
| Required tables and columns exist | Four tables and explicit column list | Detect missing or changed schema. | PASS: all required tables and columns exist with compatible types. FAIL: any required table or column missing or incompatible. |
| Required identifiers present | id in each table, links customer_id, item_id, transaction_id | Ensure records can be linked. | PASS: identifiers present, zero blanks. FAIL: any required identifier NULL. |
| Primary key uniqueness | Table id fields | Detect duplicates that double-count sales. | PASS: all values unique. FAIL: duplicates found; log for later cleanup. |
| Record count and completeness | Source vs staged output per table | Confirm all rows were copied. | PASS: source and output counts match. FAIL: counts differ or output incomplete. WARNING: zero rows or sudden big drop. |
| Date availability | rental_date, created_at, updated_at | Identify dates needed for watermark and grouping. | PASS: required dates present. WARNING: nullable dates NULL but preserved. FAIL: required date missing or invalid. |
| Extraction errors or interruptions | Run and four output files | Detect failed or partial runs. | PASS: all reads and writes complete, 4 files exist with correct counts. FAIL: any query or write fails; delete partial date folder, do not hand over. |

Required considerations covered: source connection and accessibility, required tables and columns exist, required identifiers present, primary key uniqueness, record count and completeness, extraction errors or interruptions.

---

## 4. Extraction Metadata and Log Specification

| Field | Purpose | Example value or format |
| ----- | ------- | ----------------------- |
| pipeline_run_id | Identifies and links one run. | dt=2026-09-16, or UUID on retry |
| table_name | Table or file name. | public.transactions, file transactions.parquet |
| extraction_started_at | Extraction start timestamp. | ISO 8601, e.g. 2026-09-16 17:27:21+08:00 |
| extraction_ended_at | Extraction end timestamp. | ISO 8601, e.g. 2026-09-16 17:27:24+08:00 |
| extraction_status | Run result. | SUCCESS or FAILED |
| records_read | Number of records read. | transactions: 122 |
| records_extracted | Number of records extracted to staging. | transactions: 122 saved |
| records_rejected | Number not passed on, if applicable. | Normally 0; bad rows handled next stage |
| extraction_window | Window or data range. | newer than 2026-09-15 evening |
| validation_results | Validation outcomes. | 4/4 tables PASS |
| error_code | Error category, if applicable. | SOURCE_TIMEOUT or NULL |
| error_message | Error detail, if applicable. | Short diagnostic or NULL |
| extraction_duration | Extraction duration. | Seconds, e.g. 3s demo |
| file_locations | Where proof is stored. | data/raw/dt=2026-09-16/*.parquet |
| log_file | Per-run log path. | data/logs/pipeline_2026-09-16.log |

How metadata and logs will be used:

- Monitoring: check each morning that a SUCCESS log exists with 4 saved files.
- Troubleshooting: log shows which table failed and why.
- Auditing: dated folders prove what the database gave on each date.
- Recovery: checkpoint moves forward only after full success. Failed dates repeat the same window, so no data is skipped. Retries use the same window with a new log entry.

---

## 5. Log Retention and Access

- Log retention period and justification: at least 12 months for logs, raw folders, checkpoints, and quarantine files. Covers one school year, seasonal peaks, and defense re-checks.
- Storage location: project data/logs/, data/raw/, data/state/, data/quarantine/. Zip backup before submission or defense.
- Access permissions: team members and panel can read. Only the runner can write. Passwords never in logs. .env never shared.
- Archiving requirements: logs older than 90 days zipped monthly. Old raw data may move to archive but must stay openable.
- Deletion rules and responsible process: logs must not be deleted before the retention period or while needed for troubleshooting, auditing, or recovery. Deletion needs lead approval and proof that final reports still exist. An authorized operator performs and records deletion.
- Operational logs vs audit records: daily run logs are operational. Dated raw folders and checkpoints are audit records and follow the same 12-month minimum; longer school retention applies to audit records separately if required.

---

## 6. Extraction Data Contract and Naming Convention

### 1. Required Schema

Expected output per run: four Parquet files in data/raw/dt=YYYY-MM-DD/:

- profiles.parquet, inventory_items.parquet, transactions.parquet, payments.parquet

Columns as listed in Section 2, with correct types. Required identifiers: id in every file, plus customerId, itemId, transactionId links where applicable. Constraints: main IDs not blank and not repeated in the same file; link fields not blank except where NULL is explicitly allowed and preserved for later handling.

### 2. Naming Convention

- Column naming format: live source uses snake_case. Planned pipeline canonical names use camelCase to match shop app code, with a justified mapping defined below.
- Abbreviations and consistency: Pct means percent (discountPct), At means datetime (saleEndsAt, createdAt). New fields must copy the app spelling.
- Primary and foreign keys: primary keys named id. Foreign keys named customerId, itemId, transactionId in canonical form.

### 3. Schema Consistency

Unexpected changes to column names, data types, or required fields will stop the run. Partial files will be removed. The log will name the problem column. The column list and this contract will be updated together before re-running the missed date. New source columns will be ignored until agreed. New status values will be saved as-is in raw; the next stage will decide handling with a log note.

Example mapping format required by the activity:

| Source Column | Standardized Column | Source Type | Target Type | Mapping Type |
| ------------- | ------------------- | ----------- | ----------- | ------------ |
| cust_id | customer_id | INTEGER | INTEGER | Rename |
| cust_name | customer_name | VARCHAR | VARCHAR | Rename |
| created | created_at | DATETIME | TIMESTAMP | Type mapping |

RenTech source-to-standardized mappings:

| Source Column | Standardized Column | Source Type | Target Type | Mapping Type |
| ------------- | ------------------- | ----------- | ----------- | ------------ |
| customer_id | customerId | TEXT | TEXT | Rename |
| item_id | itemId | INTEGER | INTEGER | Rename |
| transaction_id | transactionId | TEXT | TEXT | Rename |
| rental_date | rentalDate | DATE | DATE | Rename |
| rental_fee | rentalFee | NUMERIC | NUMERIC | Rename |
| total_paid | totalPaid | NUMERIC | NUMERIC | Rename |
| created_at | createdAt | TIMESTAMPTZ | TIMESTAMPTZ | Rename |
| updated_at | updatedAt | TIMESTAMPTZ | TIMESTAMPTZ | Rename |
| on_sale | onSale | BOOLEAN | BOOLEAN | Rename |
| discount_pct | discountPct | NUMERIC | NUMERIC | Rename |
| sale_price | salePrice | NUMERIC | NUMERIC | Rename |

If the standardized field name is customer_id, it must be used consistently in the documented pipeline unless a justified mapping is defined. Here the justified mapping is customer_id to customerId to match shop app code.

---

## 7. Extraction Acceptance and Handover Rules

| Validation condition | Acceptance criteria | Pipeline action | Automatic or manual decision, responsible |
| -------------------- | ------------------- | --------------- | ----------------------------------------- |
| All critical checks pass | Required tables and columns exist; identifiers present and unique; counts match; files complete; watermark safe. | Mark APPROVED and continue to Transformation. | Automatic. |
| Non-critical warnings remain | Duplicates logged, zero new rows on quiet day, or count change, but extraction is complete. | Hold for review; preserve data unchanged. | MANUAL REVIEW by lead or data owner. |
| A critical check fails | Source unavailable, identifiers invalid, counts mismatch, or extraction incomplete. | Mark REJECTED; delete partial date folder, keep old checkpoint, stop handover. Retry transient failures on next run. | Automatic stop; developer resolves the failure. |

### Handover

Planned handover to Transformation: 4 staged datasets with row counts, run ID and date, mode (demo or live), extraction window, validation results, errors, checkpoint value, log location, and acceptance status. Next stage will acknowledge run ID and status.

Required decisions:

1. Automatic Approval: all critical validation checks pass.
2. Manual Review: non-critical warnings or unexpected record counts.
3. Rejection or Failure: critical fail stops or quarantines partial output and retries later; cleaning is not done here.
4. Handover: output, metadata, and validation results passed as listed above.

Decision rules:

| Overall Condition | Acceptance Decision | Pipeline Action |
| ----------------- | ------------------- | --------------- |
| All required tables PASS | APPROVED | Proceed to Transformation |
| One or more non-critical WARNING | MANUAL REVIEW | Hold pipeline |
| One or more critical FAIL | REJECTED | Stop / retry / quarantine |

Validation summary:

| Validation Summary | Result |
| ------------------ | ------ |
| profiles table validation | PASS, 30 rows |
| inventory_items table validation | PASS, 25 rows |
| transactions table validation | PASS, 122 rows |
| payments table validation | PASS, 132 rows |
| Overall Extraction | APPROVED |

---

## 8. Common Extraction Problems and Mitigation

| Problem description | Possible cause | Impact on extraction | Proposed solution or mitigation |
| ------------------- | -------------- | -------------------- | ------------------------------- |
| Cannot connect or timeout | Wrong address, wrong password, no internet, asleep database | No data copied | Verify DATABASE_URL, SSL, retry same window, log error |
| Checkpoint file missing or broken | Deleted file or crash during write | Next run copies everything once, slow but safe | Warn, full copy once, restore from backup if available |
| Old payment fix missed | payments has no updated_at | Money totals drift | Weekly full re-copy of payments; next stage compares bookings vs payments |
| Column or table not found after shop update | Shop added, removed, or renamed a field | Copy stops | Update column list and contract together, re-run missed date |
| No read permission | Changed grants | Copy stops like missing table | Request read on 4 tables only, retry after grant |
| New status value appears | Shop adds a booking state | Next stage may mis-group it | Save as-is in raw; next stage adds explicit rule with log note |
| Half-saved file | Brownout or full disk during save | Short file gives wrong totals | Require 4 full files; delete broken date folder, re-run same date |
| Date mix-up or repeated rows | Timezone confusion or clock jump | Repeats safe, skips bad | Use one date style; move checkpoint only after full success |
| Too many connections | Two runs at the same time | Slow or refused connection | Run one copy at a time; manual runs must not overlap schedule |

Problems are specific to Supabase Postgres, watermark files, and Parquet staging. Solutions are technically reasonable for a single-machine nightly batch.

---

## Expected Output

The documentation is consistent with the planned extraction process and provides sufficient information for a team member to understand, validate, monitor, and maintain the extraction stage.

The extracted data will be validated and packaged for handover to the next stage: Transformation. Data cleaning, standardization, and business transformations will not be performed unless explicitly required by the extraction process. Planned output is four validated datasets with run metadata, validation results, and acceptance status.

---