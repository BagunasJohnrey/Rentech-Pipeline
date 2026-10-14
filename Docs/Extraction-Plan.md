# Lab 3 — Stage 1: Data Extraction Documentation

RenTech: Rental Management System with Demand Forecasting for Mylene's Boutique (Balayan, Batangas)

## Objective

The goal of this stage is to extract data from an identified source system and prepare a documented, traceable, and validated dataset for the next stage of the data pipeline.

This document covers Extraction and extraction-level validation only. Transformation and Load are documented separately and are not part of this stage.

Planned pipeline name: rentech-nightly-rental-mart. Planned run: nightly batch, once per day, not live.

---

## 1. Data Source and Extraction Specification

### 1.1 Source System

The source system is the RenTech shop system for Mylene's Boutique, a single store in Balayan, Batangas. It records clothes for rent (gowns, barongs, suits, costumes, Filipiniana), customer bookings, and payments. The system supports daily and monthly rental reporting and demand forecasting.

### 1.2 Source Database or File

The source runs on PostgreSQL, hosted on Supabase under the public schema. The planned output is Parquet files in data/raw/dt=YYYY-MM-DD/, one dated folder per run holding 4 files. No CSV, JSON, or Excel is used in the live process.

### 1.3 Extraction Method

The method is planned as incremental extraction. It copies only new or changed rows instead of reloading the whole database each time.

The extraction window relies on each table's created_at and updated_at columns. The last copy time is saved in data/state/last_run.json. Tables that have updated_at (transactions, inventory_items, profiles) use WHERE GREATEST(created_at, updated_at) > :last_run. The payments table has no updated_at, so it uses WHERE created_at > :last_run.

Extraction reads data with read-only queries for new or changed rows only. The first run has no saved time and copies everything; later runs copy only newer rows.

The extraction tool is a planned set of Python scripts using psycopg3 and SQLAlchemy. The connection is pooled and requires SSL, preferring DATABASE_URL with DIRECT_URL as fallback.

The extraction schedule is planned for daily at 00:00 Asia/Manila, plus manual runs by date for demos and missed days.

### 1.4 Extraction Scope

- Data that will be extracted: only columns needed for sales and forecast.
- Relevant tables, files, or date ranges: public.profiles, public.inventory_items, public.transactions, public.payments. Date range is everything newer than the saved watermark. Cancelled bookings and NULLs will be retained in raw for audit.
- Filter rule: copy only rows where GREATEST(created_at, updated_at) > :last_run. payments has no updated_at, so use created_at > :last_run.
- Rule: keep cancelled bookings and NULL values in the raw copy for audit; do not delete or clean here. Cleaning is covered in the separate Transformation document.

#### 1.4.1 Needed Source Files (Tables) and Columns

| # | Source File (Table) | Purpose for Sales and Forecast | Columns Needed |
| - | ------------------- | ------------------------------ | -------------- |
| 1 | public.profiles | Know who rented; link bookings to a customer. | id, name, phone, address, preferences, created_at, updated_at |
| 2 | public.inventory_items | Know what was rented; category and sale info for correct revenue. | id, name, category, price, status, on_sale, discount_pct, sale_price, sale_ends_at, created_at, updated_at |
| 3 | public.transactions | Main fact source; all revenue counts and demand reports. | id, customer_id, item_id, rental_date, due_date, return_date, status, rental_fee, downpayment, total_paid, balance, created_at, updated_at |
| 4 | public.payments | Money log to double-check bookings. | id, transaction_id, amount, method, status, created_at |

#### 1.4.2 Needed Output Files (Per Run)

Saved in data/raw/dt=YYYY-MM-DD/, one file per source table. Four files per run.

| # | Output File | Source Table | Format | Purpose |
| - | ----------- | ------------ | ------ | ------- |
| 1 | profiles.parquet | public.profiles | Parquet | Customer reference copy for the run. |
| 2 | inventory_items.parquet | public.inventory_items | Parquet | Item and sale reference copy for the run. |
| 3 | transactions.parquet | public.transactions | Parquet | Booking facts copy for the run. |
| 4 | payments.parquet | public.payments | Parquet | Payment facts copy for the run. |

#### 1.4.3 Scope Summary

| Scope Item | Value |
| ---------- | ----- |
| Number of source tables | 4 |
| Number of output files per run | 4 (one Parquet file per table) |
| Data extracted | Only columns needed for sales and forecast |
| Date range | Everything newer than the saved watermark, using the filter rule above |
| Rows kept in raw | Cancelled bookings and NULLs retained for audit |
| Cleaning | Out of scope; covered in the separate Transformation document |

### 1.5 Source Limitations and Assumptions

| # | Limitation or Assumption | How It Is Handled |
| - | ------------------------ | ----------------- |
| 1 | Read-only on the shop tables. | The pipeline never writes back to source tables. |
| 2 | payments has no updated_at, so late fixes to old payments can be missed. | Planned weekly full re-check of payments. |
| 3 | Dates are timezone-naive. | One date style is used to avoid skipped rows. |
| 4 | No internet, wrong password, or asleep database blocks the run. | Stop safely, keep the old checkpoint, and retry. |
| 5 | Schema changes (renamed or added columns) stop the run with an error. | Update the column list and this doc together before re-running. |
| 6 | Restricted access can block reads. | Read access on the 4 tables is required. |
| 7 | Small volume (hundreds of rows). | One computer and a nightly batch are enough. |

---

## 2. Source Tables and Column Specification

Only columns relevant to the pipeline are listed. Every row of the source table is not copied here.

### 2.1 Table Overview

| Table | Purpose | Primary Key | Relationships | Reason for Inclusion |
| ----- | ------- | ----------- | ------------- | -------------------- |
| public.profiles | Customer and staff reference, to know who rented. | id | Referenced by transactions.customer_id | Without it, bookings cannot be linked to customers. |
| public.inventory_items | Clothes reference, to know what was rented. | id | Referenced by transactions.item_id | Category and sale info are needed for category sales and correct revenue. |
| public.transactions | Rental bookings, the main fact source. | id | customer_id references profiles.id; item_id references inventory_items.id | All revenue counts and demand reports come from this table. Cancelled rows stay in raw for audit. |
| public.payments | Money log to double-check bookings. | id | transaction_id references transactions.id | Money in payments must match money in bookings. |

### 2.2 Column Specification

| Table | Column | Type | Nullable |
| ----- | ------ | ---- | -------- |
| profiles | id | TEXT | No |
| profiles | name | TEXT | No |
| profiles | phone | TEXT | Yes |
| profiles | address | TEXT | Yes |
| profiles | preferences | TEXT | Yes |
| profiles | created_at | TIMESTAMPTZ | No |
| profiles | updated_at | TIMESTAMPTZ | No |
| inventory_items | id | INTEGER | No |
| inventory_items | name | TEXT | No |
| inventory_items | category | TEXT | No |
| inventory_items | price | NUMERIC | No |
| inventory_items | status | TEXT | No |
| inventory_items | on_sale | BOOLEAN | No |
| inventory_items | discount_pct | NUMERIC | Yes |
| inventory_items | sale_price | NUMERIC | Yes |
| inventory_items | sale_ends_at | TIMESTAMPTZ | Yes |
| inventory_items | created_at | TIMESTAMPTZ | No |
| inventory_items | updated_at | TIMESTAMPTZ | No |
| transactions | id | TEXT | No |
| transactions | customer_id | TEXT | Yes |
| transactions | item_id | INTEGER | Yes |
| transactions | rental_date | DATE | Yes |
| transactions | due_date | DATE | Yes |
| transactions | return_date | DATE | Yes |
| transactions | status | TEXT | No |
| transactions | rental_fee | NUMERIC | No |
| transactions | downpayment | NUMERIC | No |
| transactions | total_paid | NUMERIC | No |
| transactions | balance | NUMERIC | No |
| transactions | created_at | TIMESTAMPTZ | No |
| transactions | updated_at | TIMESTAMPTZ | No |
| payments | id | TEXT | No |
| payments | transaction_id | TEXT | No |
| payments | amount | NUMERIC | No |
| payments | method | TEXT | No |
| payments | status | TEXT | No |
| payments | created_at | TIMESTAMPTZ | No |

---

## 3. Extraction Validation and Data Quality Checks

Scope limitation: focus on extraction-level validation only. Data cleaning, standardization, and business transformations belong to the separate Transformation document.

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

| # | Item | Rule |
| - | ---- | ---- |
| 1 | Retention period | At least 12 months for logs, raw folders, checkpoints, and quarantine files. This covers one school year, seasonal peaks, and defense re-checks. |
| 2 | Storage location | Project data/logs/, data/raw/, data/state/, and data/quarantine/. Zip a backup before submission or defense. |
| 3 | Access permissions | Team members and the panel can read. Only the runner can write. Passwords never appear in logs, and .env is never shared. |
| 4 | Archiving | Logs older than 90 days are zipped monthly. Old raw data may move to archive but must stay openable. |
| 5 | Deletion rules | Logs are not deleted before the retention period or while still needed for troubleshooting, auditing, or recovery. Deletion needs lead approval and proof that final reports still exist, and an authorized operator performs and records it. |
| 6 | Operational vs audit records | Daily run logs are operational. Dated raw folders and checkpoints are audit records and follow the same 12-month minimum; longer school retention applies separately if required. |

---

## 6. Extraction Data Contract and Naming Convention

### 6.1 Required Schema

Each run outputs four Parquet files in data/raw/dt=YYYY-MM-DD/: profiles.parquet, inventory_items.parquet, transactions.parquet, and payments.parquet. Columns follow Section 2 with correct types. Every file must have an id, plus the customerId, itemId, and transactionId links where they apply. Main IDs are never blank or repeated in the same file, and link fields are never blank except where NULL is explicitly allowed and preserved for later handling.

### 6.2 Naming Convention

The live source uses snake_case, while the planned pipeline uses camelCase canonical names to match the shop app code, as shown in the mapping below. Short forms stay consistent: Pct means percent (discountPct) and At means datetime (saleEndsAt, createdAt). New fields copy the app spelling. Primary keys are named id, and foreign keys use customerId, itemId, and transactionId in canonical form.

### 6.3 Schema Consistency

Unexpected changes to column names, data types, or required fields stop the run, and partial files are removed. The log names the problem column. The column list and this contract are updated together before re-running the missed date. New source columns are ignored until agreed, and new status values are saved as-is in raw, leaving the next stage to decide handling with a log note.

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
| All critical checks pass | Required tables and columns exist; identifiers present and unique; counts match; files complete; watermark safe. | Mark APPROVED and release for handover. | Automatic. |
| Non-critical warnings remain | Duplicates logged, zero new rows on quiet day, or count change, but extraction is complete. | Hold for review; preserve data unchanged. | MANUAL REVIEW by lead or data owner. |
| A critical check fails | Source unavailable, identifiers invalid, counts mismatch, or extraction incomplete. | Mark REJECTED; delete partial date folder, keep old checkpoint, stop handover. Retry transient failures on next run. | Automatic stop; developer resolves the failure. |

### Handover

Planned handover to the next stage (Transformation, separate document): 4 staged datasets with row counts, run ID and date, mode (demo or live), extraction window, validation results, errors, checkpoint value, log location, and acceptance status. Next stage will acknowledge run ID and status.

Required decisions:

1. Automatic Approval: all critical validation checks pass.
2. Manual Review: non-critical warnings or unexpected record counts.
3. Rejection or Failure: critical fail stops or quarantines partial output and retries later; cleaning is not done here.
4. Handover: output, metadata, and validation results passed as listed above.

Decision rules:

| Overall Condition | Acceptance Decision | Pipeline Action |
| ----------------- | ------------------- | --------------- |
| All required tables PASS | APPROVED | Release for handover |
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