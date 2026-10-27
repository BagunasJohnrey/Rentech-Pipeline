# Lab 4 — Stage 2: Data Transformation Documentation

RenTech: Rental Management System with Demand Forecasting for Mylene's Boutique (Balayan, Batangas)

## Objective

The goal of this stage is to take the validated bronze Parquet dataset handed over from the Extraction Stage (Stage 1) and transform it into a cleaned, standardized, and business-ready silver dataset that is documented, traceable, and validated for the next stage of the data pipeline (Load).

This document covers Transformation and transformation-level validation only. Extraction is documented separately in **Lab 3 — Stage 1: Data Extraction Documentation**. Loading into the target database or data warehouse is documented separately and is not part of this stage.

Planned pipeline name: `rentech-nightly-rental-mart`. Planned run: nightly batch, once per day, not live. Stage 2 runs after Stage 1 completes successfully.

---

## 1. Transformation Input and Processing Specification

### 1.1 Input Source

Stage 2 receives its input from Stage 1's output: the **bronze Parquet partition** written to `data/raw/dt=YYYY-MM-DD/`. Each run of Stage 1 produces one dated folder containing four Parquet files:

| File | Source Table (from Stage 1) | Purpose |
|------|----------------------------|---------|
| `profiles.parquet` | `public.profiles` | Customer reference data |
| `inventory_items.parquet` | `public.inventory_items` | Item catalog with categories and sale info |
| `transactions.parquet` | `public.transactions` | Rental bookings (main fact source) |
| `payments.parquet` | `public.payments` | Payment records for reconciliation |

Alongside these, Stage 1 writes a `_counts.json` file with the row count per table for reconciliation.

**Example**: Stage 1 extracts data for extraction run `EXT_20261018_0000`, writing to `data/raw/dt=2026-10-18/`. Stage 2 reads this partition, profiles the data, applies cleaning and standardization rules, applies business transformations, and generates the transformed silver dataset in `data/transformed/dt=2026-10-18/`.

Stage 2 **does not re-extract from the source database**. It only reads the Parquet files handed over by Stage 1. The extraction run ID and batch window are recorded in the transformation metadata for traceability.

### 1.2 Batch Processing Method

| Aspect | Detail |
|--------|--------|
| **Transformation tool** | Python with **Polars** (DataFrame library, fast, single-machine) |
| **Script** | `pipeline/transform.py` (logic), `pipeline/validate_transform.py` (validation), `pipeline/run_transform.py` (runner) |
| **Batch frequency** | Daily, after Stage 1 completes |
| **Schedule** | 00:30 Asia/Manila (30 minutes after Stage 1 at 00:00) |
| **Batch window** | Full snapshot of the bronze partition for the given date (`dt=YYYY-MM-DD`) |
| **Rerun behavior** | Fully idempotent: running the same date twice produces identical output (bit-for-bit) because all surrogate keys are deterministic hashes (`xxhash64_positive`) and the transformation is pure function of input |
| **Failed batch handling** | On failure, the staging directory (`.staging_dt=...`) is discarded, the target partition is untouched, and the run is logged as REJECTED. Retry by re-running the same date. No partial output is ever exposed. |

### 1.3 Transformation Scope

Stage 2 transforms the four bronze tables into **five core silver tables** plus **two audit tables** and **two quarantine tables**:

| Output Table | Type | Grain | Description |
|--------------|------|-------|-------------|
| `dim_customer` | Dimension | One row per customer | Cleaned customer attributes with validity flags |
| `dim_item` | Dimension | One row per inventory item | Standardized item attributes, effective price, flags |
| `dim_date` | Dimension | One row per calendar date | Date spine covering all business dates in the data |
| `fact_sales` | Fact | One row per rental transaction | Business-ready rental facts with payments, balances, flags |
| `agg_daily_category_sales` | Aggregate | One row per date × category | Daily rollup for reporting and forecasting |
| `audit/excluded_sales` | Audit | One row per cancelled transaction | Cancelled bookings with context for reporting |
| `audit/excluded_payments` | Audit | One row per payment on cancelled txn | Payments tied to cancelled transactions |
| `quarantine/quarantine_transactions` | Quarantine | One row per rejected transaction | Rows failing C-11 (bad return date) or C-12 (duplicates) |
| `quarantine/quarantine_payments` | Quarantine | One row per rejected payment | Rows failing C-16 (orphan payment) |

**Fields transformed**:
- **Cleaned**: customer name, phone, address, preferences; item name, category, pricing; transaction dates, statuses
- **Standardized**: phone formats (+63...), category values (Gown/Barong/Suit/...), payment/status codes (COMPLETED/ACTIVE/...)
- **Derived**: surrogate keys (`customer_key`, `item_key`, `sales_key`, `date_key`), effective price, rental/overdue days, balance due, overpayment, ledger mismatch flag, future booking flag
- **Aggregated**: daily category rental counts, revenue, payments, balances

### 1.4 Dependencies and Assumptions

| Dependency / Assumption | How It Is Handled |
|-------------------------|-------------------|
| **Processing order** | Dimensions first (`dim_customer`, `dim_item`, `dim_date`), then `fact_sales` (joins to dimensions), then `agg_daily_category_sales` (joins fact → dim_item) |
| **Stage 1 handover** | Stage 1 must complete with `APPROVED` status. Stage 2 reads `data/raw/dt=*/` and `_counts.json`. If missing, Stage 2 fails fast with clear error. |
| **Late-arriving data** | Stage 1 uses incremental watermark; late rows appear in next run's bronze partition. Stage 2 processes each date independently. No lookback in Stage 2. |
| **Unavailable lookup tables** | All lookups are internal (dimensions built from same bronze partition). No external dependencies. |
| **Timezone** | Bronze timestamps are naive UTC (per Stage 1 contract). Stage 2 converts to **Asia/Manila (UTC+8)** for business dates. |
| **Volume** | Small (hundreds to low thousands of rows per run). Single-machine Polars processing is sufficient. |
| **No target database** | Stage 2 is file-only: reads `data/raw/`, writes `data/transformed/` and `data/quarantine/transform/`. No database credentials needed. |

---

## 2. Data Profiling and Issue Identification

Before defining rules, we profiled the bronze data from multiple runs (demo seed=42, live runs). The following issues were consistently found:

### 2.1 Customer Data (profiles)

| Column | Issue Type | Example | Affected | Planned Treatment |
|--------|------------|---------|----------|-------------------|
| `name` | Missing/empty/whitespace | `""`, `"  "`, `null` | ~5% | Rule C-01: set to NULL, `name_valid=false` |
| `phone` | Invalid PH format | `"09123456789"`, `"+639123456789"`, `"globe"` | ~18% | Rule C-02: extract digits, normalize to `+639xxxxxxxxx` or NULL, `phone_valid` flag |
| `address` | Missing/empty | `null`, `""` | ~12% | Rule C-03: set to `"UNKNOWN"`, `address_valid=false` |
| `preferences` | Mixed formats | `"gown,barong"`, `["gown","barong"]`, `null` | ~35% non-null | Rule C-04: parse to `List(String)`, count |
| `(name, phone)` | Duplicate pairs | Same name+phone for 2+ customers | 4 pairs in 270 rows | Rule C-05: flag `potential_duplicate_customer=true` |

### 2.2 Item Data (inventory_items)

| Column | Issue Type | Example | Affected | Planned Treatment |
|--------|------------|---------|----------|-------------------|
| `category` | Inconsistent casing | `"gown"`, `"GOWN"`, `"Gown"` | ~15% | Rule S-04: case-insensitive match to standard list, else `"Other"` with `source_category` |
| `category` | Test/misc values | `"TEST"`, `"test"`, `"sample"` | ~3 items | Rule B-02: flag `is_test_item=true` |
| `on_sale` + `discount_pct` | Inconsistent flags | `on_sale=false` but `discount_pct=20` | ~8 items | Rule S-06: zero discount, flag `sale_flag_inconsistent=true` |
| `sale_price` / `sale_ends_at` | Nullable, mixed | `null`, `2026-12-31` | ~30-40% | Rule S-07, S-10: cast to nullable Float64/Date |

### 2.3 Transaction Data (transactions)

| Column | Issue Type | Example | Affected | Planned Treatment |
|--------|------------|---------|----------|-------------------|
| `returnDate` | Before rentalDate | `returnDate=2024-11-20`, `rentalDate=2024-11-25` | 2-3 per run | Rule C-11: **quarantine** entire row (`INVALID_RETURN_DATE`) |
| `(customerId, itemId, rentalDate)` | Duplicate natural key | Same customer+item+date, different `updatedAt` | 1-2 per run | Rule C-12: keep latest by `updatedAt`→`createdAt`→`id`; **quarantine** rest (`DUPLICATE_TRANSACTION`) |
| `customerId` / `itemId` | NULL | `null` | ~2-3% | Preserved in bronze; join to dimensions yields NULL key (tracked in validation) |
| `status` | Cancelled | `"Cancelled"`, `"Canceled"`, `"Void"` | ~10-15% | Rule B-07: **exclude** to `audit/excluded_sales` (not quarantine — valid business event) |

### 2.4 Payment Data (payments)

| Column | Issue Type | Example | Affected | Planned Treatment |
|--------|------------|---------|----------|-------------------|
| `transactionId` | Orphan (not in cleaned transactions) | Payment for txn that was quarantined or missing | 1-2 per run | Rule C-16: **quarantine** (`INVALID_PARENT_TRANSACTION`) |
| `status` | Non-standard values | `"succeeded"`, `"processing"`, `"declined"` | ~20% | Rule B-10: map to `COMPLETED/PENDING/FAILED/REFUNDED/UNKNOWN` |

> **Note**: Issues like negative `rental_fee` (C-13), `dueDate < rentalDate` (C-14), `salePrice > price` (C-15) are documented as **warnings** for future checks; they do not quarantine or exclude rows in this handover.

---

## 3. Data Cleaning Rules

Cleaning rules correct or remove poor-quality data. **No record is silently dropped** — every rejected row goes to a quarantine table with the rule ID, reason, and full raw record for audit.

| Rule ID | Target | Condition | Action | Justification |
|---------|--------|-----------|--------|---------------|
| **C-01** | `profiles.name` | NULL, empty, or whitespace only | Set to NULL; `name_valid=false` | Prevents false matches in joins; tracks data quality |
| **C-02** | `profiles.phone` | Not a valid PH mobile format (10 digits starting with 9, or 11/12 digits starting with 639) | Extract digits; normalize to `+639xxxxxxxxx`; if invalid, set NULL; `phone_valid=false` | Standardizes to E.164 for consistent contact; flags bad data |
| **C-03** | `profiles.address` | NULL or empty | Set to `"UNKNOWN"`; `address_valid=false` | Ensures non-null address for shipping/reporting; tracks missing |
| **C-04** | `profiles.preferences` | Comma-separated string or JSON array | Parse to `List(String)`; `preference_count = len(list)` | Enables array operations; consistent type for downstream |
| **C-05** | `profiles` (row) | Duplicate `(name, phone)` pair across rows | Flag `potential_duplicate_customer=true` on all matching rows | Identifies possible duplicate customers for review |
| **C-11** | `transactions` | `returnDate < rentalDate` | **Quarantine** row to `quarantine_transactions` with `rule_id=C-11`, `reason_code=INVALID_RETURN_DATE` | Impossible business logic; must not enter fact table |
| **C-12** | `transactions` | Duplicate on `(customerId, itemId, rentalDate)` | Keep row with latest `updatedAt` (then `createdAt`, then `id`); **quarantine** others with `rule_id=C-12`, `reason_code=DUPLICATE_TRANSACTION` | Natural key should be unique; latest update is authoritative |
| **C-16** | `payments` | `transactionId` not in cleaned transactions | **Quarantine** row to `quarantine_payments` with `rule_id=C-16`, `reason_code=INVALID_PARENT_TRANSACTION` | Orphan payment cannot be reconciled; parent missing |

> **Quarantine schema** (both tables): `quarantine_id` (hash), `source_table`, `source_id`, `rule_id`, `reason_code`, `reason_detail`, `raw_record` (JSON), `detected_at`, `transformation_run_id`, `batch_id`.

---

## 4. Standardization and Data Type Rules

Standardization makes data consistent across tables and sources. All type conversions are explicit; failures are handled per rule.

### 4.1 Customer Normalization (applied in `build_dim_customer`)

| Rule | Column | Source Format | Standard Format | On Failure |
|------|--------|---------------|-----------------|------------|
| **S-03** | `name` | Mixed case, extra spaces | Trim, collapse whitespace, **Title Case** | NULL → NULL, `name_valid=false` |
| **S-04** | `phone` | `09123456789`, `+639123456789`, `9123456789`, `globe` | **E.164**: `+639123456789` (12 digits) | Invalid → NULL, `phone_valid=false` |
| **S-05** | `address` | Mixed case, extra spaces, empty | Trim, collapse whitespace; empty → `"UNKNOWN"` | Empty → `"UNKNOWN"`, `address_valid=false` |
| **S-06** | `preferences` | `"gown, barong"` or `["gown","barong"]` | `List(String)` e.g. `["Gown","Barong"]` | NULL/empty → `[]`, count=0 |
| **S-07** | `preference_count` | (derived) | `Int64` = length of preferences list | Always computable |

### 4.2 Item Standardization (applied in `build_dim_item`)

| Rule | Column | Source Format | Standard Format | On Failure |
|------|--------|---------------|-----------------|------------|
| **S-03** | `name` | Mixed case, extra spaces | Trim, collapse whitespace, **Title Case** | — |
| **S-04** | `category` | Any string | Match to **CATEGORIES** = [`Gown`, `Barong`, `Suit`, `Costume`, `Filipiniana`] case-insensitive; else `"Other"` + `source_category` preserves original | No failure — `"Other"` is valid |
| **S-05** | `effective_price` | `price`, `on_sale`, `discount_pct` | If `on_sale=true` and consistent: `round(price * (1 - discount_pct/100), 2)` else `price` | Inconsistent → use `price`, flag `sale_flag_inconsistent` |
| **S-06** | `discount_pct` | Numeric, may be >0 when `on_sale=false` | If `on_sale=false` and `discount_pct>0`: set to `0.0`, flag `sale_flag_inconsistent=true` | Forces consistency |
| **S-07** | `sale_price` | Numeric or NULL | `Float64` (nullable) | Cast failure → NULL |
| **S-10** | `sale_ends_at` | Timestamp or NULL | **Date (Manila)** nullable | Cast failure → NULL |

### 4.3 Status Code Standardization

| Rule | Column | Source Values | Standard Values |
|------|--------|---------------|-----------------|
| **B-10** | `payment_status` | `completed`, `succeeded`, `confirmed` → `COMPLETED`; `pending`, `processing` → `PENDING`; `failed`, `declined`, `cancelled` → `FAILED`; `refunded`, `reversed` → `REFUNDED`; else → `UNKNOWN` |
| **B-13** | `transaction_status` | `Active`, `Ongoing`, `Rented` → `ACTIVE`; `Returned`, `Completed`, `Done` → `RETURNED`; `Overdue`, `Late` → `OVERDUE`; `Reserved`, `Booked`, `Pending` → `RESERVED`; `Cancelled`, `Canceled`, `Void` → `CANCELLED`; else → `UNKNOWN` |
| **B-01..B-03** | Item flags | Derived from `status`, `category`, `on_sale`, `discount_pct` | `is_deleted`, `is_test_item`, `sale_flag_inconsistent` (Boolean) |

### 4.4 Date and Time Handling

- **All timestamps in bronze are naive UTC** (per Stage 1 contract).
- **Business dates** (`rental_date`, `due_date`, `return_date`, `sale_ends_at`) are converted to **Asia/Manila (UTC+8)** using `utc_to_manila_date()`.
- **Date keys** are `YYYYMMDD` integers (e.g., `20261018`).
- **Created/updated timestamps** on dimensions/facts are kept as UTC `Datetime(time_zone="UTC")`.

---

## 5. Business Transformation Rules

These rules convert cleaned data into business-ready facts and aggregates. They are applied in `build_fact_sales` and `build_agg_daily_category_sales`.

### 5.1 Fact Sales Construction (one row per non-cancelled transaction)

| Rule ID | Input Columns | Logic / Formula | Output Column | Business Reason |
|---------|---------------|-----------------|---------------|-----------------|
| **B-01** | `customerId` + `dim_customer` | Join on `customerId` → `customer_key` (xxhash64) | `customer_key` | Surrogate key for dimensional model |
| **B-02** | `itemId` + `dim_item` | Join on `itemId` → `item_key` (xxhash64) | `item_key` | Surrogate key for dimensional model |
| **B-03** | `rentalDate`, `dueDate`, `returnDate` | Convert to Manila `Date` | `rental_date`, `due_date`, `return_date` | Business dates in store timezone |
| **B-04** | `rental_date`, `due_date` | `(due_date - rental_date).days` | `rental_days` | Contractual rental period |
| **B-05** | `rental_date`, `return_date` | If returned: `(return_date - rental_date).days` else NULL | `actual_rental_days` | Actual usage for revenue recognition |
| **B-06** | `return_date`, `due_date`, `batch_date` | If returned: `max((return_date - due_date).days, 0)` else `max((batch_date - due_date).days, 0)` | `overdue_days` | Late return tracking |
| **B-08** | Payments per transaction | Sum `amount` where `payment_status=COMPLETED` | `payment_total`, `payment_count`, `latest_payment_date` | Completed money received |
| **B-09** | `rental_fee`, `payment_total` | `max(rental_fee - payment_total, 0)` | `balance_due` | Amount still owed |
| **B-10** | `rental_fee`, `payment_total` | `max(payment_total - rental_fee, 0)` | `overpayment_amount` | Excess payment (refund candidate) |
| **B-11** | `balance` (source), `balance_due` (computed) | `\|source - computed\| > 0.01` | `ledger_mismatch` (Boolean) | Detects source data drift |
| **B-12** | `rental_date`, `batch_date` | `rental_date > batch_date` | `is_future_booking` (Boolean) | Advance bookings kept for forecasting |
| **B-13** | `return_date` | `return_date IS NOT NULL` | `is_returned` (Boolean) | Returned vs active rental |
| **B-14** | `overdue_days` | `overdue_days > 0` | `is_overdue` (Boolean) | Quick filter for overdue report |

### 5.2 Cancelled Transaction Exclusion (B-07)

| Rule ID | Input | Condition | Action | Output |
|---------|-------|-----------|--------|--------|
| **B-07** | `transactions` | `transaction_status = CANCELLED` | Move to `audit/excluded_sales` with `exclusion_reason=CANCELLED`, `rule_id=B-07`; move associated `COMPLETED` payments to `audit/excluded_payments` | Excluded from `fact_sales` (not revenue); retained for audit |

> **Why exclude here, not in Extraction?** Extraction keeps cancelled rows in bronze for audit. Transformation applies the **business rule** that cancelled bookings are not revenue. The exclusion is documented, counted, and the excluded rows are preserved in audit tables with full context.

### 5.3 Daily Category Aggregate (B-08, B-09)

| Rule ID | Input | Grouping | Measures | Output Table |
|---------|-------|----------|----------|--------------|
| **B-08** | `fact_sales` + `dim_item.category` | `rental_date_key` × `category` | `rental_count` (count), `returned_count` (sum `is_returned`), `total_rental_fee` (sum), `total_payment` (sum `payment_total`), `total_balance_due` (sum), `total_overpayment` (sum) | `agg_daily_category_sales` |
| **B-09** | `dim_item` | `item_key` → `category_key` (xxhash64 of category) | `category_key`, `category` | Joined into aggregate |

### 5.4 Dimensional Modeling Notes

- **Surrogate keys**: All keys (`customer_key`, `item_key`, `date_key`, `sales_key`, `category_key`) are generated via `xxhash64_positive()` — deterministic, no sequence dependency.
- **SCD Type 1**: Dimensions are full snapshot per run (`is_current=true`); no history tracking in this handover.
- **Date dimension**: `dim_date` spans the min/max of all business dates in the partition (createdAt, rentalDate, dueDate, returnDate) to guarantee FK coverage.

---

## 6. Transformed Data Contract and Naming Convention

### 6.1 Required Output Schema (Silver)

**Core Tables** — written to `data/transformed/dt=YYYY-MM-DD/`:

| Table | Key Columns | Important Columns | Types |
|-------|-------------|-------------------|-------|
| `dim_customer` | `customer_key` (PK, Int64) | `customer_id` (String), `name`, `name_valid` (Bool), `phone`, `phone_valid` (Bool), `has_phone` (Bool), `address`, `address_valid` (Bool), `preferences` (List<String>), `preference_count` (Int64), `potential_duplicate_customer` (Bool), `created_at` (Datetime UTC), `updated_at` (Datetime UTC), `is_current` (Bool) | All non-null except `name`, `phone`, `address` (nullable per validity flags) |
| `dim_item` | `item_key` (PK, Int64) | `item_id` (String), `name`, `category`, `source_category`, `price` (Float64), `effective_price` (Float64), `on_sale` (Bool), `discount_pct` (Float64), `sale_price` (Float64, nullable), `sale_ends_at` (Date, nullable), `status`, `is_deleted` (Bool), `is_test_item` (Bool), `sale_flag_inconsistent` (Bool), `created_at` (Datetime UTC), `updated_at` (Datetime UTC), `is_current` (Bool) | `category` non-null; `sale_price`, `sale_ends_at` nullable |
| `dim_date` | `date_key` (PK, Int64, YYYYMMDD) | `date` (Date), `year`, `quarter`, `month`, `month_name`, `day`, `day_of_week` (1-7 Mon-Sun), `day_name`, `week_of_year`, `is_weekend` (Bool), `is_holiday` (Bool, default False) | All non-null |
| `fact_sales` | `sales_key` (PK, Int64) | `transaction_id` (String), `customer_key` (FK), `customer_id` (String), `item_key` (FK), `item_id` (String), `rental_date_key` (FK), `rental_date` (Date), `due_date_key` (FK), `due_date` (Date), `return_date_key` (FK, nullable), `return_date` (Date, nullable), `is_returned` (Bool), `status` (String: ACTIVE/RETURNED/OVERDUE/RESERVED/CANCELLED/UNKNOWN), `rental_days` (Int64), `actual_rental_days` (Int64, nullable), `overdue_days` (Int64), `is_overdue` (Bool), `is_future_booking` (Bool), `rental_fee` (Float64), `payment_total` (Float64), `payment_count` (Int64), `balance_due` (Float64), `overpayment_amount` (Float64), `source_balance` (Float64), `ledger_mismatch` (Bool), `latest_payment_date` (Date, nullable), `created_at` (Datetime UTC), `updated_at` (Datetime UTC) | FKs to dimensions; `return_date*`, `latest_payment_date` nullable |
| `agg_daily_category_sales` | (`date_key`, `category_key`) (PK) | `date_key` (FK), `category_key` (Int64), `category` (String), `rental_count` (Int64), `returned_count` (Int64), `total_rental_fee` (Float64), `total_payment` (Float64), `total_balance_due` (Float64), `total_overpayment` (Float64) | All non-null |

**Audit Tables** — written to `data/transformed/dt=YYYY-MM-DD/audit/`:

| Table | Key | Purpose |
|-------|-----|---------|
| `excluded_sales` | `exclusion_key` (Int64) | Cancelled transactions with `exclusion_reason=CANCELLED`, `rule_id=B-07`, rental context |
| `excluded_payments` | `exclusion_key` (Int64) | Completed payments on cancelled transactions |

**Quarantine Tables** — written to `data/quarantine/transform/dt=YYYY-MM-DD/`:

| Table | Key | Rules |
|-------|-----|-------|
| `quarantine_transactions` | `quarantine_id` (Int64) | C-11, C-12 |
| `quarantine_payments` | `quarantine_id` (Int64) | C-16 |

### 6.2 Naming Convention

| Element | Convention | Examples |
|---------|------------|----------|
| **Files** | snake_case, singular | `dim_customer.parquet`, `fact_sales.parquet` |
| **Directories** | kebab-case | `data/transformed/dt=2026-10-18/`, `audit/`, `quarantine/transform/` |
| **Columns** | snake_case | `customer_key`, `rental_date_key`, `payment_total`, `is_overdue` |
| **Surrogate keys** | `*_key` (Int64) | `customer_key`, `item_key`, `sales_key`, `date_key` |
| **Natural/business keys** | `*_id` (String) | `customer_id`, `item_id`, `transaction_id` |
| **Dates** | `*_date` (Date), `*_at` (Datetime UTC), `*_date_key` (Int64 YYYYMMDD) | `rental_date`, `created_at`, `rental_date_key` |
| **Booleans** | `is_*`, `has_*`, `*_flag_*` | `is_returned`, `has_phone`, `sale_flag_inconsistent` |
| **Audit/Quarantine** | Prefix with table type | `audit/excluded_sales`, `quarantine/quarantine_transactions` |

### 6.3 Source-to-Target Mapping (Key Transformations)

| Source Column (Bronze) | Target Column (Silver) | Transformation Applied |
|------------------------|------------------------|------------------------|
| `profiles.id` | `dim_customer.customer_id` | Pass through (String) |
| `profiles.id` | `dim_customer.customer_key` | `xxhash64_positive(id)` |
| `profiles.name` | `dim_customer.name` | C-01, S-03 (trim, title case) |
| `profiles.phone` | `dim_customer.phone` | C-02, S-04 (E.164 normalize) |
| `profiles.address` | `dim_customer.address` | C-03, S-05 (UNKNOWN if empty) |
| `profiles.preferences` | `dim_customer.preferences` | C-04, S-06 (parse to List<String>) |
| `inventory_items.id` | `dim_item.item_id` | Cast to String |
| `inventory_items.id` | `dim_item.item_key` | `xxhash64_positive(str(id))` |
| `inventory_items.category` | `dim_item.category` | S-04 (standardize to CATEGORIES) |
| `inventory_items.price` + `on_sale` + `discount_pct` | `dim_item.effective_price` | S-05, S-06 (sale logic) |
| `transactions.id` | `fact_sales.transaction_id` | Pass through |
| `transactions.id` | `fact_sales.sales_key` | `xxhash64_positive(id)` |
| `transactions.rentalDate` | `fact_sales.rental_date` | `utc_to_manila_date()` |
| `transactions.rentalDate` | `fact_sales.rental_date_key` | `int(rental_date.strftime("%Y%m%d"))` |
| `transactions.status` | `fact_sales.status` | B-13 (standardize to ACTIVE/RETURNED/...) |
| `payments.amount` (agg) | `fact_sales.payment_total` | B-08 (sum completed payments) |
| `transactions.rentalFee` - `payment_total` | `fact_sales.balance_due` | B-09 (max with 0) |

### 6.4 Schema Consistency

- **Unexpected input schema changes** (missing/renamed columns in bronze) cause the read step to fail fast with a clear error naming the missing column.
- **New status/category values** in bronze are handled by standardization rules (map to `UNKNOWN` or `Other` with `source_*` preserved) and logged.
- **Contract updates**: Column list in `config.py` and this document are updated together before re-running.

---

## 7. Transformation Validation and Data Quality Checks

Stage 2 runs **24 validation checks (V1–V24)** after writing the silver partition. Checks V1–V21 are **Critical** (FAIL = REJECTED); V22–V24 are **Warning/Info** (FAIL = logged, partition still APPROVED).

| Check | Target | Purpose | Validation Criteria | Severity |
|-------|--------|---------|---------------------|----------|
| **V1–V5** | 5 core Parquet files | Files exist | All 5 files present | Critical |
| **V6–V10** | Each core table | Non-empty output | Row count > 0 | Critical |
| **V11–V15** | PK uniqueness | No duplicates | 0 duplicate keys (composite for aggregate) | Critical |
| **V16–V17** | FK `fact_sales` → `dim_customer`, `dim_item` | Referential integrity | 0 orphans | Critical |
| **V18–V19** | FK `fact_sales.rental_date_key`, `due_date_key` → `dim_date` | Date dimension coverage | 0 orphans | Critical |
| **V20** | FK `agg_daily_category_sales.date_key` → `dim_date` | Date dimension coverage | 0 orphans | Critical |
| **V21** | `fact_sales` | Payment reconciliation | `fee = payment + balance - overpayment` within 0.01 | Critical |
| **V22** | `fact_sales` | Future bookings retained | Count reported (always PASS) | Info |
| **V23** | `audit/excluded_sales` | Business exclusion rate | `excluded / (fact + excluded) ≤ 15%` | Warning |
| **V24** | Quarantine tables | Quarantine rate | `(quarantine_txns + quarantine_payments) / input_rows ≤ 1%` | Warning |

### 7.1 Reconciliation Rules

The `_reconciliation.json` and `_manifest.json` enforce the **row-level reconciliation equation**:

```
Input rows (bronze) = Output rows (core) + Quarantine rows + Audit rows
```

Specifically:
- **Per table**: `input_count[table] = output_count[table] + quarantine_count[table]` (for transactions/payments)
- **Business exclusions**: `excluded_sales` counted separately; not in quarantine (valid business event)
- **Amount reconciliation**: `sum(rental_fee) = sum(payment_total) + sum(balance_due) - sum(overpayment_amount)` (V21)

### 7.2 Validation Output

- **`_validation.json`**: Full check results with `status: "APPROVED"` or `"REJECTED"`, per-check details.
- **`_manifest.json`**: Updated with `validation_summary` (e.g., `"24/24 PASS"`), `status`, `rejection_rate`, `business_exclusion_rate`.
- **Run fails** (exit code 1) if any Critical check fails; exits 0 if all Critical pass.

---

## 8. Transformation Metadata and Log Specification

### 8.1 Metadata Files (written per run)

| File | Purpose | Key Fields |
|------|---------|------------|
| **`_manifest.json`** | Complete run manifest | `transformation_run_id`, `batch_id`, `batch_date`, `batch_window`, `source_extraction_run_id`, `source_partition`, `output_partition`, `rules_version`, `code_version`, `business_timezone`, `status`, `started_at`, `completed_at`, `row_counts`, `checksums` (SHA-256), `validation_summary`, `rejection_rate`, `business_exclusion_rate`, `amount_totals`, `tables`, `audit`, `rules_applied`, `quarantine`, `notes` |
| **`_reconciliation.json`** | Input/output balance | `run_id`, `batch_date`, `inputs` (bronze counts), `outputs` (silver counts), `quarantine`, `audit`, `amount_totals` |
| **`_rules_applied.json`** | Rule-level audit trail | `rules_version`, `applied_at`, `rules[]` with `rule_id`, `count`, `examples[3]` |
| **`_validation.json`** | Validation results | `batch_date`, `status`, `validated_at`, `checks[]` with `check_id`, `description`, `status`, `details`, `run_id` |

### 8.2 Log Files

- **Per-run log**: `data/logs/transform_YYYY-MM-DD.log`
- **Format**: `timestamp | LEVEL | logger | message` (e.g., `2026-10-18 00:30:15,123 | INFO | rentech.transform | Read bronze profiles: 270 rows`)
- **Level**: INFO for normal flow, ERROR for failures, WARNING for validation warnings.

### 8.3 How Metadata Is Used

| Use Case | How |
|----------|-----|
| **Monitoring** | Morning check: `transform_YYYY-MM-DD.log` exists, ends with `Validation APPROVED`, `_manifest.json.status == "APPROVED"` |
| **Troubleshooting** | Log shows which table/rule failed; `_validation.json` has exact check details; quarantine files have raw records |
| **Auditing** | Dated silver partitions prove what transformation produced on each date; `_rules_applied.json` shows every rule hit |
| **Lineage** | `transformation_run_id` links to `source_extraction_run_id`; `batch_id` ties to business date |
| **Recovery** | Failed run leaves target partition untouched; retry same date. Staging directory (`.staging_dt=...`) is atomic swap on success. |

---

## 9. Log Retention, Rejected Data, and Access

| Item | Rule |
|------|------|
| **Retention period** | At least **12 months** for logs, raw folders, checkpoints, quarantine files, and silver partitions. Covers one school year, seasonal peaks (graduation, Christmas), and defense re-checks. |
| **Storage location** | `data/logs/` (run logs), `data/raw/` (bronze), `data/transformed/` (silver), `data/quarantine/transform/` (rejected), `data/state/` (watermark). Zip a backup of `data/` before submission or defense. |
| **Quarantine location** | `data/quarantine/transform/dt=YYYY-MM-DD/` — holds `quarantine_transactions.parquet` and `quarantine_payments.parquet` with full raw records and rejection reasons. |
| **Access permissions** | Team members and the panel can **read**. Only the pipeline runner (scheduled or manual) can **write**. **Passwords never appear in logs**; `.env` is never shared or committed. |
| **Archiving** | Logs older than 90 days zipped monthly to `data/logs/archive/pipeline_YYYY-MM.zip`; keep originals until 12-month mark. Silver partitions and quarantine are not auto-archived (small volume). |
| **Deletion rules** | Not deleted before retention period or while needed for troubleshooting, auditing, or recovery. Deletion needs **lead approval** and proof that final reports still exist. |
| **Operational vs. audit records** | Daily run logs (`transform_*.log`) are **operational**. Dated bronze/silver folders, quarantine, checkpoints, and metadata JSONs are **audit records** — same 12-month minimum. |

> **Note**: Transformation quarantine stores only records that could not be safely transformed, with rejection reason, rule ID, and batch ID. Avoid storing unnecessary sensitive source data in logs or quarantine.

---

## 10. Transformation Acceptance and Handover Rules

Stage 1 decides whether data proceeds **to** Transformation; Stage 2 decides whether transformed output proceeds **to** Load.

### 10.1 Rejection Rate Threshold

- **Quarantine rate threshold**: **1%** (V24 warning). Justification: At current volume (~1K input rows), >10 quarantined rows suggests systemic source issue, not isolated bad data.
- **Business exclusion rate threshold**: **15%** (V23 warning). Justification: Cancelled bookings are a valid business event; high rate may indicate process issue but not data corruption.

### 10.2 Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action | Decision Type |
|-------------------|---------------------|-----------------|---------------|
| All Critical checks (V1–V21) PASS | **APPROVED** | Release silver partition for Load handover | Automatic |
| Any Critical check (V1–V21) FAIL | **REJECTED** | Keep partition for inspection; do not advance; log error; retry on next run | Automatic |
| Warning checks (V23, V24) FAIL only | **APPROVED with warnings** | Release for Load; log warning; recommend manual review | Manual review suggested |

### 10.3 Handover Package to Load Stage

The following are handed over to the Load Stage (or downstream BI/forecasting):

1. **Silver Parquet**: `data/transformed/dt=YYYY-MM-DD/` (5 core tables + `audit/`)
2. **Metadata**: `_manifest.json`, `_reconciliation.json`, `_rules_applied.json`, `_validation.json`
3. **Quarantine**: `data/quarantine/transform/dt=YYYY-MM-DD/` (for data quality review)
4. **Logs**: `data/logs/transform_YYYY-MM-DD.log`

### 10.4 Acceptance Status Values

| Status | Meaning |
|--------|---------|
| `APPROVED` | All critical validations passed; data safe for consumption |
| `REJECTED` | Critical validation failed; partition retained but not released |

---

## 11. Common Transformation Problems and Mitigation

| Problem | Possible Cause | Impact on Transformation | Proposed Solution / Mitigation |
|---------|----------------|--------------------------|--------------------------------|
| **FK orphans (V18–V20 FAIL)** | `dim_date` range excludes transaction dates (rental/due/return dates fall outside createdAt range) | Partition REJECTED | **Fixed**: `dim_date` now spans min/max of `createdAt`, `rentalDate`, `dueDate`, `returnDate` from all bronze tables (see `transform.py:711-722`) |
| **Quarantine rate >1% (V24 FAIL)** | High bad-data volume in source (demo includes intentional bad rows; live may have spikes) | Warning only; partition still APPROVED | Monitor trend; if persistent in live, investigate source data quality; add source-level validation |
| **Business exclusion rate >15% (V23 FAIL)** | Many CANCELLED transactions in batch (seasonal, policy change) | Warning only | Review cancellation policy; expected for seasonal peaks; not a data quality issue |
| **Payment reconciliation mismatch (V21 FAIL)** | Source `balance` column drift vs computed `balance_due` | Partition REJECTED | Compare `balance` vs computed; log mismatch rows; investigate source application bug |
| **Duplicate PK in dimensions (V11–V13 FAIL)** | Source ID collision or hash collision | Partition REJECTED | `xxhash64_positive` has negligible collision risk; verify source IDs unique; add unit test |
| **Missing bronze partition** | Stage 1 failed or not run for the date | Transform fails fast with clear error | Scheduler runs Stage 1 at 00:00, Stage 2 at 00:30; retry Stage 1 first; manual run with `--date` |
| **Out-of-memory on large batch** | Polars materializes all frames; data volume grows | OOM crash | Current volume <10K rows; if grows, process in chunks or use Polars streaming |
| **Timezone confusion** | Naive timestamps treated as local vs UTC | Wrong Manila dates, FK orphans | Code assumes bronze naive = UTC; converts via `utc_to_manila_date()`; documented in contract |
| **Schema change in bronze** | Stage 1 output columns renamed/added/removed | Read fails with explicit missing column error | Column list in `config.py` is single source of truth; update together with this doc |

---

## Expected Output

This documentation is consistent with the implemented transformation process (`pipeline/transform.py`, `pipeline/validate_transform.py`, `pipeline/run_transform.py`) and provides sufficient information for a team member to understand, validate, monitor, and maintain the transformation stage.

The transformed data is validated and packaged for handover to the next stage: **Load**. Loading into the target database or data warehouse is not performed in this stage.

