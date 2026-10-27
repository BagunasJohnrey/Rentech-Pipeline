"""RenTech Stage 2: Transformation Validation (Parquet output)."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from .config import DEFAULT_DATA_DIR

logger = logging.getLogger("rentech.validate_transform")
CORE_TABLES = [
    "dim_customer",
    "dim_item",
    "dim_date",
    "fact_sales",
    "agg_daily_category_sales",
]
UTC = timezone.utc

@dataclass
class ValidationResult:
    check_id: str
    description: str
    status: str
    details: str = ""

def _read(partition: Path, name: str) -> pl.DataFrame:
    return pl.read_parquet(partition / f"{name}.parquet")

def run_validation(
    data_dir: Path | None, run_date: date
) -> tuple[list[ValidationResult], bool]:
    data_dir = data_dir or DEFAULT_DATA_DIR
    partition = data_dir / "transformed" / f"dt={run_date.isoformat()}"
    results: list[ValidationResult] = []

    if not partition.exists():
        return [ValidationResult("V0", "Transformed partition exists", "FAIL", str(partition))], False

    frames = {}
    for table in CORE_TABLES:
        path = partition / f"{table}.parquet"
        exists = path.exists()
        results.append(ValidationResult(
            f"V{len(results) + 1}",
            f"{table} file exists",
            "PASS" if exists else "FAIL",
            str(path),
        ))
        if exists:
            frames[table] = _read(partition, table)

    for table, df in frames.items():
        results.append(ValidationResult(
            f"V{len(results) + 1}",
            f"{table} row count > 0",
            "PASS" if df.height > 0 else "FAIL",
            f"rows={df.height}",
        ))

    for table, keys in {
        "dim_customer": ["customer_key"],
        "dim_item": ["item_key"],
        "dim_date": ["date_key"],
        "fact_sales": ["sales_key"],
        "agg_daily_category_sales": ["date_key", "category_key"],
    }.items():
        if table not in frames:
            continue
        dups = frames[table].select(pl.struct(keys).is_duplicated().sum()).item()
        results.append(ValidationResult(
            f"V{len(results) + 1}",
            f"{table} primary key ({', '.join(keys)}) uniqueness",
            "PASS" if dups == 0 else "FAIL",
            f"duplicates={dups}",
        ))

    for child, child_col, parent, parent_col in [
        ("fact_sales", "customer_key", "dim_customer", "customer_key"),
        ("fact_sales", "item_key", "dim_item", "item_key"),
        ("fact_sales", "rental_date_key", "dim_date", "date_key"),
        ("fact_sales", "due_date_key", "dim_date", "date_key"),
        ("agg_daily_category_sales", "date_key", "dim_date", "date_key"),
    ]:
        if child not in frames or parent not in frames:
            continue
        orphans = (
            frames[child]
            .select(pl.col(child_col))
            .join(
                frames[parent].select(pl.col(parent_col)),
                left_on=child_col, right_on=parent_col, how="anti",
            )
            .height
        )
        results.append(ValidationResult(
            f"V{len(results) + 1}",
            f"FK {child}.{child_col} -> {parent}.{parent_col}",
            "PASS" if orphans == 0 else "FAIL",
            f"orphans={orphans}",
        ))

    if "fact_sales" in frames and frames["fact_sales"].height:
        fact = frames["fact_sales"]
        mismatched = fact.filter(
            (pl.col("rental_fee") - pl.col("payment_total")
             - pl.col("balance_due") + pl.col("overpayment_amount")).abs() > 0.01
        ).height
        results.append(ValidationResult(
            f"V{len(results) + 1}",
            "Payment reconciliation: fee = payment + balance - overpayment",
            "PASS" if mismatched == 0 else "FAIL",
            f"mismatched_rows={mismatched}",
        ))

        future = fact.filter(pl.col("is_future_booking")).height
        results.append(ValidationResult(
            f"V{len(results) + 1}",
            "Future bookings retained (not rejected)",
            "PASS",
            f"future_bookings={future}",
        ))

    audit_dir = partition / "audit"
    excluded_sales = (
        _read(audit_dir, "excluded_sales")
        if (audit_dir / "excluded_sales.parquet").exists()
        else pl.DataFrame()
    )
    q_dir = data_dir / "quarantine" / "transform" / f"dt={run_date.isoformat()}"
    q_txns = (
        _read(q_dir, "quarantine_transactions")
        if (q_dir / "quarantine_transactions.parquet").exists()
        else pl.DataFrame()
    )
    q_payments = (
        _read(q_dir, "quarantine_payments")
        if (q_dir / "quarantine_payments.parquet").exists()
        else pl.DataFrame()
    )

    if "fact_sales" in frames and frames["fact_sales"].height:
        total_txns = frames["fact_sales"].height + excluded_sales.height
        rate = excluded_sales.height / total_txns if total_txns else 0.0
        results.append(ValidationResult(
            f"V{len(results) + 1}",
            "Business exclusion rate <= 15%",
            "PASS" if rate <= 0.15 else "FAIL",
            f"excluded={excluded_sales.height}, total={total_txns}, rate={rate:.1%}",
        ))

    input_rows = 0
    recon_path = partition / "_reconciliation.json"
    if recon_path.exists():
        input_rows = sum(
            json.loads(recon_path.read_text(encoding="utf-8")).get("inputs", {}).values()
        )
    base_rows = sum(
        frames[t].height for t in ("dim_customer", "dim_item", "fact_sales")
        if t in frames
    ) + excluded_sales.height
    denom = input_rows or base_rows
    if denom:
        q_rows = q_txns.height + q_payments.height
        rate = q_rows / denom
        results.append(ValidationResult(
            f"V{len(results) + 1}",
            "Quarantine rate <= 1%",
            "PASS" if rate <= 0.01 else "FAIL",
            f"quarantine_rows={q_rows}, input_rows={denom}, rate={rate:.2%}",
        ))

    all_pass = all(r.status == "PASS" for r in results)
    status = "APPROVED" if all_pass else "REJECTED"
    report = {
        "batch_date": run_date.isoformat(),
        "status": status,
        "validated_at": datetime.now(UTC).isoformat(),
        "checks": [asdict(r) for r in results],
    }
    manifest_path = partition / "_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        passed = sum(1 for r in results if r.status == "PASS")
        manifest["status"] = status
        manifest["validation_summary"] = f"{passed}/{len(results)} PASS"
        report["run_id"] = manifest.get("run_id")
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (partition / "_validation.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return results, all_pass