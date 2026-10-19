"""Extraction-only validation V1-V7.

Scope: did we get the right bytes from the source? Cleaning, standardization,
and business rules (dueDate >= rentalDate, negative fees, sale price,
Cancelled exclusion) belong to the Transformation stage and are NOT here.

Decisions:
  - no errors, no warnings            -> APPROVED
  - warnings only (dupes, quiet day)  -> MANUAL REVIEW
  - any error (null id, missing file) -> REJECTED (partial discard in run)
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

from pipeline import config as C

log = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    status: str = "APPROVED"          # APPROVED | MANUAL REVIEW | REJECTED
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)

    @property
    def valid(self) -> bool:
        return self.status != "REJECTED"


def _null_or_missing(df: pl.DataFrame, cols: list[str]) -> list[str]:
    out = []
    for c in cols:
        if c not in df.columns or df[c].null_count() > 0:
            out.append(c)
    return out


def check_ids_present(
    frames: dict[str, pl.DataFrame], res: ExtractionResult
) -> None:
    """V4: required PK/FK columns exist and contain no NULL values."""
    for table, cols in C.REQUIRED_IDS.items():
        bad = _null_or_missing(frames[table], cols)
        if bad:
            res.errors.append(f"V4 {table}: null/missing identifiers {bad}")


def check_pk_unique(
    frames: dict[str, pl.DataFrame], res: ExtractionResult
) -> None:
    """V5: duplicate primary keys are a WARNING (Transform dedupes on id)."""
    for table, df in frames.items():
        if "id" not in df.columns:
            continue
        dupes = df.height - df["id"].n_unique()
        if dupes:
            res.warnings.append(f"V5 {table}: {dupes} duplicate id (Transform dedupes)")


def check_files_complete(
    partition: Path, frames: dict[str, pl.DataFrame], res: ExtractionResult
) -> None:
    """V7: all four files exist with matching on-disk heights."""
    for table, df in frames.items():
        f = partition / f"{table}.parquet"
        if not f.exists():
            res.errors.append(f"V7 missing bronze file {f}")
            continue
        on_disk = pl.scan_parquet(str(f)).select(pl.len()).collect().item()
        if on_disk != df.height:
            res.errors.append(
                f"V7 {table}: disk={on_disk} memory={df.height} mismatch"
            )


def check_counts(partition: Path, res: ExtractionResult) -> None:
    """V6: compare to previous partition; 0 rows or >50% drop -> WARNING."""
    raw_root = partition.parent
    priors = sorted(p for p in raw_root.glob("dt=*") if p != partition)
    if not priors:
        if sum(res.counts.values()) == 0:
            res.warnings.append("V6 first-ever run produced 0 rows")
        return
    prev_counts = priors[-1] / "_counts.json"
    if not prev_counts.exists():
        return
    prior = json.loads(prev_counts.read_text(encoding="utf-8"))
    for table, n in res.counts.items():
        p = prior.get(table, 0)
        if p and n < p * 0.5:
            res.warnings.append(f"V6 {table}: {n} rows vs prior {p} (>50% drop)")


def validate_extraction(
    frames: dict[str, pl.DataFrame], partition: Path
) -> ExtractionResult:
    """Run V4-V7 on staged frames. V1-V3 raise during extract()."""
    res = ExtractionResult(counts={t: df.height for t, df in frames.items()})
    check_ids_present(frames, res)
    check_pk_unique(frames, res)
    check_files_complete(Path(partition), frames, res)
    check_counts(Path(partition), res)

    if res.errors:
        res.status = "REJECTED"
        for e in res.errors:
            log.error("Extraction check FAILED: %s", e)
    elif res.warnings:
        res.status = "MANUAL REVIEW"
        for w in res.warnings:
            log.warning("Extraction check WARNING: %s", w)
    else:
        log.info(
            "All extraction checks PASS (%d tables, %d rows)",
            len(frames), sum(res.counts.values()),
        )
    return res
