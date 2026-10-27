"""RenTech Stage 2 runner: bronze Parquet -> silver Parquet."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from .config import DEFAULT_DATA_DIR
from .transform import run_transform
from .validate_transform import run_validation

logger = logging.getLogger("rentech.run_transform")

def _configure_logging() -> None:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for handler in (
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_dir / "transform.log", encoding="utf-8"),
    ):
        handler.setFormatter(formatter)
        root.addHandler(handler)

def _latest_batch(data_dir: Path) -> date | None:
    raw = data_dir / "raw"
    if not raw.exists():
        return None
    dates = sorted(
        p.name.removeprefix("dt=") for p in raw.glob("dt=*") if p.is_dir()
    )
    return date.fromisoformat(dates[-1]) if dates else None

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="RenTech Stage 2: Transformation Runner")
    parser.add_argument("--date", help="Run date (YYYY-MM-DD), default: latest")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args(argv)

    _configure_logging()
    data_dir = Path(args.data_dir)
    run_date = (
        date.fromisoformat(args.date) if args.date else _latest_batch(data_dir)
    )
    if run_date is None:
        logger.error("No --date given and no data/raw/dt=* partition found.")
        return 2

    try:
        if not args.validate_only:
            result = run_transform(data_dir=data_dir, run_date=run_date)
            logger.info("Output partition: %s", result["partition"])
            logger.info("Row counts: %s", result["counts"])

        results, passed = run_validation(data_dir, run_date)
        for result in results:
            if result.status != "PASS":
                logger.error("%s FAIL: %s | %s", result.check_id, result.description, result.details)
        logger.info(
            "Validation %s (%d/%d PASS)",
            "APPROVED" if passed else "REJECTED",
            sum(1 for r in results if r.status == "PASS"),
            len(results),
        )
        return 0 if passed else 1
    except Exception:
        logger.exception("Stage 2 transformation failed")
        return 2

if __name__ == "__main__":
    raise SystemExit(main())