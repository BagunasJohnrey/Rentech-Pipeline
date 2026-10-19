"""Nightly extraction runner: Extract -> Stage raw -> Validate -> watermark.

Usage:
    python -m pipeline.run_extract --demo --date 2026-09-15
    python -m pipeline.run_extract                 # live (needs DATABASE_URL)
    python -m pipeline.run_extract --schedule      # 00:00 Asia/Manila

Exit codes: 0 = SUCCESS (bronze complete; watermark advanced on live runs),
            1 = REJECTED (partial partition deleted, watermark untouched).
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from pipeline import extract as E
from pipeline import validate_extract as V

load_dotenv()
log = logging.getLogger("rentech.extract")


def setup_logging(data_dir: Path, run_date: date) -> Path:
    log_dir = Path(data_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"pipeline_{run_date.isoformat()}.log"
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    for h in (
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ):
        h.setFormatter(fmt)
        root.addHandler(h)
    return log_file


def run_once(args: argparse.Namespace) -> int:
    run_date: date = args.date
    data_dir = Path(args.data_dir)
    run_at = datetime.now(timezone.utc)
    log_file = setup_logging(data_dir, run_date)
    live = not args.demo and E.get_database_url() is not None

    log.info("=== RenTech nightly extract dt=%s (mode=%s) ===",
             run_date.isoformat(), "live" if live else "demo")
    log.info("Run log: %s", log_file)

    # ---- Extract ---------------------------------------------------------
    if live:
        engine = E.create_db_engine()
        E.ping(engine)
        frames = E.extract_all(engine, data_dir)
    else:
        if not args.demo:
            log.warning("No DATABASE_URL/DIRECT_URL - falling back to --demo data")
        frames = E.generate_sample_data(
            n_customers=args.sample_customers,
            n_items=args.sample_items,
            n_transactions=args.sample_transactions,
            seed=args.seed,
            run_at=run_at,
            include_bad_rows=args.include_bad_rows,
        )

    # ---- Stage raw -------------------------------------------------------
    partition = E.stage_raw(frames, run_date, data_dir)
    log.info("Raw bronze staged: %s (%d rows total)",
             partition, sum(f.height for f in frames.values()))

    # ---- Validate V1-V7 --------------------------------------------------
    result = V.validate_extraction(frames, partition)
    if result.status == "REJECTED":
        shutil.rmtree(partition, ignore_errors=True)
        log.error("EXTRACTION REJECTED: %s - partial partition discarded, "
                  "watermark NOT advanced.", "; ".join(result.errors))
        return 1
    if result.status == "MANUAL REVIEW":
        log.warning("MANUAL REVIEW: %s - bronze kept for inspection.",
                    "; ".join(result.warnings))

    # ---- Advance watermark (live + success only) -------------------------
    if live:
        E.write_last_run(run_at, data_dir)
        log.info("Watermark advanced to %s", run_at.isoformat())

    log.info("=== Extract SUCCESS dt=%s raw=%s rows=%d status=%s log=%s ===",
             run_date.isoformat(), partition, sum(result.counts.values()),
             result.status, log_file)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RenTech extraction-only runner")
    p.add_argument("--date", type=date.fromisoformat, default=date.today(),
                   help="Run date YYYY-MM-DD (partition key)")
    p.add_argument("--data-dir", default=os.getenv("RENTECH_DATA_DIR", "data"))
    p.add_argument("--demo", action="store_true",
                   help="File-only demo (never touches the DB)")
    p.add_argument("--sample-customers", type=int, default=30)
    p.add_argument("--sample-items", type=int, default=25)
    p.add_argument("--sample-transactions", type=int, default=120)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--include-bad-rows", dest="include_bad_rows",
                   action="store_true", default=True,
                   help="Include 2 invalid rows to demo (default: on)")
    p.add_argument("--clean-sample", dest="include_bad_rows",
                   action="store_false",
                   help="Generate sample with zero invalid rows")
    p.add_argument("--schedule", action="store_true",
                   help="Run at 00:00 midnight via APScheduler")
    return p.parse_args(argv)


def _run_scheduled_job(args: argparse.Namespace) -> None:
    """Refresh the run date each night, then run one extraction."""
    nightly = argparse.Namespace(**vars(args))
    nightly.date = date.today()
    try:
        run_once(nightly)
    except Exception:  # keep the scheduler alive; the next run retries
        log.exception("Scheduled extraction failed; watermark untouched.")


def run_scheduled(args: argparse.Namespace) -> None:
    from apscheduler.schedulers.blocking import BlockingScheduler

    sched = BlockingScheduler(timezone="Asia/Manila")
    sched.add_job(_run_scheduled_job, "cron", hour=0, minute=0, args=[args],
                  id="rentech-nightly-extract", replace_existing=True)
    log.info("Scheduler armed: 00:00 midnight Asia/Manila. Press Ctrl+C to stop.")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.schedule:
        setup_logging(Path(args.data_dir), args.date)
        run_scheduled(args)
        return 0
    return run_once(args)


if __name__ == "__main__":
    raise SystemExit(main())
