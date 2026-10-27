"""RenTech extraction-only pipeline (Stage 1: Extraction).

Reads four Supabase Postgres tables incrementally and stages dated bronze
Parquet under data/raw/dt=YYYY-MM-DD/. This package performs extraction and
extraction-level validation only. Cleaning, joins, aggregation, mart load,
and forecasting belong to later stages and must not be added here.
"""

__version__ = "0.1.0"
__all__ = ["config", "extract", "validate_extract", "run_extract", "transform", "validate_transform", "run_transform"]