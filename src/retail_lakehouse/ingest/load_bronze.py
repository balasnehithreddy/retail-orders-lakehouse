from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class IngestConfig:
    source_dir: Path
    db_path: Path


TABLE_FILES: dict[str, str] = {
    "customers": "customers.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_items": "order_items.csv",
}


def ingest_bronze(cfg: IngestConfig) -> None:
    if not cfg.source_dir.exists():
        raise FileNotFoundError(f"Source directory not found: {cfg.source_dir}")

    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(cfg.db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

        for table, filename in TABLE_FILES.items():
            csv_path = cfg.source_dir / filename
            if not csv_path.exists():
                raise FileNotFoundError(f"Missing required file: {csv_path}")

            # Day 1 idempotency: recreate tables each run.
            con.execute(f"DROP TABLE IF EXISTS bronze.{table};")
            con.execute(
                f"""
                CREATE TABLE bronze.{table} AS
                SELECT * FROM read_csv_auto('{csv_path.as_posix()}', header=true);
                """
            )
    finally:
        con.close()
