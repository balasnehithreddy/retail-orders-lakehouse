from __future__ import annotations
from retail_lakehouse.transform.build_silver import TransformConfig, build_silver

import argparse
import sys
from pathlib import Path

from retail_lakehouse.quality.checks import run_quality_checks

from retail_lakehouse.ingest.load_bronze import IngestConfig, ingest_bronze


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="retail_lakehouse")
    sub = p.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingest CSV extracts into DuckDB (bronze layer)")
    ingest.add_argument("--source", type=str, default="data/sample", help="Path to CSV folder")
    ingest.add_argument("--db", type=str, default="warehouse/retail.duckdb", help="Path to DuckDB file")
    quality = sub.add_parser("quality", help="Run fail-fast data quality checks against bronze")
    quality.add_argument("--db", type=str, default="warehouse/retail.duckdb", help="Path to DuckDB file")
    transform = sub.add_parser("transform", help="Build silver modeled tables from bronze")
    transform.add_argument("--db", type=str, default="warehouse/retail.duckdb", help="Path to DuckDB file")

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "ingest":
        cfg = IngestConfig(source_dir=Path(args.source), db_path=Path(args.db))
        ingest_bronze(cfg)
        return 0
    if args.command == "quality":
        run_quality_checks(Path(args.db))
        return 0

    if args.command == "transform":
        build_silver(TransformConfig(db_path=Path(args.db)))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
