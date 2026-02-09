from __future__ import annotations

import argparse
import sys
from pathlib import Path

from retail_lakehouse.ingest.load_bronze import IngestConfig, ingest_bronze


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="retail_lakehouse")
    sub = p.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingest CSV extracts into DuckDB (bronze layer)")
    ingest.add_argument("--source", type=str, default="data/sample", help="Path to CSV folder")
    ingest.add_argument("--db", type=str, default="warehouse/retail.duckdb", help="Path to DuckDB file")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "ingest":
        cfg = IngestConfig(source_dir=Path(args.source), db_path=Path(args.db))
        ingest_bronze(cfg)
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
