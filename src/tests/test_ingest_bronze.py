from __future__ import annotations

from pathlib import Path

import duckdb

from retail_lakehouse.ingest.load_bronze import IngestConfig, ingest_bronze


def test_ingest_creates_bronze_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "retail.duckdb"
    cfg = IngestConfig(source_dir=Path("data/sample"), db_path=db_path)

    ingest_bronze(cfg)

    con = duckdb.connect(str(db_path))
    try:
        tables = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'bronze'
            ORDER BY table_name;
            """
        ).fetchall()
        table_names = {t[0] for t in tables}

        assert table_names == {"customers", "order_items", "orders", "products"}

        for name in table_names:
            n = con.execute(f"SELECT COUNT(*) FROM bronze.{name};").fetchone()[0]
            assert n > 0
    finally:
        con.close()
