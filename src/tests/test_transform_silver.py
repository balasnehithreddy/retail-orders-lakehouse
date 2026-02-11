from __future__ import annotations

from pathlib import Path

import duckdb

from retail_lakehouse.ingest.load_bronze import IngestConfig, ingest_bronze
from retail_lakehouse.transform.build_silver import TransformConfig, build_silver


def test_build_silver_creates_table_and_rows(tmp_path: Path) -> None:
    db = tmp_path / "retail.duckdb"
    ingest_bronze(IngestConfig(source_dir=Path("data/sample"), db_path=db))

    build_silver(TransformConfig(db_path=db))

    con = duckdb.connect(str(db))
    try:
        tables = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'silver'
            ORDER BY table_name;
            """
        ).fetchall()
        assert ("order_items_enriched",) in tables

        silver_rows = con.execute("SELECT COUNT(*) FROM silver.order_items_enriched;").fetchone()[0]
        bronze_rows = con.execute("SELECT COUNT(*) FROM bronze.order_items;").fetchone()[0]
        assert silver_rows == bronze_rows

        # Sanity: no null keys
        nulls = con.execute(
            """
            SELECT COUNT(*)
            FROM silver.order_items_enriched
            WHERE order_id IS NULL OR product_id IS NULL OR customer_id IS NULL;
            """
        ).fetchone()[0]
        assert nulls == 0

        # Sanity: product_sku exists and non-empty
        empty_sku = con.execute(
            """
            SELECT COUNT(*)
            FROM silver.order_items_enriched
            WHERE product_sku IS NULL OR product_sku = '';
            """
        ).fetchone()[0]
        assert empty_sku == 0
    finally:
        con.close()

