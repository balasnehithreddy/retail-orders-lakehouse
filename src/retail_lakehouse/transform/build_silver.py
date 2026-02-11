from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class TransformConfig:
    db_path: Path


def build_silver(cfg: TransformConfig) -> None:
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(cfg.db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        con.execute("DROP TABLE IF EXISTS silver.order_items_enriched;")

        con.execute(
            """
            CREATE TABLE silver.order_items_enriched AS
            SELECT
                oi.order_id,
                oi.product_id,
                o.customer_id,

                oi.qty::INTEGER AS quantity,
		oi.unit_price::DOUBLE AS unit_price,
		(oi.qty::DOUBLE * oi.unit_price::DOUBLE) AS line_total,

                c.email,
                c.state,
                p.category,
                p.sku AS product_sku,

                o.order_ts::TIMESTAMP AS order_ts,
                c.created_at::TIMESTAMP AS customer_created_at
            FROM bronze.order_items oi
            JOIN bronze.orders o
                ON o.order_id = oi.order_id
            JOIN bronze.customers c
                ON c.customer_id = o.customer_id
            JOIN bronze.products p
                ON p.product_id = oi.product_id
            ;
            """
        )
    finally:
        con.close()

