from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class CheckResult:
    name: str
    failing_rows: int


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def run_quality_checks(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB not found: {db_path}")

    con = duckdb.connect(str(db_path))
    try:
        checks: list[CheckResult] = []

        # Not-null
        checks += [
            CheckResult(
                "customers.customer_id_not_null",
                _scalar_int(con, "SELECT COUNT(*) FROM bronze.customers WHERE customer_id IS NULL"),
            ),
            CheckResult(
                "products.product_id_not_null",
                _scalar_int(con, "SELECT COUNT(*) FROM bronze.products WHERE product_id IS NULL"),
            ),
            CheckResult(
                "orders.order_id_not_null",
                _scalar_int(con, "SELECT COUNT(*) FROM bronze.orders WHERE order_id IS NULL"),
            ),
            CheckResult(
                "orders.customer_id_not_null",
                _scalar_int(con, "SELECT COUNT(*) FROM bronze.orders WHERE customer_id IS NULL"),
            ),
            CheckResult(
                "order_items.order_id_not_null",
                _scalar_int(con, "SELECT COUNT(*) FROM bronze.order_items WHERE order_id IS NULL"),
            ),
            CheckResult(
                "order_items.product_id_not_null",
                _scalar_int(con, "SELECT COUNT(*) FROM bronze.order_items WHERE product_id IS NULL"),
            ),
        ]

        # Uniqueness
        checks += [
            CheckResult(
                "customers.customer_id_unique",
                _scalar_int(
                    con,
                    "SELECT COUNT(*) FROM (SELECT customer_id FROM bronze.customers GROUP BY 1 HAVING COUNT(*)>1)",
                ),
            ),
            CheckResult(
                "products.product_id_unique",
                _scalar_int(
                    con,
                    "SELECT COUNT(*) FROM (SELECT product_id FROM bronze.products GROUP BY 1 HAVING COUNT(*)>1)",
                ),
            ),
            CheckResult(
                "orders.order_id_unique",
                _scalar_int(
                    con,
                    "SELECT COUNT(*) FROM (SELECT order_id FROM bronze.orders GROUP BY 1 HAVING COUNT(*)>1)",
                ),
            ),
            CheckResult(
                "order_items.order_id_product_id_unique",
                _scalar_int(
                    con,
                    "SELECT COUNT(*) FROM (SELECT order_id, product_id FROM bronze.order_items GROUP BY 1,2 HAVING COUNT(*)>1)",
                ),
            ),
        ]

        # Referential integrity
        checks += [
            CheckResult(
                "orders.customer_id_fk_customers",
                _scalar_int(
                    con,
                    """
                    SELECT COUNT(*)
                    FROM bronze.orders o
                    LEFT JOIN bronze.customers c ON o.customer_id = c.customer_id
                    WHERE c.customer_id IS NULL
                    """,
                ),
            ),
            CheckResult(
                "order_items.order_id_fk_orders",
                _scalar_int(
                    con,
                    """
                    SELECT COUNT(*)
                    FROM bronze.order_items oi
                    LEFT JOIN bronze.orders o ON oi.order_id = o.order_id
                    WHERE o.order_id IS NULL
                    """,
                ),
            ),
            CheckResult(
                "order_items.product_id_fk_products",
                _scalar_int(
                    con,
                    """
                    SELECT COUNT(*)
                    FROM bronze.order_items oi
                    LEFT JOIN bronze.products p ON oi.product_id = p.product_id
                    WHERE p.product_id IS NULL
                    """,
                ),
            ),
        ]

        failures = [c for c in checks if c.failing_rows > 0]
        if failures:
            msg = "Data quality failed:\n" + "\n".join(
                f"- {c.name}: failing_rows={c.failing_rows}" for c in failures
            )
            raise ValueError(msg)
    finally:
        con.close()
