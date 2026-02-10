from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from retail_lakehouse.ingest.load_bronze import IngestConfig, ingest_bronze
from retail_lakehouse.quality.checks import run_quality_checks


def test_quality_passes_on_sample(tmp_path: Path) -> None:
    db = tmp_path / "retail.duckdb"
    ingest_bronze(IngestConfig(source_dir=Path("data/sample"), db_path=db))

    run_quality_checks(db)


def test_quality_fails_on_duplicate_customer_id(tmp_path: Path) -> None:
    db = tmp_path / "retail.duckdb"
    ingest_bronze(IngestConfig(source_dir=Path("data/sample"), db_path=db))

    con = duckdb.connect(str(db))
    try:
        con.execute("INSERT INTO bronze.customers SELECT * FROM bronze.customers LIMIT 1;")
    finally:
        con.close()

    with pytest.raises(ValueError) as e:
        run_quality_checks(db)

    assert "customers.customer_id_unique" in str(e.value)
