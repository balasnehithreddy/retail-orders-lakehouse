.PHONY: setup lint test ingest-sample

setup:
	python3 -m venv .venv || true
	. .venv/bin/activate && python -m pip install --upgrade pip && pip install duckdb pandas pytest ruff

lint:
	. .venv/bin/activate && ruff check src

test:
	. .venv/bin/activate && pytest -q

ingest-sample:
	. .venv/bin/activate && python -m retail_lakehouse.cli ingest --source data/sample --db warehouse/retail.duckdb
