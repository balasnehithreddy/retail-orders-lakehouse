# retail-orders-lakehouse

## Problem
Build a repeatable analytics ingestion pipeline for daily retail CSV extracts.
The system lands raw vendor files into a queryable warehouse and prepares the foundation for validation and modeling.

## Architecture (Day 1)
Raw CSV extracts → DuckDB `bronze` schema (landing zone, minimal transformation)

## How to run locally
```bash
make setup
make lint
make test
make ingest-sample
