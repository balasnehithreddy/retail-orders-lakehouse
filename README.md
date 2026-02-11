retail-orders-lakehouse

Problem
Build a repeatable analytics ingestion pipeline for daily retail CSV extracts.
The system lands raw vendor files into a queryable warehouse and prepares the foundation for validation and modeling.

Architecture (Day 1)
Raw CSV extracts -> DuckDB bronze schema (landing zone, minimal transformation)

How to run locally
  make setup
  make lint
  make test
  make ingest-sample
  make transform

Data quality gates
Run fail-fast checks against the bronze layer:
  make quality
Architecture (Day 3)
Bronze -> Silver: silver.order_items_enriched (joins orders, customers, products, order_items)

Checks enforced:
- Not-null: primary/foreign keys across customers, products, orders, order_items
- Uniqueness: customers.customer_id, products.product_id, orders.order_id, and (order_id, product_id) in order_items
- Referential integrity: orders.customer_id -> customers, order_items.order_id -> orders, order_items.product_id -> products

Behavior:
- If any rule fails, the command raises an error and exits non-zero.

