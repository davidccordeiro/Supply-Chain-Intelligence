# setup_db.py
# Loads all Parquet files from data/raw/ into DuckDB raw schema
# Usage: python setup_db.py

import duckdb
from pathlib import Path

DB_PATH  = Path("warehouse.db")
RAW_DIR  = Path("data/raw")

TABLES = {
    "stores":                "stores.parquet",
    "suppliers":             "suppliers.parquet",
    "products":              "products.parquet",
    "product_price_history": "product_price_history.parquet",
    "inventory_snapshots":   "inventory_snapshots.parquet",
    "pos_sales":             "pos_sales.parquet",
    "purchase_orders":       "purchase_orders.parquet",
}

def setup_database():
    print(" DuckDB Warehouse Setup")
    print(f"    Database : {DB_PATH}")
    print(f"    Raw dir  : {RAW_DIR}\n")

    con = duckdb.connect(str(DB_PATH))

    # ── Create schemas ─────────────────────────────────────────────────────────
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS supply")
    con.execute("CREATE SCHEMA IF NOT EXISTS demand")
    print("Schema creation completed\n")

    # ── Load Parquet files into raw schema ─────────────────────────────────────
    print("Loading Parquet files into raw schema...")
    for table_name, filename in TABLES.items():
        parquet_path = RAW_DIR / filename

        if not parquet_path.exists():
            print(f"MISSING: {filename} — run generate_data.py first")
            continue

        con.execute(f"""
            CREATE OR REPLACE TABLE raw.{table_name} AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)

        row_count = con.execute(
            f"SELECT COUNT(*) FROM raw.{table_name}"
        ).fetchone()[0]

        print(f"  ✓  raw.{table_name:<30} {row_count:>10,} rows")

    # ── Validation queries ─────────────────────────────────────────────────────
    # Spot-check referential integrity before dbt even runs.
    print("\nRunning validation checks...")

    checks = [
        (
            "Sales with valid store",
            """
            SELECT COUNT(*) FROM raw.pos_sales s
            WHERE s.store_id IN (SELECT store_id FROM raw.stores)
            """,
        ),
        (
            "Sales with valid product",
            """
            SELECT COUNT(*) FROM raw.pos_sales s
            WHERE s.product_id IN (SELECT product_id FROM raw.products)
            """,
        ),
        (
            "Inventory stockout rate",
            """
            SELECT ROUND(
                100.0 * SUM(CASE WHEN quantity_on_hand = 0 THEN 1 ELSE 0 END)
                / COUNT(*), 2
            ) AS pct_stockout
            FROM raw.inventory_snapshots
            """,
        ),
        (
            "Products with price changes",
            """
            SELECT COUNT(DISTINCT product_id)
            FROM raw.product_price_history
            GROUP BY product_id
            HAVING COUNT(*) > 1
            """,
        ),
    ]

    for label, query in checks:
        result = con.execute(query).fetchone()[0]
        print(f"  ✓  {label:<35} {result:>12,}")

    con.close()
    print(f"Database ready at {DB_PATH}")


if __name__ == "__main__":
    setup_database()