
# Validates raw Parquet files before dbt runs

import pandas as pd
import duckdb
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any
import sys
import json
from datetime import datetime

DB_PATH  = Path("warehouse.db")
RAW_DIR  = Path("data/raw")
LOG_DIR  = Path("validation/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 1. Expectation primitives ────────────────────────────────────────────────────

# We implement a lightweight expectation runner on top of DuckDB.


@dataclass
class ExpectationResult:
    expectation: str
    table:       str
    column:      str
    passed:      bool
    details:     dict = field(default_factory=dict)

class RawValidator:
    """Runs SQL-based expectations against DuckDB raw schema."""

    def __init__(self, db_path: Path):
        self.con     = duckdb.connect(str(db_path))
        self.results: list[ExpectationResult] = []

    def _run(self, sql: str) -> Any:
        return self.con.execute(sql).fetchone()[0]

    def expect_no_nulls(self, table: str, column: str) -> ExpectationResult:
        null_count = self._run(f"""
            SELECT COUNT(*) FROM raw.{table}
            WHERE {column} IS NULL
        """)
        result = ExpectationResult(
            expectation = "expect_no_nulls",
            table       = table,
            column      = column,
            passed      = null_count == 0,
            details     = {"null_count": null_count},
        )
        self.results.append(result)
        return result

    def expect_unique(self, table: str, column: str) -> ExpectationResult:
        dup_count = self._run(f"""
            SELECT COUNT(*) FROM (
                SELECT {column} FROM raw.{table}
                GROUP BY {column}
                HAVING COUNT(*) > 1
            )
        """)
        result = ExpectationResult(
            expectation = "expect_unique",
            table       = table,
            column      = column,
            passed      = dup_count == 0,
            details     = {"duplicate_count": dup_count},
        )
        self.results.append(result)
        return result

    def expect_accepted_values(
        self, table: str, column: str, values: list[str]
    ) -> ExpectationResult:
        values_str  = ", ".join(f"'{v}'" for v in values)
        bad_count   = self._run(f"""
            SELECT COUNT(*) FROM raw.{table}
            WHERE {column} NOT IN ({values_str})
              AND {column} IS NOT NULL
        """)
        result = ExpectationResult(
            expectation = "expect_accepted_values",
            table       = table,
            column      = column,
            passed      = bad_count == 0,
            details     = {
                "bad_count":       bad_count,
                "accepted_values": values,
            },
        )
        self.results.append(result)
        return result

    def expect_values_between(
        self, table: str, column: str,
        min_val: float, max_val: float,
        mostly: float = 1.0
    ) -> ExpectationResult:
        total = self._run(f"SELECT COUNT(*) FROM raw.{table}")
        bad   = self._run(f"""
            SELECT COUNT(*) FROM raw.{table}
            WHERE {column} < {min_val}
               OR {column} > {max_val}
        """)
        pct_passing = (total - bad) / total if total > 0 else 1.0
        result = ExpectationResult(
            expectation = "expect_values_between",
            table       = table,
            column      = column,
            passed      = pct_passing >= mostly,
            details     = {
                "min_val":     min_val,
                "max_val":     max_val,
                "mostly":      mostly,
                "pct_passing": round(pct_passing, 4),
                "bad_count":   bad,
            },
        )
        self.results.append(result)
        return result

    def expect_row_count_between(
        self, table: str, min_rows: int, max_rows: int
    ) -> ExpectationResult:
        count = self._run(f"SELECT COUNT(*) FROM raw.{table}")
        result = ExpectationResult(
            expectation = "expect_row_count_between",
            table       = table,
            column      = "*",
            passed      = min_rows <= count <= max_rows,
            details     = {
                "row_count": count,
                "min_rows":  min_rows,
                "max_rows":  max_rows,
            },
        )
        self.results.append(result)
        return result

    def expect_referential_integrity(
        self, table: str, column: str,
        ref_table: str, ref_column: str
    ) -> ExpectationResult:
        orphan_count = self._run(f"""
            SELECT COUNT(*) FROM raw.{table} t
            WHERE t.{column} NOT IN (
                SELECT {ref_column} FROM raw.{ref_table}
            )
            AND t.{column} IS NOT NULL
        """)
        result = ExpectationResult(
            expectation = "expect_referential_integrity",
            table       = table,
            column      = f"{column} → {ref_table}.{ref_column}",
            passed      = orphan_count == 0,
            details     = {"orphan_count": orphan_count},
        )
        self.results.append(result)
        return result

    def expect_freshness(
        self, table: str, date_column: str, max_days_old: int
    ) -> ExpectationResult:
        days_old = self._run(f"""
            SELECT DATEDIFF('day', MAX({date_column}), CURRENT_DATE)
            FROM raw.{table}
        """)
        result = ExpectationResult(
            expectation = "expect_freshness",
            table       = table,
            column      = date_column,
            passed      = days_old <= max_days_old,
            details     = {
                "days_since_latest": days_old,
                "max_allowed":       max_days_old,
            },
        )
        self.results.append(result)
        return result


# 2. Expectation suites ────────────────────────────────────────────────────────

def validate_stores(v: RawValidator):
    print("stores")
    v.expect_row_count_between("stores", 350, 450)
    v.expect_no_nulls("stores", "store_id")
    v.expect_unique("stores", "store_id")
    v.expect_no_nulls("stores", "state")
    v.expect_accepted_values("stores", "state",
        ["NSW", "VIC", "QLD", "WA", "SA"])
    v.expect_accepted_values("stores", "store_cluster",
        ["metro_large", "metro_small", "suburban",
         "regional", "rural", "convenience"])


def validate_products(v: RawValidator):
    print("products")
    v.expect_row_count_between("products", 400, 600)
    v.expect_no_nulls("products", "product_id")
    v.expect_unique("products", "product_id")
    v.expect_accepted_values("products", "category",
        ["fresh_produce", "dairy", "bakery", "meat_seafood",
         "pantry", "frozen", "beverages", "health_beauty"])
    v.expect_values_between("products", "unit_price", 0.50, 100.00)
    v.expect_values_between("products", "cost_price", 0.10, 80.00)
    v.expect_referential_integrity(
        "products", "supplier_id", "suppliers", "supplier_id")


def validate_suppliers(v: RawValidator):
    print("suppliers")
    v.expect_row_count_between("suppliers", 40, 60)
    v.expect_no_nulls("suppliers", "supplier_id")
    v.expect_unique("suppliers", "supplier_id")
    v.expect_values_between("suppliers", "lead_time_days", 1, 21)
    v.expect_accepted_values("suppliers", "payment_terms",
        ["NET30", "NET60", "NET14", "COD"])


def validate_pos_sales(v: RawValidator):
    print("pos_sales")
    v.expect_row_count_between("pos_sales", 100_000, 10_000_000)
    v.expect_no_nulls("pos_sales", "sale_id")
    v.expect_no_nulls("pos_sales", "store_id")
    v.expect_no_nulls("pos_sales", "product_id")
    v.expect_no_nulls("pos_sales", "sale_date")
    v.expect_values_between("pos_sales", "quantity", 1, 500)
    v.expect_values_between("pos_sales", "total_amount",
        0.01, 50_000, mostly=0.999)
    v.expect_freshness("pos_sales", "sale_date", max_days_old=400)
    v.expect_referential_integrity(
        "pos_sales", "store_id", "stores", "store_id")
    v.expect_referential_integrity(
        "pos_sales", "product_id", "products", "product_id")


def validate_inventory(v: RawValidator):
    print("inventory_snapshots")
    v.expect_row_count_between("inventory_snapshots", 500_000, 20_000_000)
    v.expect_no_nulls("inventory_snapshots", "store_id")
    v.expect_no_nulls("inventory_snapshots", "product_id")
    v.expect_no_nulls("inventory_snapshots", "snapshot_date")
    v.expect_values_between(
        "inventory_snapshots", "quantity_on_hand", 0, 10_000)
    v.expect_freshness(
        "inventory_snapshots", "snapshot_date", max_days_old=400)


def validate_purchase_orders(v: RawValidator):
    print("purchase_orders")
    v.expect_row_count_between("purchase_orders", 1_000, 100_000)
    v.expect_no_nulls("purchase_orders", "po_id")
    v.expect_unique("purchase_orders", "po_id")
    v.expect_accepted_values("purchase_orders", "status",
        ["delivered", "in_transit", "pending", "cancelled"])
    v.expect_referential_integrity(
        "purchase_orders", "store_id", "stores", "store_id")
    v.expect_referential_integrity(
        "purchase_orders", "product_id", "products", "product_id")


# 3. Runner ────────────────────────────────────────────────────────────────────

def print_results(results: list[ExpectationResult]) -> bool:
    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed]

    print(f"\n{'─'*60}")
    print(f"  Results: {len(passed)} passed, {len(failed)} failed")
    print(f"{'─'*60}")

    if failed:
        print("Failed expectations:")
        for r in failed:
            print(f"     {r.table}.{r.column}")
            print(f"       {r.expectation}: {r.details}")

    print(f"\n  {'All expectations passed!' if not failed else 'Fix failures before running dbt'}")
    return len(failed) == 0


def save_log(results: list[ExpectationResult]):
    log = {
        "run_at":  datetime.now().isoformat(),
        "passed":  sum(1 for r in results if r.passed),
        "failed":  sum(1 for r in results if not r.passed),
        "results": [
            {
                "expectation": r.expectation,
                "table":       r.table,
                "column":      r.column,
                "passed":      r.passed,
                "details":     r.details,
            }
            for r in results
        ],
    }
    log_path = LOG_DIR / f"validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    log_path.write_text(json.dumps(log, indent=2))
    print(f"\n  Log saved to {log_path}")


def main():
    print("Raw Data Validation")
    print(f"    Database : {DB_PATH}")
    print(f"    Time     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not DB_PATH.exists():
        print("  ✗  warehouse.db not found — run setup_db.py first")
        sys.exit(1)

    v = RawValidator(DB_PATH)

    validate_stores(v)
    validate_suppliers(v)
    validate_products(v)
    validate_pos_sales(v)
    validate_inventory(v)
    validate_purchase_orders(v)

    all_passed = print_results(v.results)
    save_log(v.results)

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()