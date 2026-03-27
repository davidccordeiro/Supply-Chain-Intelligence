# Translates Prophet forecasts into actionable reorder decisions
# Reads from demand.fct_forecasts and main_mart.fct_inventory_snapshots
# Writes demand.reorder_recommendations to DuckDB


import pandas as pd
import duckdb
from pathlib import Path
from datetime import datetime

DB_PATH = Path("warehouse.db")

# Safety stock multiplier by urgency tier
# Higher = more conservative (more stock held as buffer)
# Lower  = leaner (less working capital tied up in inventory)
SAFETY_STOCK_MULTIPLIER = {
    "critical": 1.5,   # long lead time suppliers — hold extra buffer
    "high":     1.3,
    "medium":   1.1,
    "low":      1.0,
}


def load_data(con: duckdb.DuckDBPyConnection) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("  Loading forecasts and inventory...")

    forecasts = con.execute("""
        SELECT
            forecast_date,
            category,
            store_cluster,
            yhat            AS forecast_qty,
            yhat_lower      AS forecast_lower,
            yhat_upper      AS forecast_upper
        FROM demand.fct_forecasts
        ORDER BY category, store_cluster, forecast_date
    """).df()

    inventory = con.execute("""
        SELECT
            i.store_id,
            i.product_id,
            i.snapshot_date,
            i.category,
            i.store_cluster,
            i.quantity_on_hand,
            i.reorder_point,
            i.reorder_qty,
            i.stock_status,
            i.days_of_stock_remaining,
            i.lead_time_days,
            i.product_name,
            i.supplier_name
        FROM main_mart.fct_inventory_snapshots i
        WHERE i.snapshot_date = (
            SELECT MAX(snapshot_date)
            FROM main_mart.fct_inventory_snapshots
        )
    """).df()

    print(f"Forecasts : {len(forecasts):,} rows")
    print(f"Inventory : {len(inventory):,} rows")
    return forecasts, inventory


def calculate_reorder_recommendations(
    forecasts: pd.DataFrame,
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each store/product combination:
    1. Look up the forecast for the next lead_time_days
    2. Compare expected demand against current stock
    3. Recommend reorder quantity if stock will run out
    """
    print("Calculating reorder recommendations...")

    # Aggregate forecast to weekly demand per category/cluster
    weekly_forecast = (
        forecasts
        .groupby(["category", "store_cluster"])
        .agg(
            avg_weekly_forecast = ("forecast_qty",   "mean"),
            max_weekly_forecast = ("forecast_qty",   "max"),
            forecast_upper_avg  = ("forecast_upper", "mean"),
        )
        .reset_index()
    )

    # Join inventory to forecast by category + cluster
    merged = inventory.merge(
        weekly_forecast,
        on  = ["category", "store_cluster"],
        how = "left",
    )

    recommendations = []
    for _, row in merged.iterrows():

        # Expected demand during lead time
        # Using upper forecast bound for safety stock calc
        daily_demand    = row["avg_weekly_forecast"] / 7
        daily_demand_ub = row["forecast_upper_avg"]  / 7

        demand_during_lead_time = daily_demand    * row["lead_time_days"]
        demand_upper_bound      = daily_demand_ub * row["lead_time_days"]

        # Safety stock = extra buffer above expected demand during lead time
        multiplier    = SAFETY_STOCK_MULTIPLIER.get(row["stock_status"], 1.0)
        safety_stock  = demand_upper_bound * (multiplier - 1.0)

        # Reorder point = demand during lead time + safety stock
        calc_reorder_point = demand_during_lead_time + safety_stock

        # Should we reorder?
        should_reorder = (
            row["quantity_on_hand"] <= calc_reorder_point
            or row["stock_status"] in ("stockout", "critical")
        )

        # Recommended order quantity
        # Target: 4 weeks of average demand above reorder point
        if should_reorder:
            target_stock   = daily_demand * 28   # 4 weeks
            reorder_qty    = max(
                0,
                target_stock - row["quantity_on_hand"] + safety_stock
            )
            reorder_qty    = round(reorder_qty, 0)
        else:
            reorder_qty    = 0

        recommendations.append({
            "store_id":                row["store_id"],
            "product_id":              row["product_id"],
            "product_name":            row["product_name"],
            "supplier_name":           row["supplier_name"],
            "category":                row["category"],
            "store_cluster":           row["store_cluster"],
            "current_stock":           row["quantity_on_hand"],
            "stock_status":            row["stock_status"],
            "days_of_stock_remaining": row["days_of_stock_remaining"],
            "lead_time_days":          row["lead_time_days"],
            "avg_weekly_forecast":     round(row["avg_weekly_forecast"] or 0, 1),
            "demand_during_lead_time": round(demand_during_lead_time, 1),
            "safety_stock":            round(safety_stock, 1),
            "calc_reorder_point":      round(calc_reorder_point, 1),
            "should_reorder":          should_reorder,
            "recommended_order_qty":   reorder_qty,
            "created_at":              datetime.now(),
        })

    df = pd.DataFrame(recommendations)
    reorder_count = df["should_reorder"].sum()
    print(f"  ✓  {len(df):,} store/product combinations evaluated")
    print(f"  ✓  {reorder_count:,} reorder recommendations generated")
    return df


def write_recommendations(
    con: duckdb.DuckDBPyConnection,
    df:  pd.DataFrame,
) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS demand")
    con.execute("DROP TABLE IF EXISTS demand.reorder_recommendations")
    con.execute("""
        CREATE TABLE demand.reorder_recommendations AS
        SELECT * FROM df
    """)
    print(f"\n  ✓  demand.reorder_recommendations written ({len(df):,} rows)")


def print_top_recommendations(con: duckdb.DuckDBPyConnection) -> None:
    print("\n  Top 10 urgent reorder recommendations:")
    print(f"  {'Product':<30} {'Store':<10} {'Status':<10} "
          f"{'Stock':>6} {'Order Qty':>10}")
    print(f"  {'─'*30} {'─'*10} {'─'*10} {'─'*6} {'─'*10}")

    top = con.execute("""
        SELECT
            product_name,
            store_id,
            stock_status,
            current_stock,
            recommended_order_qty
        FROM demand.reorder_recommendations
        WHERE should_reorder = TRUE
        ORDER BY
            CASE stock_status
                WHEN 'stockout'  THEN 1
                WHEN 'critical'  THEN 2
                WHEN 'low'       THEN 3
                ELSE 4
            END,
            recommended_order_qty DESC
        LIMIT 10
    """).df()

    for _, row in top.iterrows():
        print(f"  {str(row['product_name']):<30} "
              f"{str(row['store_id']):<10} "
              f"{str(row['stock_status']):<10} "
              f"{int(row['current_stock']):>6} "
              f"{int(row['recommended_order_qty']):>10}")


def main():
    print("Inventory Reorder Recommendation Engine")
    print(f"Database: {DB_PATH}\n")

    con = duckdb.connect(str(DB_PATH))
    forecasts, inventory = load_data(con)
    recommendations      = calculate_reorder_recommendations(forecasts, inventory)
    write_recommendations(con, recommendations)
    print_top_recommendations(con)
    con.close()

    print(f"Reorder recommendations complete")
    print(f"Query demand.reorder_recommendations for full results\n")


if __name__ == "__main__":
    main()