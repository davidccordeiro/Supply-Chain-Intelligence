
# Trains Prophet demand forecasting models per category
# Writes forecasts and model metadata back to DuckDB


import pandas as pd
import numpy as np
import duckdb
import json
import warnings
from pathlib import Path
from datetime import datetime
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics

warnings.filterwarnings("ignore")

# 1. Configuration ─────────────────────────────────────────────────────────────

DB_PATH         = Path("warehouse.db")
FORECAST_WEEKS  = 12
MODEL_DIR       = Path("forecasting/models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

MIN_HISTORY_WEEKS = 26

AU_HOLIDAYS = pd.DataFrame({
    "holiday": [
        "New Year's Day", "Australia Day", "Good Friday",
        "Easter Monday", "Anzac Day", "Queen's Birthday",
        "Christmas Day", "Boxing Day",
    ],
    "ds": pd.to_datetime([
        "2023-01-01", "2023-01-26", "2023-04-07",
        "2023-04-10", "2023-04-25", "2023-06-12",
        "2023-12-25", "2023-12-26",
    ]),
    "lower_window": 0,
    "upper_window": 1,
})


# 2. Data loading ───────────────────────────────────────────────────────────────

def load_sales_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load weekly aggregated sales from the demand mart.
    We aggregate by category only — not category x cluster.

    Why: the 15% store/product sampling in generate_data.py means
    many category x cluster combinations have sparse, zero-heavy
    weekly data. Prophet with multiplicative seasonality explodes
    on sparse data. Aggregating by category gives ~8 dense time
    series instead of 48 sparse ones.
    """
    print("  Loading sales data from main_mart.fct_sales...")
    df = con.execute("""
        SELECT
            DATE_TRUNC('week', sale_date)   AS week_start,
            category,
            SUM(quantity)                   AS total_quantity,
            SUM(revenue)                    AS total_revenue,
            COUNT(DISTINCT store_id)        AS store_count
        FROM main_mart.fct_sales
        GROUP BY
            DATE_TRUNC('week', sale_date),
            category
        ORDER BY week_start
    """).df()

    df["week_start"] = pd.to_datetime(df["week_start"])

    # Remove partial first week (often low due to data cutoff)
    min_week = df["week_start"].min()
    df = df[df["week_start"] > min_week]

    print(f"  ✓  Loaded {len(df):,} weekly aggregates")
    print(f"     Categories : {df['category'].nunique()}")
    print(f"     Date range : {df['week_start'].min().date()} → "
          f"{df['week_start'].max().date()}")
    return df


# 3. Model training ─────────────────────────────────────────────────────────────

def train_prophet_model(
    segment_df: pd.DataFrame,
    category:   str,
) -> dict:

    prophet_df = segment_df.rename(columns={
        "week_start":     "ds",
        "total_quantity": "y",
    })[["ds", "y"]].copy()

    if len(prophet_df) < MIN_HISTORY_WEEKS:
        return {
            "status":   "skipped",
            "reason":   f"Only {len(prophet_df)} weeks of history",
            "category": category,
        }

    # Check for data quality issues before training
    zero_pct  = (prophet_df["y"] == 0).mean()
    if zero_pct > 0.3:
        return {
            "status":   "skipped",
            "reason":   f"Too many zero weeks ({zero_pct:.0%}) — sparse data",
            "category": category,
        }

    # ── Prophet config ─────────────────────────────────────────────────────────
    # ADDITIVE seasonality — critical fix.
    # Multiplicative mode scales seasonal swings proportionally to trend.
    # When data has zeros or near-zeros, multiplicative mode produces
    # astronomically large forecasts. Additive mode adds a fixed
    # seasonal component regardless of trend level — safe for all data.
    model = Prophet(
        seasonality_mode        = "additive",
        changepoint_prior_scale = 0.05,
        yearly_seasonality      = True,
        weekly_seasonality      = False,   # weekly grain — not needed
        daily_seasonality       = False,
        holidays                = AU_HOLIDAYS,
        interval_width          = 0.80,
    )

    model.add_seasonality(
        name          = "monthly",
        period        = 30.5,
        fourier_order = 3,
    )

    model.fit(prophet_df)

    future   = model.make_future_dataframe(periods=FORECAST_WEEKS, freq="W")
    forecast = model.predict(future)

    # ── Sanity cap ─────────────────────────────────────────────────────────────
    # Cap forecast at 3x historical max — prevents wild extrapolation.
    # If the model genuinely predicts growth beyond this, it's almost
    # certainly overfitting to a data artefact.
    hist_max = prophet_df["y"].max()
    cap      = hist_max * 3.0

    forecast["yhat"]       = forecast["yhat"].clip(lower=0, upper=cap)
    forecast["yhat_lower"] = forecast["yhat_lower"].clip(lower=0, upper=cap)
    forecast["yhat_upper"] = forecast["yhat_upper"].clip(lower=0, upper=cap)

    # ── Cross validation ───────────────────────────────────────────────────────
    # ── Cross validation ───────────────────────────────────────────────────────
    # With only 53 weeks of history, we need a large initial window.
    # initial = 42 weeks leaves 11 weeks for CV — enough for 2-3 folds.
    # Using a small initial window on short history produces wildly
    # inflated MAPE because early folds have almost no training data.
    
    try:
        cv_results = cross_validation(
            model,
            initial      = "294 days",   # 42 weeks — ~80% of history
            period       = "28 days",    # one fold per month
            horizon      = "28 days",    # evaluate 4 weeks ahead
            disable_tqdm = True,
        )
        metrics   = performance_metrics(cv_results)
        mape      = round(metrics["mape"].mean() * 100, 2)
        rmse      = round(metrics["rmse"].mean(), 2)
        cv_status = "success"
    except Exception as e:
        mape      = None
        rmse      = None
        cv_status = f"failed: {str(e)[:100]}"

    failure_modes = detect_failure_modes(prophet_df, mape, category)

    return {
        "status":        "success",
        "category":      category,
        "model":         model,
        "forecast":      forecast,
        "history":       prophet_df,
        "mape":          mape,
        "rmse":          rmse,
        "cv_status":     cv_status,
        "n_weeks":       len(prophet_df),
        "failure_modes": failure_modes,
    }


def detect_failure_modes(
    history:  pd.DataFrame,
    mape:     float | None,
    category: str,
) -> list[str]:
    modes = []

    if mape is not None and mape > 25:
        modes.append(
            f"High forecast error (MAPE {mape}%) — treat with caution."
        )
    if len(history) < 40:
        modes.append(
            f"Limited history ({len(history)} weeks). Higher uncertainty."
        )

    zero_pct = (history["y"] == 0).mean() * 100
    if zero_pct > 10:
        modes.append(
            f"Zero-sales rate {zero_pct:.0f}% — stockouts may suppress true demand."
        )
    if category == "fresh_produce":
        modes.append(
            "Fresh produce sensitive to weather/promotions not in model."
        )
    if not modes:
        modes.append("No known failure modes detected.")

    return modes


# 4. Persistence ────────────────────────────────────────────────────────────────

def write_forecasts_to_db(
    con:     duckdb.DuckDBPyConnection,
    results: list[dict],
) -> None:
    print("\n  Writing forecasts to DuckDB...")

    forecast_rows = []
    metadata_rows = []

    for r in results:
        if r["status"] != "success":
            continue

        forecast  = r["forecast"]
        hist_max  = r["history"]["ds"].max()
        future    = forecast[forecast["ds"] > hist_max]

        for _, row in future.iterrows():
            forecast_rows.append({
                "forecast_date": row["ds"].date(),
                "category":      r["category"],
                "store_cluster": "all",          # aggregated across clusters
                "yhat":          round(row["yhat"],       2),
                "yhat_lower":    round(row["yhat_lower"], 2),
                "yhat_upper":    round(row["yhat_upper"], 2),
                "trend":         round(row["trend"],      2),
                "created_at":    datetime.now(),
            })

        metadata_rows.append({
            "category":      r["category"],
            "store_cluster": "all",
            "mape":          r["mape"],
            "rmse":          r["rmse"],
            "cv_status":     r["cv_status"],
            "n_weeks":       r["n_weeks"],
            "failure_modes": json.dumps(r["failure_modes"]),
            "created_at":    datetime.now(),
        })

    forecast_df = pd.DataFrame(forecast_rows)
    metadata_df = pd.DataFrame(metadata_rows)

    con.execute("CREATE SCHEMA IF NOT EXISTS demand")
    con.execute("DROP TABLE IF EXISTS demand.fct_forecasts")
    con.execute("DROP TABLE IF EXISTS demand.forecast_metadata")
    con.execute("CREATE TABLE demand.fct_forecasts AS SELECT * FROM forecast_df")
    con.execute("CREATE TABLE demand.forecast_metadata AS SELECT * FROM metadata_df")

    print(f"  ✓  demand.fct_forecasts    ({len(forecast_df):,} rows)")
    print(f"  ✓  demand.forecast_metadata ({len(metadata_df):,} rows)")


def print_model_summary(results: list[dict]) -> None:
    print(f"\n{'─'*60}")
    print(f"  {'Category':<25} {'MAPE':>8} {'Weeks':>6} {'Status'}")
    print(f"{'─'*60}")
    for r in sorted(results, key=lambda x: x.get("mape") or 999):
        if r["status"] == "skipped":
            print(f"  {r['category']:<25} {'—':>8} {'—':>6}  skipped")
        else:
            mape_str = f"{r['mape']}%" if r["mape"] else "CV failed"
            print(f"  {r['category']:<25} {mape_str:>8} {r['n_weeks']:>6}  ✓")
    print(f"{'─'*60}")


# 5. Main ───────────────────────────────────────────────────────────────────────

def main():
    print("Prophet Demand Forecasting Pipeline")
    print(f"    Database        : {DB_PATH}")
    print(f"    Forecast horizon: {FORECAST_WEEKS} weeks")
    print(f"    Seasonality     : additive")
    print(f"    Segmentation    : category only (8 models)\n")

    con   = duckdb.connect(str(DB_PATH))
    sales = load_sales_data(con)

    categories = sales["category"].unique()
    print(f"  Training {len(categories)} models...\n")

    results = []
    for i, category in enumerate(categories, 1):
        segment_df = sales[sales["category"] == category].copy()
        print(f"  [{i}/{len(categories)}] {category} "
              f"({len(segment_df)} weeks)...", end=" ", flush=True)

        result = train_prophet_model(segment_df, category)
        results.append(result)

        if result["status"] == "success":
            print(f"MAPE: {result['mape']}%")
        else:
            print(f"SKIPPED — {result['reason']}")

    print_model_summary(results)
    write_forecasts_to_db(con, results)
    con.close()

    successful = sum(1 for r in results if r["status"] == "success")
    print(f"Done — {successful} models trained\n")


if __name__ == "__main__":
    main()