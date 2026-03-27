# Architecture Overview

---

## Problem statement

A national grocery retailer operates 400 stores with fragmented
data across POS systems, warehouse management, supplier EDI feeds,
and marketing platforms. The platform unifies these sources to
enable merchandising teams to:

1. Identify stockout patterns before they impact customers
2. Optimise reorder quantities using demand forecasting
3. Forecast demand by region and category for 12 weeks forward

---

## Architecture diagram
```
┌─────────────────────────────────────────────────────────────┐
│                      SOURCE SYSTEMS                          │
│  POS Platform  │  WMS  │  ERP  │  Product PIM  │  Store MDM │
└────────────────────────────┬────────────────────────────────┘
                             │ Airbyte (production)
                             │ setup_db.py (local dev)
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                    RAW SCHEMA (DuckDB)                        │
│  raw.pos_sales  │  raw.inventory_snapshots  │  raw.products  │
│  raw.stores     │  raw.suppliers            │  raw.purchase_ │
│                 │  raw.product_price_history│  orders        │
└──────────────────────┬──────────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          │ Great Expectations      │
          │ validate_raw.py         │
          └────────────┬────────────┘
                       │
          ┌────────────┴────────────────────────────────┐
          │              dbt MESH                        │
          │                                              │
          │  ┌─────────────────┐  ┌──────────────────┐  │
          │  │  SUPPLY DOMAIN  │  │  DEMAND DOMAIN   │  │
          │  │                 │  │                  │  │
          │  │  stg_stores     │  │  stg_sales       │  │
          │  │  stg_products   │  │                  │  │
          │  │  stg_suppliers  │  │  fct_sales ◄─────┼──┼─ ref('supply',
          │  │  stg_inventory  │  │  fct_stockouts ◄─┼──┼─  'dim_products')
          │  │  stg_po         │  │                  │  │
          │  │                 │  │                  │  │
          │  │  dim_stores ────┼──►                  │  │
          │  │  dim_products ──┼──►                  │  │
          │  │  fct_inventory ─┼──►                  │  │
          │  │  fct_po         │  │                  │  │
          │  └─────────────────┘  └──────────────────┘  │
          └─────────────────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          │    PROPHET ML LAYER     │
          │                         │
          │  train_forecast.py      │
          │  → fct_forecasts        │
          │  → forecast_metadata    │
          │                         │
          │  forecast_inventory.py  │
          │  → reorder_             │
          │    recommendations      │
          └────────────┬────────────┘
                       │
          ┌────────────┴────────────┐
          │  OBSERVABILITY          │
          │  Elementary anomaly     │
          │  detection on all marts │
          └─────────────────────────┘
```

---

## Technology decisions

| Layer | Tool | Why |
|---|---|---|
| Database | DuckDB | Zero-config, runs locally, Parquet-native, Snowflake-compatible SQL |
| Transformation | dbt Core | Industry standard, Mesh pattern for domain isolation |
| Ingestion | setup_db.py (Airbyte in prod) | Parquet → DuckDB without infrastructure |
| Validation | Custom GE-style validator | Lightweight, DuckDB-native, no server required |
| Observability | Elementary | Anomaly detection on metrics, integrates with dbt |
| Forecasting | Prophet | Interpretable, handles seasonality, retail-proven |
| Orchestration | run_pipeline.py (Prefect in prod) | Simple dependency chain, no scheduler required locally |
| CI/CD | GitHub Actions | Runs dbt test on every PR, free for public repos |

---

## Local development setup
```bash
# 1. Clone and install
git clone <repo>
pip install -r requirements.txt

# 2. Generate synthetic data
python generate_data.py

# 3. Load into DuckDB
python setup_db.py

# 4. Run supply domain
cd supply
dbt deps --profiles-dir .
dbt snapshot --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..

# 5. Run demand domain
cd demand
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..

# 6. Validate raw data
python validation/validate_raw.py

# 7. Run forecasting
python forecasting/train_forecast.py
python forecasting/forecast_inventory.py
python forecasting/visualise_forecasts.py


```

---
