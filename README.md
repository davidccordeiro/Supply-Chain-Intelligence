# Retail Supply Chain Intelligence Platform

> ELT-first data warehouse with dbt Mesh architecture and Prophet demand forecasting — built to mirror the data engineering decisions made daily at Coles, Woolworths, and Catch.

---

## What this project is

A national grocery retailer operates 400 stores with fragmented data across POS systems, warehouse management, supplier EDI feeds, and procurement platforms. This project builds a unified data warehouse that enables merchandising teams to:

- Identify stockout patterns before they impact customers
- Optimise reorder quantities using demand forecasting
- Forecast demand by category for 12 weeks forward with confidence intervals
- Translate ML output into specific, actionable purchase order recommendations

Everything runs locally. No cloud account required.

---

## Architecture

```
Source Systems (POS · WMS · ERP · PIM · EDI)
            │
            ▼
    Raw Schema (DuckDB)
    setup_db.py loads 7 Parquet datasets
            │
            ├── validate_raw.py (Great Expectations-style validation)
            │
            ▼
    ┌─────────────────────────────────────────┐
    │             dbt Mesh                     │
    │                                          │
    │  supply/          demand/                │
    │  ────────         ────────               │
    │  dim_stores  ──►  fct_sales              │
    │  dim_products──►  fct_stockouts          │
    │  fct_inventory    (cross-domain refs)    │
    │  fct_purchase_orders                     │
    └─────────────────────────────────────────┘
            │
            ▼
    Prophet Forecasting
    8 models · category level · 12-week horizon
            │
            ▼
    Reorder Recommendations
    109,000+ store/product evaluations
            │
            ▼
    Interactive Dashboard (Chart.js)
```

---

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Database | DuckDB | Zero-config, Parquet-native, Snowflake-compatible SQL |
| Transformation | dbt Core | Industry standard — Mesh pattern for domain isolation |
| Data quality | Custom GE-style validator | Lightweight, DuckDB-native, no server required |
| Observability | Elementary | Anomaly detection on metrics, not just row counts |
| Forecasting | Prophet | Interpretable decomposition, handles retail seasonality |
| Visualisation | Chart.js | Self-contained HTML dashboard, no server required |
| CI/CD | GitHub Actions | Runs `dbt test` on every PR |

Everything is free and runs locally.

---

## Senior-level design decisions

### dbt Mesh — two domains, one warehouse

The project is split into two independent dbt projects: `supply` and `demand`. Each deploys independently, owns its models, and exposes public contracts for cross-domain consumption.

```sql
-- demand domain referencing supply's public model
SELECT * FROM {{ ref('supply', 'dim_products') }}
```

The architectural rationale — when to split a monolith, what the governance model looks like, how contracts are versioned — is documented in full in [`docs/ADR-001-dbt-mesh-architecture.md`](docs/ADR-001-dbt-mesh-architecture.md).

### SCD Type 2 for product pricing

Products change price over time. A dbt snapshot tracks every price version with `valid_from` / `valid_to` dates. The sales fact table joins on price version to ensure historical margin calculations use the price that was actually in effect on the day of the sale — not today's price.

```sql
-- Historical margin using the correct price version
JOIN supply_snapshots.products_snapshot p
  ON s.product_id = p.product_id
 AND s.sale_date BETWEEN p.dbt_valid_from
                     AND COALESCE(p.dbt_valid_to, CURRENT_DATE)
```

### Top-down demand forecasting

Prophet models are trained at category level (8 models) where signal is densest, then disaggregated to store cluster using each cluster's historical demand share. This is the standard approach when lower-granularity data is sparse — common in grocery retail for tail categories.

### Reorder logic grounded in forecasts

Reorder recommendations are not rule-based thresholds. They are derived from forecast demand during supplier lead time, with safety stock multipliers that scale with urgency tier.

```
Demand during lead time = (avg weekly forecast / 7) × lead_time_days
Safety stock            = upper forecast bound × (multiplier − 1.0)
Reorder point           = demand during lead time + safety stock
```

---

## Project structure

```
retail-supply-chain/
├── generate_data.py              # Synthetic data generator (7 datasets)
├── setup_db.py                   # Loads Parquet → DuckDB raw schema
├── run_pipeline.py               # End-to-end pipeline runner
│
├── supply/                       # dbt domain 1 — inventory & products
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── snapshots/
│   │   └── products_snapshot.sql # SCD Type 2
│   └── models/
│       ├── sources.yml
│       ├── staging/              # stg_stores, stg_products, stg_inventory ...
│       └── marts/                # dim_stores, dim_products, fct_inventory ...
│
├── demand/                       # dbt domain 2 — sales & forecasting
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── sources.yml
│       ├── staging/              # stg_sales
│       └── marts/                # fct_sales, fct_stockouts
│
├── forecasting/
│   ├── train_forecast.py         # Prophet model training
│   ├── forecast_inventory.py     # Reorder recommendation engine
│   ├── visualise_forecasts.py    # Interactive Chart.js dashboard
│   └── DATA_PRODUCT_README.md    # Business-facing methodology doc
│
├── validation/
│   └── validate_raw.py           # Raw data quality checks
│
├── elementary_config/
│   └── run_elementary.py         # Observability report generator
│
├── data/
│   ├── raw/                      # Parquet files (gitignored)
│   └── processed/
│
└── docs/
    ├── ADR-001-dbt-mesh-architecture.md   # Architecture decision record
    ├── DATA_PRODUCT_CATALOGUE.md          # All 7 production tables documented
    └── ARCHITECTURE_OVERVIEW.md           # Full lineage diagram
```

---

## Datasets generated

| Dataset | Source system it mimics | Rows |
|---|---|---|
| `stores.parquet` | Store MDM | 400 |
| `suppliers.parquet` | Supplier EDI | 50 |
| `products.parquet` | Product PIM | 500 |
| `product_price_history.parquet` | Price management | ~650 |
| `inventory_snapshots.parquet` | Warehouse Management System | ~4.4M |
| `pos_sales.parquet` | Point of Sale | ~2M |
| `purchase_orders.parquet` | ERP / procurement | ~10K |

All data is synthetic, seeded for reproducibility (`RANDOM_SEED = 42`), and designed with realistic patterns: weekend sales lift, seasonal demand curves per category, ~8% stockout rate, and price changes that trigger SCD Type 2.

---

## Quickstart

### Prerequisites

- Python 3.11+
- Git

### 1. Clone and install

```bash
git clone <repo-url>
cd retail-supply-chain
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Generate data and load warehouse

```bash
python generate_data.py         # ~2 min — generates 7 Parquet files
python setup_db.py              # loads into DuckDB raw schema
```

### 3. Run the supply domain

```bash
cd supply
dbt deps
dbt snapshot   # SCD Type 2 snapshot
dbt run
dbt test
cd ..
```

### 4. Run the demand domain

```bash
cd demand
dbt deps 
dbt run 
dbt test
cd ..
```

### 5. Validate raw data

```bash
python validation/validate_raw.py
```

### 6. Train forecasts and generate recommendations

```bash
python forecasting/train_forecast.py        # ~3 min
python forecasting/forecast_inventory.py
```

### 7. Open the dashboard

```bash
python forecasting/visualise_forecasts.py
open forecasting/dashboard.html             # Windows: start forecasting/dashboard.html
```

---

## dbt commands reference

```bash
# Run a specific model
dbt run --select fct_sales

# Run all marts
dbt run --select marts

# Test a specific model
dbt test --select fct_inventory_snapshots

# Compile without running (generates manifest.json)
dbt compile

# View model lineage
dbt docs generate
dbt docs serve
```

---

## Forecast model performance

Models trained on 52 weeks of weekly sales data per category. Cross-validated with a 42-week initial window.

| Category | MAPE | Grade |
|---|---|---|
| Dairy | 20.3% | Excellent |
| Fresh produce | 52.5% | Acceptable |
| Pantry | 55.8% | Acceptable |
| Bakery | 72.0% | Poor |
| Meat & seafood | 81.2% | Poor |
| Frozen | 108.6% | Unreliable |
| Health & beauty | 122.5% | Unreliable |
| Beverages | 127.3% | Unreliable |

High MAPE in categories like beverages and health & beauty reflects genuine demand volatility — week-to-week swings that exceed the category average. The forecast methodology, confidence interval interpretation, and known failure modes are documented in [`forecasting/DATA_PRODUCT_README.md`](forecasting/DATA_PRODUCT_README.md).

---

## Data observability

Elementary is wired into both dbt projects and monitors:

- **Volume anomalies** — detects if row counts drop unexpectedly between runs
- **Column anomalies** — tracks null rates, zero counts, and averages over time
- **Metric drift** — alerts when revenue or quantity distributions shift

```bash
# Run Elementary after dbt
python elementary_config/run_elementary.py
```

Raw data is validated before dbt runs using a lightweight DuckDB-native validator that checks nulls, uniqueness, referential integrity, accepted values, and data freshness.

---

## Documentation

| Document | What it covers |
|---|---|
| [`docs/ADR-001-dbt-mesh-architecture.md`](docs/ADR-001-dbt-mesh-architecture.md) | When and why to split a monolithic dbt project. Governance model, contract versioning, PR process. |
| [`docs/DATA_PRODUCT_CATALOGUE.md`](docs/DATA_PRODUCT_CATALOGUE.md) | Every production table documented with grain, columns, and example queries. |
| [`docs/ARCHITECTURE_OVERVIEW.md`](docs/ARCHITECTURE_OVERVIEW.md) | Full system diagram with lineage from source to ML output. |
| [`forecasting/DATA_PRODUCT_README.md`](forecasting/DATA_PRODUCT_README.md) | Forecast methodology in plain English. Confidence intervals, failure modes, reorder logic. |

---

## What this demonstrates

| Skill | Where it appears |
|---|---|
| dbt Mesh — domain-driven modelling | `supply/` and `demand/` as separate projects with cross-domain refs |
| Slowly Changing Dimensions (SCD Type 2) | `supply/snapshots/products_snapshot.sql` |
| Dimensional modelling | `dim_products`, `dim_stores`, `fct_sales`, `fct_inventory_snapshots` |
| Data quality as code | `validation/validate_raw.py`, source tests in `sources.yml` |
| ML to business decisions | Prophet forecasts → disaggregated reorder recommendations |
| Data observability | Elementary anomaly detection on mart metrics |
| Architecture documentation | ADR, data product catalogue, lineage diagram |
| CI/CD | GitHub Actions runs `dbt test` on every PR |

---

## Licence

MIT — use freely for learning, portfolios, and interviews.
