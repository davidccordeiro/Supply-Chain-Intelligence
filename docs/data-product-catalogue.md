# Data Product Catalogue

---

## How to use this catalogue

This catalogue describes every data product available to internal
consumers. A "data product" is a stable, documented, tested dataset
that a team has committed to maintaining. Raw tables and staging
models are not data products — they are implementation details.

To request access or report an issue, contact the listed owner
or raise a ticket in Jira (DATA board).

---

## Supply Domain

**Domain owner:** Supply Chain Engineering Team  
**dbt project:** `supply`  
**Schemas:** `supply_staging`, `supply_mart`, `snapshots`

---

### dim_stores

| Property | Value |
|---|---|
| **Table** | `supply_mart.dim_stores` |
| **Owner** | Supply Chain Engineering |
| **Grain** | One row per store |
| **Refresh** | Daily |
| **SLA** | Available by 07:00 AEST |
| **Status** | Production |

**Description**  
Master store dimension. Contains all 400 store locations with
their geographic attributes, cluster classification, and
operational status.

**Key columns**

| Column | Type | Description |
|---|---|---|
| `store_id` | VARCHAR | Primary key. Format: STR0001 |
| `store_name` | VARCHAR | Display name |
| `state` | VARCHAR | NSW, VIC, QLD, WA, SA |
| `store_cluster` | VARCHAR | Demand cluster for forecasting |
| `cluster_tier` | INTEGER | 1 (metro large) to 6 (convenience) |
| `is_metro` | BOOLEAN | True for metro_large and metro_small |
| `is_active` | BOOLEAN | False = closed or temporarily inactive |
| `opened_date` | DATE | Store opening date |

**Common queries**
```sql
-- Active stores by state
SELECT state, COUNT(*) AS store_count
FROM supply_mart.dim_stores
WHERE is_active = TRUE
GROUP BY state;

-- Metro vs regional split
SELECT is_metro, COUNT(*) AS store_count
FROM supply_mart.dim_stores
GROUP BY is_metro;
```

---

### dim_products

| Property | Value |
|---|---|
| **Table** | `supply_mart.dim_products` |
| **Owner** | Supply Chain Engineering |
| **Grain** | One row per product × price version |
| **Refresh** | Daily |
| **SLA** | Available by 07:00 AEST |
| **Status** | Production |

**Description**  
Product dimension with full SCD Type 2 price history. Contains
every version of every product's price since January 2023.
Filter to `is_current_version = TRUE` for the current catalogue.

**Important:** Do not use this table for historical margin
analysis without filtering on the validity window. Always join
on `product_id AND sale_date BETWEEN valid_from AND valid_to`.

**Key columns**

| Column | Type | Description |
|---|---|---|
| `product_version_key` | VARCHAR | Surrogate key (unique per version) |
| `product_id` | VARCHAR | Natural key. Format: PRD0001 |
| `product_name` | VARCHAR | Display name |
| `category` | VARCHAR | One of 8 grocery categories |
| `perishability` | VARCHAR | Perishable or Non-Perishable |
| `unit_price` | DECIMAL | Price at this version |
| `cost_price` | DECIMAL | Cost at this version |
| `gross_margin_pct` | DECIMAL | (price - cost) / price × 100 |
| `valid_from` | DATE | When this price became effective |
| `valid_to` | DATE | When superseded (NULL = current) |
| `is_current_version` | BOOLEAN | True = currently active price |
| `supplier_name` | VARCHAR | Denormalised supplier name |
| `lead_time_days` | INTEGER | Supplier delivery lead time |

**Common queries**
```sql
-- Current product catalogue
SELECT product_id, product_name, category, unit_price
FROM supply_mart.dim_products
WHERE is_current_version = TRUE
  AND is_active = TRUE;

-- Products with price changes this year
SELECT product_id, product_name, COUNT(*) AS price_versions
FROM supply_mart.dim_products
GROUP BY product_id, product_name
HAVING COUNT(*) > 1
ORDER BY price_versions DESC;
```

---

### fct_inventory_snapshots

| Property | Value |
|---|---|
| **Table** | `supply_mart.fct_inventory_snapshots` |
| **Owner** | Supply Chain Engineering |
| **Grain** | One row per store × product × snapshot date |
| **Refresh** | Weekly (every Monday) |
| **SLA** | Available by 08:00 AEST Monday |
| **Status** | Production |

**Description**  
Weekly stock-on-hand fact table. The primary source for
stockout analysis, reorder decisions, and inventory valuation.
`stock_status` and `days_of_stock_remaining` are the two most
important columns for procurement teams.

**Key columns**

| Column | Type | Description |
|---|---|---|
| `store_id` | VARCHAR | FK to dim_stores |
| `product_id` | VARCHAR | FK to dim_products |
| `snapshot_date` | DATE | Date of stock count |
| `quantity_on_hand` | INTEGER | Units in stock |
| `reorder_point` | INTEGER | Trigger level for new order |
| `days_of_stock_remaining` | DECIMAL | qty / avg daily demand |
| `stock_status` | VARCHAR | stockout, critical, low, healthy |
| `inventory_value` | DECIMAL | qty × current cost price |
| `is_stockout` | BOOLEAN | quantity_on_hand = 0 |
| `is_below_reorder_point` | BOOLEAN | Below trigger but not zero |

**Common queries**
```sql
-- Current stockouts by category
SELECT category, COUNT(*) AS stockout_count
FROM supply_mart.fct_inventory_snapshots
WHERE snapshot_date = (SELECT MAX(snapshot_date)
                       FROM supply_mart.fct_inventory_snapshots)
  AND is_stockout = TRUE
GROUP BY category
ORDER BY stockout_count DESC;

-- Stores with most critical stock situations
SELECT store_id, COUNT(*) AS critical_products
FROM supply_mart.fct_inventory_snapshots
WHERE snapshot_date = (SELECT MAX(snapshot_date)
                       FROM supply_mart.fct_inventory_snapshots)
  AND stock_status IN ('stockout', 'critical')
GROUP BY store_id
ORDER BY critical_products DESC
LIMIT 20;
```

---

## Demand Domain

**Domain owner:** Demand Analytics Team  
**dbt project:** `demand`  
**Schemas:** `demand_staging`, `demand_mart`

---

### fct_sales

| Property | Value |
|---|---|
| **Table** | `demand_mart.fct_sales` |
| **Owner** | Demand Analytics |
| **Grain** | One row per sale transaction |
| **Refresh** | Daily |
| **SLA** | Available by 06:00 AEST |
| **Status** | Production |

**Description**  
Core sales fact table. Every POS transaction enriched with
store and product dimensions. Uses SCD Type 2 join to ensure
historical prices are used for margin calculations — not
today's price.

**Key columns**

| Column | Type | Description |
|---|---|---|
| `sale_key` | VARCHAR | Surrogate primary key |
| `store_id` | VARCHAR | FK to dim_stores |
| `product_id` | VARCHAR | FK to dim_products |
| `sale_date` | DATE | Transaction date |
| `quantity` | INTEGER | Units sold |
| `revenue` | DECIMAL | quantity × pos_unit_price |
| `gross_profit` | DECIMAL | revenue − (quantity × cost_price) |
| `gross_margin_pct` | DECIMAL | Historical margin at sale price |
| `price_variance` | DECIMAL | POS price − catalogue price |
| `store_cluster` | VARCHAR | From dim_stores |
| `category` | VARCHAR | From dim_products (SCD Type 2) |
| `is_weekend` | BOOLEAN | Saturday or Sunday sale |

---

### fct_stockouts

| Property | Value |
|---|---|
| **Table** | `demand_mart.fct_stockouts` |
| **Owner** | Demand Analytics |
| **Grain** | One row per store × product × stockout date |
| **Refresh** | Weekly |
| **SLA** | Available by 09:00 AEST Monday |
| **Status** | Production |

**Description**  
Stockout and low-stock events with estimated lost revenue.
Joins supply domain inventory data to demand domain sales
velocity to estimate the revenue impact of each stockout event.

**Key columns**

| Column | Type | Description |
|---|---|---|
| `store_id` | VARCHAR | FK to dim_stores |
| `product_id` | VARCHAR | FK to dim_products |
| `stockout_date` | DATE | Date of stockout snapshot |
| `estimated_lost_revenue_7d` | DECIMAL | Estimated 7-day revenue loss |
| `urgency` | VARCHAR | critical, high, medium, low |
| `lead_time_days` | INTEGER | Days to restock from supplier |
| `avg_daily_revenue` | DECIMAL | Baseline daily revenue for product |

---

### fct_forecasts

| Property | Value |
|---|---|
| **Table** | `demand.fct_forecasts` |
| **Owner** | Demand Analytics |
| **Grain** | One row per category × cluster × forecast date |
| **Refresh** | Weekly (every Monday) |
| **SLA** | Available by 10:00 AEST Monday |
| **Status** | Production |

**Description**  
12-week forward demand forecast from Prophet models.
See `forecasting/DATA_PRODUCT_README.md` for full methodology,
confidence interval interpretation, and known failure modes.

**Key columns**

| Column | Type | Description |
|---|---|---|
| `forecast_date` | DATE | Week being forecast |
| `category` | VARCHAR | Grocery category |
| `store_cluster` | VARCHAR | Store cluster type |
| `yhat` | DECIMAL | Point forecast (best estimate) |
| `yhat_lower` | DECIMAL | 80% CI lower bound |
| `yhat_upper` | DECIMAL | 80% CI upper bound (use for orders) |
| `trend` | DECIMAL | Underlying trend component |

---

### reorder_recommendations

| Property | Value |
|---|---|
| **Table** | `demand.reorder_recommendations` |
| **Owner** | Demand Analytics |
| **Grain** | One row per store × product |
| **Refresh** | Weekly (every Monday after forecasts) |
| **SLA** | Available by 11:00 AEST Monday |
| **Status** | Production |

**Description**  
Actionable reorder decisions derived from Prophet forecasts
and current inventory levels. This is the primary operational
output of the platform — procurement teams action this table
directly to raise purchase orders.

**Key columns**

| Column | Type | Description |
|---|---|---|
| `store_id` | VARCHAR | Store to reorder for |
| `product_id` | VARCHAR | Product to reorder |
| `should_reorder` | BOOLEAN | TRUE = raise a PO now |
| `recommended_order_qty` | DECIMAL | Units to order |
| `stock_status` | VARCHAR | Current urgency level |
| `days_of_stock_remaining` | DECIMAL | How long current stock lasts |
| `avg_weekly_forecast` | DECIMAL | Expected weekly demand |
| `safety_stock` | DECIMAL | Buffer above lead time demand |
| `supplier_name` | VARCHAR | Who to order from |

**Common queries**
```sql
-- Urgent orders to raise today
SELECT
    store_id,
    product_name,
    supplier_name,
    recommended_order_qty,
    stock_status
FROM demand.reorder_recommendations
WHERE should_reorder = TRUE
  AND stock_status IN ('stockout', 'critical')
ORDER BY
    CASE stock_status WHEN 'stockout' THEN 1 ELSE 2 END,
    recommended_order_qty DESC;
```

---

## Lineage overview
```
Source Systems
──────────────
POS Platform          →  raw.pos_sales
WMS                   →  raw.inventory_snapshots
ERP / Procurement     →  raw.purchase_orders
Product PIM           →  raw.products + raw.product_price_history
Store MDM             →  raw.stores
Supplier EDI          →  raw.suppliers

Supply Domain (dbt)
───────────────────
raw.*  →  supply_staging.*  →  supply_mart.dim_stores
                            →  supply_mart.dim_products      (SCD Type 2)
                            →  supply_mart.fct_inventory_snapshots
                            →  supply_mart.fct_purchase_orders

Demand Domain (dbt)
───────────────────
raw.pos_sales          →  demand_staging.stg_sales
supply_mart.dim_*      →  demand_mart.fct_sales              (cross-domain)
supply_mart.fct_inv_*  →  demand_mart.fct_stockouts          (cross-domain)

ML Layer (Prophet)
──────────────────
demand_mart.fct_sales  →  demand.fct_forecasts
                       →  demand.forecast_metadata
demand.fct_forecasts
+ supply_mart.fct_inv  →  demand.reorder_recommendations
```

---

