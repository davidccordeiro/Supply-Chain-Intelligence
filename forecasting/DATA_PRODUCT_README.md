# Data Product: Weekly Demand Forecast
**Owner:** Demand Analytics Team  
**Last Updated:** 2024-01-01  
**Status:** Production  
**Refresh Cadence:** Weekly (every Monday 06:00 AEST)

---

## What is this data product?

This data product provides a **12-week forward demand forecast** for every
grocery category across six store cluster types. It is designed to answer
one question for the merchandising and supply chain teams:

> *"How much of each category should we expect to sell over the next
> 12 weeks, and how confident are we in that estimate?"*

The forecast feeds directly into the **Reorder Recommendation Engine**,
which translates predicted demand into specific purchase order quantities
per store and product.

---

## How to use this data

### For merchandising analysts
Query `demand.fct_forecasts` for the forward-looking forecast:
```sql
SELECT
    forecast_date,
    category,
    store_cluster,
    yhat            AS forecast_units,
    yhat_lower      AS low_estimate,
    yhat_upper      AS high_estimate
FROM demand.fct_forecasts
WHERE forecast_date >= CURRENT_DATE
  AND category       = 'fresh_produce'
ORDER BY forecast_date;
```

**Which number should I use?**
- `yhat` — best estimate. Use for planning and budgeting.
- `yhat_lower` — optimistic scenario (80% confidence lower bound).
  Use when you want to minimise overstock risk.
- `yhat_upper` — conservative scenario (80% confidence upper bound).
  Use for safety stock calculations. **The reorder engine uses this.**

### For supply chain planners
Query `demand.reorder_recommendations` for specific order actions:
```sql
SELECT
    store_id,
    product_name,
    supplier_name,
    stock_status,
    current_stock,
    recommended_order_qty
FROM demand.reorder_recommendations
WHERE should_reorder = TRUE
  AND stock_status IN ('stockout', 'critical')
ORDER BY
    CASE stock_status
        WHEN 'stockout' THEN 1
        WHEN 'critical' THEN 2
    END;
```

---

## Methodology

### Model type
**Facebook Prophet** — an additive time series model developed for
business forecasting. It decomposes demand into three components:
```
Demand = Trend + Seasonality + Holiday Effects + Error
```

- **Trend:** the long-run direction of demand (growing, flat, declining)
- **Seasonality:** recurring patterns at weekly, monthly, and yearly cycles
- **Holiday effects:** demand spikes or dips around Australian public holidays
- **Error:** random variation the model cannot explain

### Segmentation
We train **one model per category × store cluster combination**
(up to 48 models). This means a metro large store's fresh produce
forecast is trained separately from a rural store's — their demand
patterns are meaningfully different and a single model would average
out important variation.

### Training data
- **Source:** `demand_mart.fct_sales` — weekly aggregated POS sales
- **History used:** all available weeks (up to 52 weeks)
- **Minimum required:** 26 weeks (segments below this are excluded)
- **Aggregation:** weekly total units sold per category × cluster

### Confidence intervals
The 80% confidence interval (`yhat_lower` to `yhat_upper`) means:
> *In 80 out of 100 weeks, actual demand will fall within this range.*

We use 80% (not 95%) deliberately. A 95% interval is so wide it
becomes unusable for inventory planning — it would suggest ordering
far more than needed most of the time. 80% balances accuracy with
actionability.

### Australian public holidays
The model includes the following holiday effects:
- New Year's Day (+1 day window)
- Australia Day (+1 day window)
- Good Friday / Easter Monday (+1 day window)
- Anzac Day (+1 day window)
- Queen's Birthday (+1 day window)
- Christmas Day / Boxing Day (+1 day window)

These are treated as one-off events that override the normal
weekly seasonality pattern.

---

## Model performance (actual results)

| Category | MAPE | Grade | Notes |
|---|---|---|---|
| dairy | 20.26% | ✅ Excellent | High volume, stable demand pattern |
| fresh_produce | 52.45% | ⚠️ Acceptable | Seasonal volatility expected |
| pantry | 55.79% | ⚠️ Acceptable | Promotional sensitivity |
| bakery | 71.96% | ⚠️ Poor | Low volume amplifies % error |
| meat_seafood | 81.24% | ⚠️ Poor | High price variance affects units |
| frozen | 108.57% | ❌ Unreliable | High week-to-week noise |
| health_beauty | 122.48% | ❌ Unreliable | Irregular purchase patterns |
| beverages | 127.31% | ❌ Unreliable | Promotional spikes not modelled |

### Why are some MAPEs so high?

MAPE (Mean Absolute Percentage Error) is particularly sensitive to
categories with **high coefficient of variation** — where week-to-week
swings are large relative to the average. For beverages and health_beauty,
the standard deviation of weekly demand is greater than 20% of the mean,
which means even a directionally correct forecast will show high MAPE.

**Practical implication:** For categories with MAPE > 80%, do not use
`yhat` directly. Instead:
- Use `yhat_upper` as your order quantity (conservative)
- Apply a minimum order floor based on the last 4-week average
- Flag these categories for manual review by category managers

**Categories where the forecast IS reliable (MAPE < 30%):**
- Dairy: use `yhat` directly with standard safety stock

Query current model performance:
```sql
SELECT
    category,
    store_cluster,
    mape,
    rmse,
    n_weeks,
    failure_modes
FROM demand.forecast_metadata
ORDER BY mape DESC;
```

---

## Known failure modes

These are situations where the forecast is known to be unreliable.
The model will still produce a number — but you should apply
additional judgement.

### 1. Stockout-driven demand suppression
**What happens:** When a product is out of stock, sales drop to zero.
The model sees zero sales and learns that demand is low — but the
true demand was suppressed by the stockout, not absent.

**Impact:** The model will underestimate future demand for products
with a history of stockouts.

**What to do:** For products with `stock_status = 'stockout'` in the
last 4 weeks, add 15–20% to the `yhat_upper` estimate.

### 2. Promotional events not in the model
**What happens:** Major promotions (catalogue sales, price drops,
loyalty events) cause demand spikes that the model has no visibility
into unless they repeat at the same time every year.

**Impact:** The model will under-forecast during promotions and
over-forecast in weeks immediately after (when demand has been
pulled forward).

**What to do:** For weeks with planned promotions, override the
forecast manually using the category's historical promotional
uplift factor. The merchandising team maintains this in Confluence.

### 3. New store openings
**What happens:** New stores have no sales history. The cluster-level
model will apply the cluster average — which may not match a new
store's actual catchment.

**Impact:** Forecast accuracy is lower for stores open less than
26 weeks.

**What to do:** For stores open less than 6 months, treat the forecast
as a rough guide only and lean on the store manager's local knowledge.

### 4. Seasonal category volatility (fresh produce)
**What happens:** Fresh produce demand responds to weather, local
events, and competitor promotions in ways that don't repeat
predictably year-on-year.

**Impact:** Fresh produce MAPE is typically 5–10% higher than
packaged categories.

**What to do:** Always use `yhat_upper` for fresh produce ordering.
Never order to `yhat` — the cost of a fresh produce stockout
(customer dissatisfaction, perishable waste) exceeds the cost
of mild overstock.

### 5. Rural cluster thin data
**What happens:** Rural clusters have fewer stores contributing
data, making the aggregate noisier.

**Impact:** Confidence intervals are wider. `yhat_upper` may be
significantly higher than `yhat`.

**What to do:** Use `yhat` for rural ordering but add a flat
buffer of one extra week's supply for perishable categories.

---

## What this model does NOT do

- ❌ Does not forecast at individual store level (use cluster × category)
- ❌ Does not account for competitor pricing or promotions
- ❌ Does not model weather effects
- ❌ Does not forecast new products with no sales history
- ❌ Does not replace the judgement of experienced category managers
  for high-stakes decisions

---

## Reorder recommendation logic

The reorder engine (`demand.reorder_recommendations`) translates
forecasts into order quantities using this formula:
```
Demand during lead time = (avg weekly forecast / 7) × lead_time_days

Safety stock = (forecast upper bound / 7) × lead_time_days
               × (safety multiplier - 1)

Reorder point = demand during lead time + safety stock

If current_stock ≤ reorder point → place order

Order quantity = (daily demand × 28 days) - current_stock + safety_stock
```

**Safety stock multipliers by stock status:**

| Status | Multiplier | Meaning |
|---|---|---|
| stockout | 1.5 | Hold 50% extra buffer above lead time demand |
| critical | 1.3 | Hold 30% extra buffer |
| low | 1.1 | Hold 10% extra buffer |
| healthy | 1.0 | No extra buffer needed |

---

## Contact and feedback

**Forecast questions:** demand-analytics@retailer.com.au  
**Data quality issues:** data-platform@retailer.com.au  
**Model improvement requests:** Raise a ticket in Jira (DMND board)

*This data product is reviewed quarterly by the Demand Analytics Team.
Last methodology review: Q4 2023.*