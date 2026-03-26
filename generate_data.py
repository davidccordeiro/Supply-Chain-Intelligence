# generate_data.py
# Run once to populate data/raw/ with Parquet files
# Usage: python generate_data.py

import pandas as pd
import numpy as np
from faker import Faker
from pathlib import Path
import datetime

# 1. Configuration ─────────────────────────────────────────────────────────────

RANDOM_SEED    = 42
NUM_STORES     = 400
NUM_PRODUCTS   = 500
NUM_SUPPLIERS  = 50
NUM_DAYS       = 365          # one year of history
START_DATE     = datetime.date(2023, 1, 1)
OUTPUT_DIR     = Path("data/raw")

fake = Faker("en_AU")         # Australian locale — fits Coles/Woolworths context
rng  = np.random.default_rng(RANDOM_SEED)
Faker.seed(RANDOM_SEED)


# 2. Helpers ───────────────────────────────────────────────────────────────────

def date_range(start: datetime.date, n_days: int) -> list[datetime.date]:
    return [start + datetime.timedelta(days=i) for i in range(n_days)]

def write_parquet(df: pd.DataFrame, name: str) -> None:
    path = OUTPUT_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"  ✓  {name}.parquet  ({len(df):,} rows)")


# 3. Stores ─────────────────────────────────────────────────────────────────
# 400 stores across 5 states, grouped into 6 store clusters.

def generate_stores() -> pd.DataFrame:
    states        = ["NSW", "VIC", "QLD", "WA", "SA"]
    state_weights = [0.32, 0.28, 0.20, 0.12, 0.08]   # population-weighted
    clusters      = ["metro_large", "metro_small", "suburban",
                     "regional", "rural", "convenience"]

    store_ids = [f"STR{str(i).zfill(4)}" for i in range(1, NUM_STORES + 1)]
    stores = pd.DataFrame({
        "store_id":      store_ids,
        "store_name":    [f"{fake.last_name()} {rng.choice(['Metro','Local','Express','Plus'])}"
                          for _ in store_ids],
        "state":         rng.choice(states, size=NUM_STORES, p=state_weights),
        "city":          [fake.city() for _ in store_ids],
        "postcode":      [fake.postcode() for _ in store_ids],
        "store_cluster": rng.choice(clusters, size=NUM_STORES,
                                    p=[0.15, 0.20, 0.30, 0.20, 0.10, 0.05]),
        "opened_date":   [fake.date_between(start_date="-10y", end_date="-1y")
                          for _ in store_ids],
        "is_active":     rng.choice([True, False], size=NUM_STORES, p=[0.97, 0.03]),
    })
    return stores

# 4. Suppliers ──────────────────────────────────────────────────────────────
# 50 suppliers. Each has a lead_time_days — critical for reorder logic later.

def generate_suppliers() -> pd.DataFrame:
    supplier_ids = [f"SUP{str(i).zfill(3)}" for i in range(1, NUM_SUPPLIERS + 1)]
    suppliers = pd.DataFrame({
        "supplier_id":      supplier_ids,
        "supplier_name":    [fake.company() for _ in supplier_ids],
        "country":          rng.choice(["Australia", "New Zealand", "USA", "China", "Thailand"],
                                       size=NUM_SUPPLIERS, p=[0.60, 0.15, 0.10, 0.10, 0.05]),
        "lead_time_days":   rng.integers(1, 21, size=NUM_SUPPLIERS),
        "payment_terms":    rng.choice(["NET30", "NET60", "NET14", "COD"],
                                       size=NUM_SUPPLIERS, p=[0.40, 0.25, 0.25, 0.10]),
        "is_active":        rng.choice([True, False], size=NUM_SUPPLIERS, p=[0.94, 0.06]),
    })
    return suppliers

# 5. Products ───────────────────────────────────────────────────────────────
# 500 SKUs across 8 grocery categories.
# Each product has a current price AND an effective_from date.

def generate_products(suppliers: pd.DataFrame) -> pd.DataFrame:
    categories = {
        "fresh_produce":    (0.99,  8.99,  0.18),   # (min_price, max_price, weight)
        "dairy":            (1.50, 12.00,  0.12),
        "bakery":           (1.00, 10.00,  0.10),
        "meat_seafood":     (5.00, 45.00,  0.12),
        "pantry":           (0.80, 15.00,  0.18),
        "frozen":           (2.00, 18.00,  0.10),
        "beverages":        (1.20, 22.00,  0.12),
        "health_beauty":    (2.50, 35.00,  0.08),
    }
    cat_names   = list(categories.keys())
    cat_weights = [v[2] for v in categories.values()]

    product_ids  = [f"PRD{str(i).zfill(4)}" for i in range(1, NUM_PRODUCTS + 1)]
    assigned_cats = rng.choice(cat_names, size=NUM_PRODUCTS,
                               p=[w/sum(cat_weights) for w in cat_weights])

    prices = np.array([
        round(rng.uniform(categories[c][0], categories[c][1]), 2)
        for c in assigned_cats
    ])

    products = pd.DataFrame({
        "product_id":        product_ids,
        "product_name":      [f"{fake.word().capitalize()} {cat.replace('_',' ').title()}"
                              for cat in assigned_cats],
        "category":          assigned_cats,
        "supplier_id":       rng.choice(suppliers["supplier_id"].values, size=NUM_PRODUCTS),
        "unit_price":        prices,
        "cost_price":        np.round(prices * rng.uniform(0.45, 0.75, size=NUM_PRODUCTS), 2),
        "unit_of_measure":   rng.choice(["each", "kg", "litre", "pack"],
                                        size=NUM_PRODUCTS, p=[0.50, 0.20, 0.15, 0.15]),
        "is_active":         rng.choice([True, False], size=NUM_PRODUCTS, p=[0.95, 0.05]),
        "effective_from":    START_DATE,    # initial price effective date
    })
    return products

# 6. Product price history ──────────────────────────────────────────────────
# ~30% of products get 1-3 price changes during the year.
# This is what forces SCD Type 2 — the product dimension changes over time

def generate_price_history(products: pd.DataFrame) -> pd.DataFrame:
    records = []
    dates   = date_range(START_DATE, NUM_DAYS)

    # Every product gets its initial price as row 1
    for _, prod in products.iterrows():
        records.append({
            "product_id":     prod["product_id"],
            "unit_price":     prod["unit_price"],
            "cost_price":     prod["cost_price"],
            "effective_from": START_DATE,
            "effective_to":   None,          # None = currently active
        })

    # ~30% of products get price changes
    products_with_changes = products.sample(frac=0.30, random_state=RANDOM_SEED)
    for _, prod in products_with_changes.iterrows():
        n_changes   = rng.integers(1, 4)
        change_dates = sorted(rng.choice(dates[30:], size=n_changes, replace=False))

        current_price = prod["unit_price"]
        current_cost  = prod["cost_price"]

        for i, change_date in enumerate(change_dates):
            # Update the previous record's effective_to
            records[-1]["effective_to"] = change_date - datetime.timedelta(days=1) \
                if records and records[-1]["product_id"] == prod["product_id"] \
                else None

            # Price moves ±5–20%
            price_delta   = rng.uniform(0.85, 1.20)
            current_price = round(current_price * price_delta, 2)
            current_cost  = round(current_cost  * price_delta, 2)

            records.append({
                "product_id":     prod["product_id"],
                "unit_price":     current_price,
                "cost_price":     current_cost,
                "effective_from": change_date,
                "effective_to":   None,
            })

    df = pd.DataFrame(records)
    df["effective_from"] = pd.to_datetime(df["effective_from"])
    df["effective_to"]   = pd.to_datetime(df["effective_to"])
    return df

# 7. Inventory snapshots ────────────────────────────────────────────────────
# Daily stock-on-hand per store per product — but we don't generate all rows. Instead each store carries ~60% of the catalogue # and we sample weekly snapshots to keep it manageable (~4.4M rows).
# ~8% of records hit zero stock → these become stockout events in dbt.

def generate_inventory(stores: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    dates         = date_range(START_DATE, NUM_DAYS)
    weekly_dates  = dates[::7]                  # every 7 days
    active_stores = stores[stores["is_active"]]["store_id"].values
    active_prods  = products[products["is_active"]]["product_id"].values

    records = []
    for store_id in active_stores:
        # Each store carries ~60% of catalogue
        store_products = rng.choice(active_prods,
                                    size=int(len(active_prods) * 0.60),
                                    replace=False)
        for product_id in store_products:
            # Base stock level varies by store cluster
            base_stock = rng.integers(10, 200)
            for snap_date in weekly_dates:
                # 8% chance of stockout
                if rng.random() < 0.08:
                    qty = 0
                else:
                    noise = rng.integers(-20, 30)
                    qty   = max(0, base_stock + noise)
                records.append({
                    "store_id":        store_id,
                    "product_id":      product_id,
                    "snapshot_date":   snap_date,
                    "quantity_on_hand": qty,
                    "reorder_point":   rng.integers(10, 40),
                    "reorder_qty":     rng.integers(20, 100),
                })

    df = pd.DataFrame(records)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    return df

# 8. POS Sales ──────────────────────────────────────────────────────────────
# The core transactional table. Sales are driven by:
#   - Base demand per category
#   - Weekend lift (+30-50%)
#   - Seasonal curve (summer peak for beverages, winter for bakery etc.)
#   - Store cluster multiplier (metro sells more than rural)
#
# Sample store/product combos rather than full cross join to stay manageable.

def generate_sales(stores: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    dates         = date_range(START_DATE, NUM_DAYS)
    active_stores = stores[stores["is_active"]]
    active_prods  = products[products["is_active"]]

    # Cluster multipliers — metro stores sell more units
    cluster_mult = {
        "metro_large":  2.5, "metro_small": 1.8, "suburban":    1.4,
        "regional":     1.0, "rural":       0.6, "convenience": 0.4,
    }

    # Seasonal index by month: index 0=Jan … 11=Dec
    # Different categories peak at different times
    seasonal = {
        "fresh_produce": [1.0,1.0,1.0,1.0,1.1,1.1,1.2,1.2,1.1,1.0,1.0,1.1],
        "dairy":         [1.0,1.0,1.0,1.0,1.0,1.1,1.1,1.1,1.0,1.0,1.0,1.1],
        "bakery":        [1.1,1.0,1.0,1.0,1.1,1.2,1.3,1.2,1.1,1.0,1.0,1.3],
        "meat_seafood":  [1.2,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.1,1.3],
        "pantry":        [1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.1,1.1,1.2],
        "frozen":        [1.0,1.0,1.0,1.0,1.1,1.2,1.3,1.3,1.1,1.0,1.0,1.0],
        "beverages":     [1.3,1.2,1.1,1.0,1.0,0.9,0.9,1.0,1.0,1.1,1.2,1.3],
        "health_beauty": [1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.1,1.2],
    }

    records = []
    # Sample 15% of store/product combos to keep row count manageable
    store_sample   = active_stores.sample(frac=0.15, random_state=RANDOM_SEED)
    product_sample = active_prods.sample(frac=0.15, random_state=RANDOM_SEED)

    for _, store in store_sample.iterrows():
        mult = cluster_mult.get(store["store_cluster"], 1.0)
        for _, product in product_sample.iterrows():
            base_demand = rng.integers(1, 15)
            for sale_date in dates:
                # Weekend lift
                dow_mult = 1.35 if sale_date.weekday() >= 5 else 1.0
                # Seasonal lift
                sea_mult = seasonal[product["category"]][sale_date.month - 1]
                # Final quantity
                qty = max(0, int(base_demand * mult * dow_mult * sea_mult
                                 + rng.normal(0, 1.5)))
                if qty == 0:
                    continue                 # don't record zero-sale rows
                records.append({
                    "sale_id":    f"SL{rng.integers(100000, 999999)}",
                    "store_id":   store["store_id"],
                    "product_id": product["product_id"],
                    "sale_date":  sale_date,
                    "quantity":   qty,
                    "unit_price": product["unit_price"],
                    "total_amount": round(qty * product["unit_price"], 2),
                })

    df = pd.DataFrame(records)
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    return df

# 9. Purchase Orders ────────────────────────────────────────────────────────
# Supplier replenishment orders. Generated when inventory hits reorder point.
# status reflects the order lifecycle — useful for supply domain modelling.

def generate_purchase_orders(stores: pd.DataFrame,
                              products: pd.DataFrame,
                              suppliers: pd.DataFrame) -> pd.DataFrame:
    active_stores = stores[stores["is_active"]]["store_id"].values
    active_prods  = products[products["is_active"]]
    dates         = date_range(START_DATE, NUM_DAYS)

    records = []
    po_num = 1
    for store_id in rng.choice(active_stores, size=100, replace=False):
        for _, product in active_prods.sample(n=20, random_state=RANDOM_SEED).iterrows():
            n_orders = rng.integers(1, 6)
            for _ in range(n_orders):
                order_date    = rng.choice(dates)
                lead_time     = int(suppliers.loc[
                    suppliers["supplier_id"] == product["supplier_id"],
                    "lead_time_days"
                ].values[0]) if product["supplier_id"] in suppliers["supplier_id"].values else 7
                expected_date = order_date + datetime.timedelta(days=lead_time)
                status        = rng.choice(
                    ["delivered", "in_transit", "pending", "cancelled"],
                    p=[0.70, 0.15, 0.10, 0.05]
                )
                records.append({
                    "po_id":            f"PO{str(po_num).zfill(6)}",
                    "store_id":         store_id,
                    "product_id":       product["product_id"],
                    "supplier_id":      product["supplier_id"],
                    "order_date":       order_date,
                    "expected_date":    expected_date,
                    "quantity_ordered": rng.integers(20, 200),
                    "unit_cost":        product["cost_price"],
                    "status":           status,
                })
                po_num += 1

    df = pd.DataFrame(records)
    df["order_date"]    = pd.to_datetime(df["order_date"])
    df["expected_date"] = pd.to_datetime(df["expected_date"])
    return df

# 10. Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Retail Supply Chain — Synthetic Data Generator")
    print(f"    Seed: {RANDOM_SEED} | Stores: {NUM_STORES} | "
          f"Products: {NUM_PRODUCTS} | Days: {NUM_DAYS}\n")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating tables...")
    stores    = generate_stores();         write_parquet(stores,    "stores")
    suppliers = generate_suppliers();      write_parquet(suppliers, "suppliers")
    products  = generate_products(suppliers); write_parquet(products, "products")
    price_hist = generate_price_history(products); write_parquet(price_hist, "product_price_history")
    inventory = generate_inventory(stores, products); write_parquet(inventory, "inventory_snapshots")
    sales     = generate_sales(stores, products);     write_parquet(sales,     "pos_sales")
    pos       = generate_purchase_orders(stores, products, suppliers)
    write_parquet(pos, "purchase_orders")

    print(f"All files written to {OUTPUT_DIR}/")
    print("    Next step: Group 3 — load these into DuckDB\n")

if __name__ == "__main__":
    main()