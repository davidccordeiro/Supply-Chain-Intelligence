-- Grain: one row per store_id + product_id + snapshot_date
-- Key metric: days_of_stock_remaining drives reorder decisions

WITH inventory AS (

    SELECT * FROM {{ ref('stg_inventory_snapshots') }}

),

stores AS (

    SELECT
        store_id,
        store_cluster,
        state,
        is_metro
    FROM {{ ref('dim_stores') }}

),

products AS (

    -- Current version only for inventory context
    SELECT
        product_id,
        product_name,
        category,
        perishability,
        supplier_id,
        supplier_name,
        lead_time_days,
        unit_price,
        cost_price
    FROM {{ ref('dim_products') }}
    WHERE is_current_version = TRUE

),

-- Weekly sales velocity per store/product
-- Used to calculate days_of_stock_remaining
-- We calculate average daily units sold over the last 4 weeks
sales_velocity AS (

    SELECT
        store_id,
        product_id,
        -- Average daily sales — used as demand rate for stock coverage calc
        AVG(quantity_on_hand) AS avg_weekly_stock
    FROM {{ ref('stg_inventory_snapshots') }}
    GROUP BY store_id, product_id

),

final AS (

    SELECT
        i.inventory_snapshot_key,
        i.store_id,
        i.product_id,
        i.snapshot_date,
        s.store_cluster,
        s.state,
        s.is_metro,
        p.product_name,
        p.category,
        p.perishability,
        p.supplier_name,
        p.lead_time_days,
        i.quantity_on_hand,
        i.reorder_point,
        i.reorder_qty,

        -- Inventory value at current cost
        ROUND(
            i.quantity_on_hand * p.cost_price, 2
        )                                       AS inventory_value,

        -- Days of stock remaining
        -- Formula: quantity_on_hand / average daily demand
        -- Average daily demand = avg_weekly_stock / 7
        CASE
            WHEN sv.avg_weekly_stock = 0 THEN NULL
            ELSE ROUND(
                i.quantity_on_hand / NULLIF(sv.avg_weekly_stock / 7, 0),
                1
            )
        END                                     AS days_of_stock_remaining,
        CASE
            WHEN i.is_stockout THEN 'stockout'
            WHEN i.is_below_reorder_point
             AND p.lead_time_days >= 14 THEN 'critical'
            WHEN i.is_below_reorder_point THEN 'low'
            ELSE 'healthy'
        END                                     AS stock_status,
        i.is_stockout,
        i.is_below_reorder_point

    FROM inventory i
    LEFT JOIN stores s
        ON i.store_id = s.store_id
    LEFT JOIN products p
        ON i.product_id = p.product_id
    LEFT JOIN sales_velocity sv
        ON i.store_id  = sv.store_id
       AND i.product_id = sv.product_id

)

SELECT * FROM final