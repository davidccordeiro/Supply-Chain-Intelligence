-- Grain: one row per store_id + product_id + snapshot_date where stockout occurred
--
-- Business question this answers:
-- "When we had a stockout, how much revenue did we likely lose?"
-- Lost revenue is estimated using the store's average daily sales for that product.

WITH inventory AS (

    SELECT
        store_id,
        product_id,
        snapshot_date,
        quantity_on_hand,
        stock_status,
        is_stockout,
        is_below_reorder_point,
        days_of_stock_remaining,
        store_cluster,
        state,
        product_name,
        category,
        lead_time_days
    FROM {{ ref('supply', 'fct_inventory_snapshots') }}

),

-- Average daily sales per store/product
avg_daily_sales AS (

    SELECT
        store_id,
        product_id,
        AVG(revenue)    AS avg_daily_revenue,
        AVG(quantity)   AS avg_daily_quantity
    FROM {{ ref('fct_sales') }}
    GROUP BY store_id, product_id

),

stockouts AS (

    SELECT
        i.store_id,
        i.product_id,
        i.snapshot_date         AS stockout_date,
        i.stock_status,
        i.is_stockout,
        i.is_below_reorder_point,
        i.days_of_stock_remaining,
        i.store_cluster,
        i.state,
        i.product_name,
        i.category,
        i.lead_time_days,

        -- Estimated lost revenue
        -- Assumes stockout lasts 7 days (weekly snapshot interval)
        -- Multiply avg daily revenue by 7 days of stockout
        COALESCE(ads.avg_daily_revenue, 0)  AS avg_daily_revenue,
        COALESCE(ads.avg_daily_quantity, 0) AS avg_daily_quantity,

        CASE
            WHEN i.is_stockout THEN
                ROUND(COALESCE(ads.avg_daily_revenue, 0) * 7, 2)
            ELSE 0
        END                     AS estimated_lost_revenue_7d,

        -- Urgency: stockouts with long supplier lead times are worst
        CASE
            WHEN i.is_stockout AND i.lead_time_days >= 14 THEN 'critical'
            WHEN i.is_stockout AND i.lead_time_days <  14 THEN 'high'
            WHEN i.is_below_reorder_point              THEN 'medium'
            ELSE 'low'
        END                     AS urgency

    FROM inventory i
    LEFT JOIN avg_daily_sales ads
        ON  i.store_id  = ads.store_id
        AND i.product_id = ads.product_id
    WHERE i.is_stockout = TRUE
       OR i.is_below_reorder_point = TRUE

)

SELECT * FROM stockouts