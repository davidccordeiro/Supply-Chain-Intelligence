-- Purchase order fact table
-- Grain: one row per purchase order line (po_id is unique)

WITH purchase_orders AS (

    SELECT * FROM {{ ref('stg_purchase_orders') }}

),

stores AS (

    SELECT store_id, store_cluster, state
    FROM {{ ref('dim_stores') }}

),

products AS (

    SELECT product_id, product_name, category
    FROM {{ ref('dim_products') }}
    WHERE is_current_version = TRUE

),

suppliers AS (

    SELECT supplier_id, supplier_name, lead_time_days
    FROM {{ ref('stg_suppliers') }}

),

final AS (

    SELECT
        po.po_id,
        po.store_id,
        po.product_id,
        po.supplier_id,
        s.store_cluster,
        s.state,
        p.product_name,
        p.category,
        sup.supplier_name,
        sup.lead_time_days,
        po.order_date,
        po.expected_date,
        po.quantity_ordered,
        po.unit_cost,
        po.total_order_value,
        po.status,

        -- Actual vs expected delivery window
        CAST(po.expected_date - po.order_date AS INTEGER) AS promised_lead_days,

        -- Days overdue (if late)
        CASE
            WHEN po.is_late
            THEN CAST(CURRENT_DATE - po.expected_date AS INTEGER)
            ELSE 0
        END                             AS days_overdue,
        po.is_late,
        po.is_fulfilled

    FROM purchase_orders po
    LEFT JOIN stores s
        ON po.store_id = s.store_id
    LEFT JOIN products p
        ON po.product_id = p.product_id
    LEFT JOIN suppliers sup
        ON po.supplier_id = sup.supplier_id

)

SELECT * FROM final