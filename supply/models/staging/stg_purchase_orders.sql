-- Grain: one row per purchase order line (po_id is unique)

WITH source AS (

    SELECT * FROM {{ source('raw', 'purchase_orders') }}

),

renamed AS (

    SELECT
        po_id,
        store_id,
        product_id,
        supplier_id,
        CAST(order_date    AS DATE)         AS order_date,
        CAST(expected_date AS DATE)         AS expected_date,
        CAST(quantity_ordered AS INTEGER)   AS quantity_ordered,
        CAST(unit_cost       AS DECIMAL(10,2)) AS unit_cost,
        ROUND(
            quantity_ordered * unit_cost, 2
        )                                   AS total_order_value,
        status,
        CASE
            WHEN status IN ('in_transit', 'pending')
             AND CURRENT_DATE > expected_date THEN TRUE
            ELSE FALSE
        END                                 AS is_late,
        CASE
            WHEN status = 'delivered' THEN TRUE
            ELSE FALSE
        END                                 AS is_fulfilled

    FROM source

)

SELECT * FROM renamed