-- Grain: one row per product_id + valid_from (price version)
-- Filter to is_current_version = true for current product list

WITH products AS (

    SELECT * FROM {{ ref('stg_products') }}

),

suppliers AS (

    SELECT
        supplier_id,
        supplier_name,
        lead_time_days,
        is_reliable_supplier
    FROM {{ ref('stg_suppliers') }}

),

final AS (

    SELECT
        p.product_version_key,
        p.product_id,
        p.product_name,
        p.category,
        p.unit_of_measure,
        p.is_active,
        p.unit_price,
        p.cost_price,
        p.gross_margin_pct,
        p.valid_from,
        p.valid_to,
        p.is_current_version,
        s.supplier_id,
        s.supplier_name,
        s.lead_time_days,
        s.is_reliable_supplier,
        CASE p.category
            WHEN 'fresh_produce' THEN 'Perishable'
            WHEN 'dairy'         THEN 'Perishable'
            WHEN 'meat_seafood'  THEN 'Perishable'
            WHEN 'bakery'        THEN 'Perishable'
            WHEN 'frozen'        THEN 'Non-Perishable'
            WHEN 'pantry'        THEN 'Non-Perishable'
            WHEN 'beverages'     THEN 'Non-Perishable'
            WHEN 'health_beauty' THEN 'Non-Perishable'
        END                         AS perishability

    FROM products p
    LEFT JOIN suppliers s
        ON p.supplier_id = s.supplier_id

)

SELECT * FROM final