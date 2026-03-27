-- Grain: one row per sale event (sale_key is unique)
--
-- Cross-domain refs:
--   {{ ref('supply', 'dim_products') }} — product attributes + SCD Type 2 price
--   {{ ref('supply', 'dim_stores') }}   — store attributes + cluster
--
-- The SCD Type 2 join is the most important pattern in this model.
-- We join on product_id AND sale_date BETWEEN valid_from AND valid_to to get the price that was actually in effect on the day of the sale.

WITH sales AS (

    SELECT * FROM {{ ref('stg_sales') }}

),

-- Cross-domain ref — supply project's public dim_products
-- This is the dbt Mesh pattern in action
products AS (

    SELECT
        product_id,
        product_name,
        category,
        perishability,
        supplier_name,
        unit_price      AS catalogue_price,
        cost_price,
        gross_margin_pct,
        valid_from,
        valid_to
    FROM {{ ref('supply', 'dim_products') }}

),

stores AS (

    SELECT
        store_id,
        store_name,
        state,
        store_cluster,
        cluster_tier,
        is_metro
    FROM {{ ref('supply', 'dim_stores') }}

),

-- SCD Type 2 join — match sale to the product price version that was active on the day of the sale
sales_with_product AS (

    SELECT
        s.sale_key,
        s.sale_id,
        s.store_id,
        s.product_id,
        s.sale_date,
        s.sale_year,
        s.sale_month,
        s.sale_dow,
        s.is_weekend,
        s.quantity,
        s.unit_price        AS pos_unit_price,
        s.total_amount,
        s.has_price_discrepancy,

        -- Product attributes at time of sale
        p.product_name,
        p.category,
        p.perishability,
        p.supplier_name,
        p.catalogue_price,
        p.cost_price,
        p.gross_margin_pct

    FROM sales s
    LEFT JOIN products p
        ON  s.product_id = p.product_id
        AND s.sale_date  >= p.valid_from
        AND s.sale_date  <  COALESCE(p.valid_to, CURRENT_DATE + 1)

    -- If a sale date falls in an overlapping price window,
    -- keep only the most recently effective price version
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.sale_key
        ORDER BY 
            p.valid_from DESC,
            p.valid_to DESC
    ) = 1

),

final AS (

    SELECT
        swp.sale_key,
        swp.sale_id,
        swp.store_id,
        swp.product_id,
        st.store_name,
        st.state,
        st.store_cluster,
        st.cluster_tier,
        st.is_metro,
        swp.product_name,
        swp.category,
        swp.perishability,
        swp.supplier_name,
        swp.sale_date,
        swp.sale_year,
        swp.sale_month,
        swp.sale_dow,
        swp.is_weekend,
        swp.quantity,
        swp.pos_unit_price,
        swp.total_amount        AS revenue,

        -- Margin at historical price 
        ROUND(
            swp.quantity * (swp.pos_unit_price - swp.cost_price), 2
        )                       AS gross_profit,

        ROUND(
            swp.gross_margin_pct, 2
        )                       AS gross_margin_pct,

        -- Price variance: did the POS charge match the catalogue?
        ROUND(
            swp.pos_unit_price - swp.catalogue_price, 2
        )                       AS price_variance,

        swp.has_price_discrepancy

    FROM sales_with_product swp
    LEFT JOIN stores st
        ON swp.store_id = st.store_id

)

SELECT * FROM final