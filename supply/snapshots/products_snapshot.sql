-- SCD Type 2: tracks full price history for every product
--
-- Output columns added by dbt:
--   dbt_scd_id      → unique id for each snapshot record
--   dbt_updated_at  → when dbt last touched this record
--   dbt_valid_from  → when this version became active
--   dbt_valid_to    → when this version was superseded (NULL = current)

{% snapshot products_snapshot %}

{{
    config(
        target_schema = "snapshots",
        unique_key    = "product_id",
        strategy      = "timestamp",
        updated_at    = "effective_from",
        invalidate_hard_deletes = true,
    )
}}

-- We join price history back to products here to capture
-- all product attributes alongside each price version.
-- This means dim_products can read from one place.
SELECT
    ph.product_id,
    p.product_name,
    p.category,
    p.supplier_id,
    p.unit_of_measure,
    p.is_active,
    ph.unit_price,
    ph.cost_price,
    ph.effective_from,
    ph.effective_to,

    -- Margin at this price version — useful for historical margin analysis
    ROUND(
        (ph.unit_price - ph.cost_price) / NULLIF(ph.unit_price, 0) * 100,
        2
    ) AS gross_margin_pct

FROM {{ source('raw', 'product_price_history') }} ph
INNER JOIN {{ source('raw', 'products') }} p
    ON ph.product_id = p.product_id

{% endsnapshot %}