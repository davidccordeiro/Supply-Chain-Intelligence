-- Grain: one row per product_id + price version (dbt_valid_from)

WITH source AS (

    SELECT * FROM {{ ref('products_snapshot') }}

),
-- We keep the most recently updated snapshot record.
deduped AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_id, dbt_valid_from
        ORDER BY dbt_updated_at DESC
    ) = 1

),

renamed AS (

    SELECT
        {{ generate_surrogate_key([
            'product_id',
            'dbt_valid_from'
        ]) }}                               AS product_version_key,
        product_id,
        product_name,
        category,
        supplier_id,
        unit_of_measure,
        CAST(unit_price       AS DECIMAL(10,2)) AS unit_price,
        CAST(cost_price       AS DECIMAL(10,2)) AS cost_price,
        CAST(gross_margin_pct AS DECIMAL(5,2))  AS gross_margin_pct,
        CAST(dbt_valid_from AS DATE) AS valid_from,
        CAST(dbt_valid_to   AS DATE) AS valid_to,
        CASE
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current_version,
        CAST(is_active AS BOOLEAN) AS is_active

    FROM deduped

)

SELECT * FROM renamed