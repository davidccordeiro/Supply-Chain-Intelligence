-- Grain: one row per sale event
-- Note: sale_id is NOT unique in source — POS systems can duplicate on network retry. We deduplicate here using ROW_NUMBER().

WITH source AS (

    SELECT * FROM {{ source('raw', 'pos_sales') }}

),

deduped AS (

    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sale_id
        ORDER BY total_amount DESC
    ) = 1

),

renamed AS (

    SELECT
        -- Surrogate key — sale_id from source is not reliable as PK
        {{ generate_surrogate_key(['sale_id', 'store_id', 'product_id', 'sale_date']) }}
                                        AS sale_key,

        sale_id,
        store_id,
        product_id,
        CAST(sale_date AS DATE)         AS sale_date,

        -- Derived date parts — useful for seasonal analysis
        EXTRACT(YEAR  FROM sale_date)   AS sale_year,
        EXTRACT(MONTH FROM sale_date)   AS sale_month,
        EXTRACT(DOW   FROM sale_date)   AS sale_dow,   -- 0=Sunday, 6=Saturday

        -- Is this a weekend sale?
        CASE
            WHEN EXTRACT(DOW FROM sale_date) IN (0, 6) THEN TRUE
            ELSE FALSE
        END                             AS is_weekend,
        CAST(quantity    AS INTEGER)        AS quantity,
        CAST(unit_price  AS DECIMAL(10,2))  AS unit_price,
        CAST(total_amount AS DECIMAL(10,2)) AS total_amount,

        -- Validate POS calculation — flag rounding errors > 1 cent
        CASE
            WHEN ABS(total_amount - (quantity * unit_price)) > 0.01
            THEN TRUE
            ELSE FALSE
        END                             AS has_price_discrepancy

    FROM deduped

)

SELECT * FROM renamed