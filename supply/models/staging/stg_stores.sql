-- Grain: one row per store (store_id is unique)

WITH source AS (

    SELECT * FROM {{ source('raw', 'stores') }}

),

renamed AS (

    SELECT
        store_id,
        store_name,
        city,
        postcode,
        state,
        store_cluster,
        CAST(opened_date AS DATE) AS opened_date,
        CASE
            WHEN store_cluster IN ('metro_large', 'metro_small')
            THEN TRUE
            ELSE FALSE
        END AS is_metro,
        CAST(is_active AS BOOLEAN) AS is_active

    FROM source

)

SELECT * FROM renamed