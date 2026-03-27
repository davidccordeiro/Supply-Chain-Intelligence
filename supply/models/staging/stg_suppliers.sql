-- Grain: one row per supplier (supplier_id is unique)

WITH source AS (

    SELECT * FROM {{ source('raw', 'suppliers') }}

),

renamed AS (

    SELECT
        supplier_id,
        supplier_name,
        country,
        payment_terms,
        CAST(lead_time_days AS INTEGER) AS lead_time_days,
        CASE
            WHEN is_active = TRUE
             AND lead_time_days < 14
            THEN TRUE
            ELSE FALSE
        END AS is_reliable_supplier,
        CAST(is_active AS BOOLEAN) AS is_active

    FROM source

)

SELECT * FROM renamed