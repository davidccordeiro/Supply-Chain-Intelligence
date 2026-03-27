-- Grain: one row per store (store_id is unique)


WITH stores AS (

    SELECT * FROM {{ ref('stg_stores') }}

),

final AS (

    SELECT
        store_id,
        store_name,
        city,
        postcode,
        state,
        store_cluster,
        opened_date,
        is_metro,
        is_active,
        CASE store_cluster
            WHEN 'metro_large'  THEN 1
            WHEN 'metro_small'  THEN 2
            WHEN 'suburban'     THEN 3
            WHEN 'regional'     THEN 4
            WHEN 'rural'        THEN 5
            WHEN 'convenience'  THEN 6
        END                         AS cluster_tier,
        CASE state
            WHEN 'NSW' THEN 'New South Wales'
            WHEN 'VIC' THEN 'Victoria'
            WHEN 'QLD' THEN 'Queensland'
            WHEN 'WA'  THEN 'Western Australia'
            WHEN 'SA'  THEN 'South Australia'
        END                         AS state_full_name

    FROM stores

)

SELECT * FROM final