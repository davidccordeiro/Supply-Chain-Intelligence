
-- Grain: one row per store_id + product_id + snapshot_date

WITH source AS (

    SELECT * FROM {{ source('raw', 'inventory_snapshots') }}

),

renamed AS (

    SELECT
        {{ generate_surrogate_key([
            'store_id',
            'product_id',
            'snapshot_date'
        ]) }}                               AS inventory_snapshot_key,
        store_id,
        product_id,
        CAST(snapshot_date AS DATE)         AS snapshot_date,
        CAST(quantity_on_hand  AS INTEGER)  AS quantity_on_hand,
        CAST(reorder_point     AS INTEGER)  AS reorder_point,
        CAST(reorder_qty       AS INTEGER)  AS reorder_qty,
        CASE
            WHEN quantity_on_hand = 0 THEN TRUE
            ELSE FALSE
        END                                 AS is_stockout,
        CASE
            WHEN quantity_on_hand > 0
             AND quantity_on_hand <= reorder_point THEN TRUE
            ELSE FALSE
        END                                 AS is_below_reorder_point

    FROM source

)

SELECT * FROM renamed