SELECT
    order_id,
    items
FROM {{ ref('stg_food_daily__order') }}
WHERE length(items) = 0