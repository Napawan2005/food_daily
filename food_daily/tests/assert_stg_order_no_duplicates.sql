SELECT
    order_id,
    count(*) as row_count
FROM {{ref('stg_food_daily__order')}}
GROUP BY order_id
HAVING row_count > 1