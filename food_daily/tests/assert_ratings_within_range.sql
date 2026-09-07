SELECT
    order_id,
    ratings
FROM {{ref('fct_orders')}}
WHERE ratings < 1 or ratings > 5