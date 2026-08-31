WITH source AS
(
    SELECT * FROM {{ ref('stg_food_daily__order') }}
)

SELECT 
    toUInt8(splitByChar(':' , toString(order_time))[1]) as hour,
    order_status ,
    count(*) as order_count ,
    sum(length(items)) as count_items,
    avg(length(items)) as avg_items,
    sum(amount) as total_amount ,
    avg(amount) as avg_amount
FROM
    source
GROUP BY hour , order_status
ORDER BY hour ASC
