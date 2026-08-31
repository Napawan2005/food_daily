with source as(
    SELECT * FROM {{ref('stg_food_daily__order')}}
)

SELECT 
    mode ,
    count(*) as count_order,
    sum(amount) as total_amount ,
    avg(amount) as avg_amount
FROM
source
GROUP BY mode
ORDER BY count_order desc