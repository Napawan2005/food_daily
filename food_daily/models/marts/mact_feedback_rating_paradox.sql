WITH source AS(
    SELECT * FROM {{ref('fct_orders')}}
),

dim_category AS(
    SELECT * FROM {{ref('dim_category')}}
)

SELECT
    c.category,

    count(*) as  total_orders,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as pct_of_all_orders,

    countIf(s.ratings <= 2) as low_rated_orders_count,
    round(100.0 * countIf(s.ratings <= 2) / sum(countIf(s.ratings <= 2)) over (), 2) as pct_low_rated_orders_count,

    countIf(s.feedback_sentiment = 'Negative') as negative_order,
    round(100.0 * countIf(s.feedback_sentiment = 'Negative') / sum(countIf(s.feedback_sentiment = 'Negative')) over (), 2) as pct_negative_order,

    countIf(s.feedback_sentiment = 'Negative' AND s.ratings <= 2) as low_rated_order_negative,
    round(100.0 * countIf(s.feedback_sentiment = 'Negative' AND  s.ratings <= 2) / sum(countIf(s.feedback_sentiment = 'Negative' AND  s.ratings <= 2 )) over (), 2) as pct_low_rated_order_negative,
    round(avg(s.ratings), 2 ) as avg_ratings,

    sum(s.amount) as gmv,
    sumIf(s.amount , s.ratings <= 2) as gmv_loss_ratings,
    sumIf(s.amount , s.feedback_sentiment = 'Negative' ) as gmv_negative,
    sumIf(s.amount , s.feedback_sentiment = 'Negative' AND  s.ratings <= 2) as gmv_low_rated

FROM source as s
LEFT JOIN dim_category as c
ON s.category_id = c.category_id
GROUP BY c.category

