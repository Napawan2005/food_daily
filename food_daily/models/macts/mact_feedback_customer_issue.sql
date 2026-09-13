WITH source AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

dim_category AS(
    SELECT * FROM {{ref('dim_category')}}
),

dim_status AS(
    SELECT * FROM {{ ref('dim_order_status') }}
)


SELECT 
    ds.order_status,
    c.category,
    count(*) as count_order_category,
    countIf(s.order_id , s.feedback_sentiment = 'Negative') as count_negative,
    countIf(s.ratings <= 2 ) as count_ratings_loss,
    countIf(s.order_id , s.s.feedback_sentiment = 'Negative' AND  s.ratings <= 2) as count_negative_ratings_loss,

    sum(s.amount) as gmv,
    sumIf(s.amount , s.ratings <= 2) as gmv_loss_ratings,
    sumIf(s.amount , s.feedback_sentiment = 'Negative' ) as gmv_negative,
    sumIf(s.amount , s.feedback_sentiment = 'Negative' AND  s.ratings <= 2) as gmv_low_rated,

    dense_rank() over(
        PARTITION BY ds.order_status
        ORDER BY countIf(s.ratings <= 2 ) DESC
    ) as rank_loss
FROM source as s
LEFT JOIN dim_category as c 
ON s.category_id = c.category_id
LEFT JOIN dim_status as ds
ON ds.order_status_id = s.order_status_id
WHERE c.category IN ('Platform(App & System)', 'Platform(Logistics)', 'Overall', 'Unclassified')
GROUP BY c.category, ds.order_status
ORDER BY ds.order_status ASC


