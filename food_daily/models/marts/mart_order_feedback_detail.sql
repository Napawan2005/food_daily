WITH source AS(
    SELECT * FROM {{ref('fct_orders')}}
)
,
dim_category as(
    SELECT * FROM {{ref('dim_category')}}
)

SELECT 
    s.ratings,
    s.feedback_sentiment,
    c.category,
    s.feedback,
    s.order_id

FROM source s
LEFT JOIN dim_category c using (category_id)

