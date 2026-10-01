WITH source AS(
    SELECT * FROM {{ref('fct_orders')}}
)

SELECT
    ratings,
    feedback_sentiment,
    category,
    feedback,
    order_id
FROM source
