{{ config(materialized='table') }}

WITH feedback_classified_orders AS (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT
    order_id,
    customer_id,
    restaurant,
    order_status,
    mode,
    category,
    order_date,
    order_time,
    amount,
    ratings,
    feedback,
    feedback_sentiment
FROM feedback_classified_orders
