{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ ref('int_feedback_category') }}
),

restaurants AS (
    SELECT * FROM {{ ref('dim_restaurant') }}
),

statuses AS (
    SELECT * FROM {{ ref('dim_order_status') }}
),

payment AS (
    SELECT * FROM {{ ref('dim_payment_mode') }}
),

categories AS (
    SELECT * FROM {{ ref('dim_category') }}
)

SELECT
    s.order_id,
    s.customer_id,
    r.restaurant_id,
    su.order_status_id,
    p.mode_id,
    c.category_id,
    s.order_date,
    s.order_time,
    s.amount,
    s.ratings,
    s.feedback
FROM source AS s
LEFT JOIN restaurants AS r  ON s.restaurant   = r.restaurant_name
LEFT JOIN statuses    AS su ON s.order_status = su.order_status
LEFT JOIN payment     AS p  ON s.mode         = p.mode
LEFT JOIN categories  AS c  ON s.category     = c.category
