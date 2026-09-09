with source as (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT DISTINCT 
    cityHash64(toString(order_status)) as order_status_id,
    order_status
FROM
    source