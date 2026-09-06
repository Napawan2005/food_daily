with source as (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT DISTINCT
    row_number() OVER(ORDER BY restaurant) as restaurant_id,
    restaurant as restaurant_name
FROM source
