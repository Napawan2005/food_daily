with source as (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT DISTINCT
    cityHash64(restaurant) as restaurant_id,
    restaurant as restaurant_name
FROM source
