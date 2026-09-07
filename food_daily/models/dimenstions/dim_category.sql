with source as (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT DISTINCT
    cityHash64(toString(category)) as category_id,
    category
FROM source