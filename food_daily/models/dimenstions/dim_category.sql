with source as (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT DISTINCT
    row_number() over (order by category) as category_id,
    category
FROM source