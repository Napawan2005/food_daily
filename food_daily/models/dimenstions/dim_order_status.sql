with source as (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT DISTINCT 
    row_number() over (order by category) as category_key,
    category
FROM
    source