with source as (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT DISTINCT 
    row_number() over (order by mode) as mode_id,
    mode 
FROM source