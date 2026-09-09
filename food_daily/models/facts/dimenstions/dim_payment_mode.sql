with source as (
    SELECT * FROM {{ ref('int_feedback_category') }}
)

SELECT DISTINCT 
    cityHash64(toString(mode)) as mode_id,
    mode
FROM source