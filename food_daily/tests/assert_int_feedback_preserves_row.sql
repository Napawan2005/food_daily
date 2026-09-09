WITH stg AS (
    SELECT count(*) as row_count FROM {{ ref('stg_food_daily__order') }}
),

int_model AS (
    SELECT count(*) as row_count FROM {{ ref('int_feedback_category') }}
)

SELECT
    s.row_count as staging_rows,
    i.row_count as intermediate_rows
FROM stg AS s
CROSS JOIN int_model AS i
WHERE s.row_count != i.row_count
