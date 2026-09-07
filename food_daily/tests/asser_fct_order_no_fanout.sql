WITH fct_count AS(
    SELECT count(*) as row_count FROM {{ref('fct_orders')}}
),

source_count AS(
    SELECT count(*) as row_count FROM {{ref('int_feedback_category')}}
)

SELECT
    f.row_count as fct_rows,
    s.row_count as source_rows
FROM fct_count as f
CROSS JOIN source_count as s
WHERE f.row_count != s.row_count