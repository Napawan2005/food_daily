SELECT *
FROM {{ref('mart_feedback_top_platform_app')}}
where rank_in_sentiment > 5
