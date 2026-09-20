SELECT feedback_sentiment, count() as n
FROM {{ref('mart_feedback_top_platform_app')}}
group by feedback_sentiment
having n > 5
