WITH source AS(
    SELECT * FROM {{ref('fct_orders')}}
),

dim_category as(
    select * from {{ref('dim_category')}}
),
category_app_and_system AS(
    select
        *
    from source
    JOIN dim_category c using (category_id)
    WHERE c.category = 'Platform(App & System)'
),

feedback_counts AS(
    select 
        category,
        feedback_sentiment,
        feedback,
        count(order_id) as order_count
    from category_app_and_system
    GROUP BY feedback_sentiment, feedback , category
),
ranked AS (
    select 
        * ,
        row_number() over(
            PARTITION by feedback_sentiment
            ORDER BY order_count DESC, feedback
        ) AS rank_in_sentiment
    from feedback_counts
)

select 
    feedback_sentiment,
    feedback,
    order_count
from ranked
where rank_in_sentiment <= 5 
order by feedback_sentiment, rank_in_sentiment
