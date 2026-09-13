
WITH source AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

select
    s.order_status,
    count(*)                                                as orders,
    round(100.0 * count(*) / sum(count(*)) over (), 2)      as pct_orders,
    round(avg(f.amount), 2)                                 as avg_ticket,
    sum(f.amount)                                                as total_amount,
        sumIf(f.amount , f.feedback_sentiment = 'Negative') as order_amount_negative,
    countIf(f.feedback_sentiment = 'Negative') as count_sentiment_negative,
    round(
        100.0 * countIf(
            f.feedback_sentiment = 'Negative'
            ) / count(*)
        ) as pct_negative_in_status

from {{ ref('fct_orders') }} f
join {{ ref('dim_order_status') }} s using (order_status_id)
group by s.order_status
order by orders DESC

