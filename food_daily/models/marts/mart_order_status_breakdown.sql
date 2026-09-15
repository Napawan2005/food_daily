
WITH source AS (
    SELECT * FROM {{ ref('fct_orders') }}
),

select
    s.order_status,

    -- counts
    count(*)                                                        as orders,
    countIf(f.feedback_sentiment = 'Negative')                      as count_negative,
    countIf(f.ratings <= 2)                                         as count_ratings_loss,
    countIf(f.feedback_sentiment = 'Negative' AND f.ratings <= 2)   as count_negative_ratings_loss,

    -- share of total 
    round(100.0 * count(*) / sum(count(*)) over (), 2)                                                  as pct_orders,
    round(100.0 * countIf(f.feedback_sentiment = 'Negative') / sum(countIf(f.feedback_sentiment = 'Negative')) over (), 2)   as pct_negative_of_total,
    round(100.0 * countIf(f.ratings <= 2) / sum(countIf(f.ratings <= 2)) over (), 2)                    as pct_ratings_loss_of_total,
    round(100.0 * countIf(f.feedback_sentiment = 'Negative' AND f.ratings <= 2)
        / sum(countIf(f.feedback_sentiment = 'Negative' AND f.ratings <= 2)) over (), 2)                as pct_negative_ratings_loss_of_total,

    -- share within status 
    round(100.0 * countIf(f.feedback_sentiment = 'Negative') / count(*), 2)                             as pct_negative_in_status,
    round(100.0 * countIf(f.ratings <= 2) / count(*), 2)                                                as pct_ratings_loss_in_status,
    round(100.0 * countIf(f.feedback_sentiment = 'Negative' AND f.ratings <= 2) / count(*), 2)          as pct_negative_ratings_loss_in_status,

    -- money
    round(avg(f.amount), 2)                                             as avg_amount,
    sum(f.amount)                                                       as gmv,
    sumIf(f.amount, f.ratings <= 2)                                     as gmv_ratings_loss,
    sumIf(f.amount, f.feedback_sentiment = 'Negative')                  as gmv_negative,
    sumIf(f.amount, f.feedback_sentiment = 'Negative' AND f.ratings <= 2) as gmv_negative_ratings_loss
from {{ ref('fct_orders') }} f
join {{ ref('dim_order_status') }} s using (order_status_id)
group by s.order_status
order by orders desc
