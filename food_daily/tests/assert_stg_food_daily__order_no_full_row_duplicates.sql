select
    customer_id, order_date, order_time, order_id, items,
    mode, amount, restaurnt, order_status, ratings, feedback,
    count(*) as duplicate_count
from {{ ref('stg_food_daily__order') }}
group by
    customer_id, order_date, order_time, order_id, items,
    mode, amount, restaurnt, order_status, ratings, feedback
having duplicate_count > 1
