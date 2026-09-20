with source as (
    SELECT * FROM {{ ref('fct_orders') }}
)

select ratings , count(ratings) as order_ratings 
from source
GROUP BY ratings
ORDER BY ratings
