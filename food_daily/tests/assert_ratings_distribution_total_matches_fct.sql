SELECT 1
FROM (
    SELECT sum(order_ratings) as mart_total
    FROM {{ref('mart_ratings_distribution')}}
) m,
(
    select count(ratings) as fct_total
    from {{ ref('fct_orders')}}
) f
where m.mart_total != f.fct_total