-- sum(total_amount) in the daily summary must equal sum(amount) in the
-- upstream staging model — aggregation should never drop or double-count rows.
-- Fails (returns a row) if the two totals differ.
with summary_total as (
    select sum(total_amount) as total_amount
    from {{ ref('int_daily_sales_summary') }}
),

staging_total as (
    select sum(amount) as total_amount
    from {{ ref('stg_food_daily__order') }}
)

select
    summary_total.total_amount as summary_total_amount,
    staging_total.total_amount as staging_total_amount
from summary_total
cross join staging_total
where summary_total.total_amount != staging_total.total_amount
