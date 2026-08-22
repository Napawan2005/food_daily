-- The model floors amount to 1 when the raw value is below 1, so amount
-- should never be non-positive in the staged output.
-- Fails (returns rows) if any order has amount < 1.
select order_id, amount
from {{ ref('stg_food_daily__order') }}
where amount < 1
