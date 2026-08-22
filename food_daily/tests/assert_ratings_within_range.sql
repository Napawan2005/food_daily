-- Ratings, when present, should be between 1 and 5 inclusive.
-- Fails (returns rows) if any non-null rating falls outside that range.
select order_id, ratings
from {{ ref('stg_food_daily__order') }}
where ratings is not null
  and (ratings < 1 or ratings > 5)
