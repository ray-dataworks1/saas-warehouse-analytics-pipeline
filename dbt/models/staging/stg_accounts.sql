select
  account_id,
  account_name,
  industry,
  country,
  signup_date,
  referral_source,
  plan_tier,
  cast(seats as int64) as seats,
  cast(is_trial as bool) as is_trial,
  cast(churn_flag as bool) as churn_flag
from {{ source('ravenstack_raw', 'accounts') }}

-- Smoke test model :)