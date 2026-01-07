{{  config(materialized='table',
           schema='core'
)}}

-- fct_subscription_lifecycle
-- Grain: 1 row per subscription_id + event_start
-- Purpose: normalised subscription lifecycle segments (upgrade/downgrade etc.)

with base as (

  select
    subscription_id,
    account_id,

    cast(start_date as date) as event_start,
    cast(end_date   as date) as raw_event_end,

    seats,
    plan_tier,
    mrr_amount as mrr,

    cast(upgrade_flag   as bool) as is_upgrade,
    cast(downgrade_flag as bool) as is_downgrade

  from {{ ref('stg_subscriptions') }}
),

bounded as (
  select
    *,
    -- choose your preference:
    -- 1) keep null end dates if you want "open" segments
    -- 2) or close them at current_date() to make later daily expansion simpler
    coalesce(raw_event_end, current_date()) as event_end
  from base
),

-- Unpivot upgrade/downgrade flags into one categorical lifecycle_event_type
event_types as (

  select
    subscription_id,
    account_id,
    event_start,
    event_end,
    seats,
    plan_tier,
    mrr,

    e.lifecycle_event_type

  from bounded b
  cross join unnest([
    struct('upgrade'   as lifecycle_event_type, b.is_upgrade   as flag),
    struct('downgrade' as lifecycle_event_type, b.is_downgrade as flag)
  ]) e
  where e.flag is true

),

-- If neither upgrade nor downgrade is flagged, classify as 'baseline' (or 'new')
-- This ensures EVERY subscription segment still exists as a row.
baseline as (
  select
    subscription_id,
    account_id,
    event_start,
    event_end,
    seats,
    plan_tier,
    mrr,
    'baseline' as lifecycle_event_type
  from bounded
  where coalesce(is_upgrade, false) = false
    and coalesce(is_downgrade, false) = false
),

final as (
  select * from event_types
  union all
  select * from baseline
),

deduped as (
  select
    *
  from final
  qualify row_number() over (
    partition by subscription_id, event_start
    order by case lifecycle_event_type
      when 'upgrade' then 1
      when 'downgrade' then 2
      when 'baseline' then 3
      else 99
    end
  ) = 1
)

select
  subscription_id,
  account_id,
  event_start,
  event_end,
  seats,
  plan_tier,
  mrr,
  lifecycle_event_type
from deduped

