{{ config(materialized='table',
          schema='core') }}

-- fct_feature_usage_daily
-- Grain: 1 row per account_id + date + feature
-- Source is pre-aggregated per subscription_id + usage_date + feature_name.
-- We map subscription_id -> account_id, then sum to account level.

with usage as (
  select
    subscription_id,
    cast(usage_date as date) as date,
    cast(feature_name as string) as feature,
    cast(usage_count as int64) as usage_count,
    cast(usage_duration_secs as int64) as usage_duration_seconds,
    cast(error_count as int64) as error_count,
    cast(is_beta_feature as bool) as is_beta_feature
  from {{ ref('stg_feature_usage') }}
),

sub_to_account as (
  select
    subscription_id,
    account_id
  from {{ ref('stg_subscriptions') }}  -- adjust if your staging model name differs
),

joined as (
  select
    a.account_id,
    u.date,
    u.feature,
    u.is_beta_feature,
    u.usage_count,
    u.usage_duration_seconds,
    u.error_count
  from usage u
  join sub_to_account a
    on u.subscription_id = a.subscription_id
),

final as (
  select
    account_id,
    date,
    feature,

    -- A feature can be both beta and non-beta across subscriptions, so this makes "beta if any beta rows exist"
    logical_or(is_beta_feature) as is_beta_feature,

    sum(usage_count) as usage_events,
    sum(usage_duration_seconds) as usage_duration_seconds,
    sum(error_count) as error_count

  from joined
  group by 1,2,3
)

select * from final