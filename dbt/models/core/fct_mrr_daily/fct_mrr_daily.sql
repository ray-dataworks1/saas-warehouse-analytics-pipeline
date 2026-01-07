{{ config(materialized='table',
          schema='core') }}

-- fct_mrr_daily
-- Grain: 1 row per account_id + date
-- Purpose: daily MRR snapshot and movement classification (new/expansion/contraction/churn/reactivation)

with segments as (
  select
    account_id,
    subscription_id,
    event_start,
    event_end,
    mrr
  from {{ ref('fct_subscription_lifecycle') }}
),

-- Bound the date spine to only the range we actually need (keeps BigQuery cost sane)
span as (
  select
    min(event_start) as min_date,
    max(event_end)   as max_date
  from segments
),

dates as (
  select d.date
  from {{ ref('dim_date') }} d
  join span s
    on d.date between s.min_date and s.max_date
),

accounts as (
  -- Only accounts that appear in segments need to be in the grid
  select distinct account_id
  from segments
),

grid as (
  -- Full account × date grid so we can represent 0-MRR days (needed for churn/reactivation)
  select
    a.account_id,
    d.date
  from accounts a
  cross join dates d
),

daily_mrr as (
  -- Sum MRR from all active segments on each day
  select
    g.account_id,
    g.date,
    coalesce(sum(s.mrr), 0) as mrr
  from grid g
  left join segments s
    on s.account_id = g.account_id
   and g.date between s.event_start and s.event_end
  group by 1,2
),

with_prev as (
  select
    account_id,
    date,
    mrr,
    lag(mrr) over (partition by account_id order by date) as prev_mrr
  from daily_mrr
),

final as (
  select
    account_id,
    date,
    mrr,
    coalesce(prev_mrr, 0) as prev_mrr,
    (mrr - coalesce(prev_mrr, 0)) as mrr_movement,

    case
      when prev_mrr is null and mrr > 0 then 'new'
      when coalesce(prev_mrr, 0) = 0 and mrr > 0 then 'reactivation'
      when coalesce(prev_mrr, 0) > 0 and mrr = 0 then 'churn'
      when mrr > coalesce(prev_mrr, 0) then 'expansion'
      when mrr < coalesce(prev_mrr, 0) then 'contraction'
      else 'no_change'
    end as mrr_movement_type
  from with_prev
)

select * from final