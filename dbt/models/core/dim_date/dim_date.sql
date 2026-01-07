{{ config(
    materialized = 'table',
    schema='core',
    tags = ['core', 'dimension']
) }}

with span as (
  select
    least(
      -- earliest date we care about across sources
      (select min(signup_date) from {{ ref('stg_accounts') }} where signup_date >= date '2000-01-01'),
      (select min(cast(usage_date as date)) from {{ ref('stg_feature_usage') }}),
      date '2000-01-01'
    ) as min_date,

    greatest(
      -- latest date across sources, plus a safety buffer
      (select coalesce(max(c.churn_date), date '2000-01-01')
       from {{ ref('stg_churn_events') }} c),
      (select max(cast(usage_date as date)) from {{ ref('stg_feature_usage') }}),
      current_date()
    ) as max_date
),
dates as (
  select date_add(min_date, interval i day) as calendar_date
  from span, unnest(generate_array(0, date_diff(max_date, min_date, day))) as i
)
select
  calendar_date                                       as date,
  cast(format_date('%Y%m%d', calendar_date) as int64) as date_key,
  extract(year  from calendar_date)                   as year,
  extract(month from calendar_date)                   as month,
  extract(day   from calendar_date)                   as day,
  format_date('%Y-%m', calendar_date)                 as month_id,
  extract(isoweek from calendar_date)                 as iso_week,
  extract(quarter from calendar_date)                 as quarter,
  case when extract(dayofweek from calendar_date) in (1,7) then true else false end as is_weekend
from dates