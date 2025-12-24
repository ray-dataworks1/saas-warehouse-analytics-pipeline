{{ config(
        severity='error',
        tags=['dim_date', 'core']
) }}

with ordered as (
  select
    date,
    lead(date) over (order by date) as next_date
  from {{ ref('dim_date') }}
),
gaps as (
  select *
  from ordered
  where next_date is not null
    and date_diff(next_date, date, day) != 1
)
select * from gaps


