{{ config(
    severity='warn'
) }}
with src as (
  select min(signup_date) as min_signup
  from {{ ref('stg_accounts') }}
  where signup_date >= date '2000-01-01'
),
dim as (
  select min(date) as min_dim, max(date) as max_dim
  from {{ ref('dim_date') }}
)
select *
from src, dim
where min_dim > min_signup
   or max_dim != current_date()



