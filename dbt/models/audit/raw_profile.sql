{{ config(
    materialized='table',
    alias='raw_profile'
) }}

with stats as (

    select
        'ravenstack_accounts' as table_name,
        count(*) as row_count,
        min(safe_cast(signup_date as date)) as min_signup_date,
        max(safe_cast(signup_date as date)) as max_signup_date,
        cast(null as date) as min_churn_date,
        cast(null as date) as max_churn_date,
        cast(null as date) as min_usage_date,
        cast(null as date) as max_usage_date,
        cast(null as date) as min_start_date,
        cast(null as date) as max_start_date,
        cast(null as date) as min_end_date,
        cast(null as date) as max_end_date,
        cast(null as timestamp) as min_submitted_at,
        cast(null as timestamp) as max_submitted_at,
        cast(null as timestamp) as min_closed_at,
        cast(null as timestamp) as max_closed_at
    from {{ source('ravenstack_raw', 'ravenstack_accounts') }}

    union all

    select
        'ravenstack_subscriptions' as table_name,
        count(*) as row_count,
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        min(safe_cast(start_date as date)) as min_start_date,
        max(safe_cast(start_date as date)) as max_start_date,
        min(safe_cast(end_date   as date)) as min_end_date,
        max(safe_cast(end_date   as date)) as max_end_date,
        cast(null as timestamp), cast(null as timestamp),
        cast(null as timestamp), cast(null as timestamp)
    from {{ source('ravenstack_raw', 'ravenstack_subscriptions') }}

    union all

    select
        'ravenstack_feature_usage' as table_name,
        count(*) as row_count,
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        min(safe_cast(usage_date as date)) as min_usage_date,
        max(safe_cast(usage_date as date)) as max_usage_date,
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        cast(null as timestamp), cast(null as timestamp),
        cast(null as timestamp), cast(null as timestamp)
    from {{ source('ravenstack_raw', 'ravenstack_feature_usage') }}

    union all

    select
        'ravenstack_support_tickets' as table_name,
        count(*) as row_count,
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        min(safe_cast(submitted_at as timestamp)) as min_submitted_at,
        max(safe_cast(submitted_at as timestamp)) as max_submitted_at,
        min(safe_cast(closed_at    as timestamp)) as min_closed_at,
        max(safe_cast(closed_at    as timestamp)) as max_closed_at
    from {{ source('ravenstack_raw', 'ravenstack_support_tickets') }}

    union all

    select
        'ravenstack_churn_events' as table_name,
        count(*) as row_count,
        cast(null as date), cast(null as date),
        min(safe_cast(churn_date as date)) as min_churn_date,
        max(safe_cast(churn_date as date)) as max_churn_date,
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        cast(null as date), cast(null as date),
        cast(null as timestamp), cast(null as timestamp),
        cast(null as timestamp), cast(null as timestamp)
    from {{ source('ravenstack_raw', 'ravenstack_churn_events') }}

)

select * from stats
