{{ config(
    materialized='view',
    schema='stg'
) }}


with source as (
    select *
    from {{ source('ravenstack_raw', 'ravenstack_accounts') }}
),

cleaned as (

    select 
        -- keys
        safe_cast(account_id as string) as account_id,

        -- basic attributes
        account_name,
        industry,
        country,
      
        -- dates
        safe_cast(signup_date as date) as signup_date,

        -- enums / categories (normalised)
        {{ normalise_referral_source('referral_source') }} as referral_source,
        {{ normalise_plan_tier('plan_tier') }} as plan_tier,

        -- numeric
        safe_cast(seats as int64) as seats,

        -- booleans (no nulls in staging)
        coalesce(is_trial, false) as is_trial,
        coalesce(churn_flag, false) as churn_flag

    from source

)

select * from cleaned
