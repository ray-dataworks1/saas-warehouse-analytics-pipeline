{{ config(
    materialized='view',
    schema='stg'
) }}


with source as (
    select *
    from {{ source('ravenstack_raw', 'ravenstack_subscriptions') }}
),

cleaned as (

    select 
        -- keys
        safe_cast(subscription_id as string) as subscription_id,
        safe_cast(account_id as string) as account_id,

        -- dates
        safe_cast(start_date as date) as start_date,
        safe_cast(end_date as date) as end_date,

        -- enums / categories (normalised)
        {{ normalise_plan_tier('plan_tier') }} as plan_tier,

        -- numeric
        safe_cast(seats as int64) as seats,
        safe_cast(mrr_amount as float64) as mrr_amount,
        safe_cast(arr_amount as float64) as arr_amount,

        -- booleans (no nulls in staging)
        coalesce(is_trial, false) as is_trial,
        coalesce(upgrade_flag, false) as upgrade_flag,
        coalesce(downgrade_flag, false) as downgrade_flag,
        coalesce(churn_flag, false) as churn_flag,
        coalesce(auto_renew_flag, false) as auto_renew_flag,

        -- billing frequency
        {{ normalise_billing_frequency('billing_frequency') }} as billing_frequency


    from source

)

select * from cleaned