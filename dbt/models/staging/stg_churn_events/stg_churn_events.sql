{{ config(
   materialized='view',
   schema='stg'
   ) }}

with source as (
    select *
    from {{ source('ravenstack_raw', 'ravenstack_churn_events') }}
),  

cleaned as (
    select
        -- keys
        safe_cast(churn_event_id as string) as churn_event_id,
        safe_cast(account_id as string) as account_id,

        -- dates
        safe_cast(churn_date as date) as churn_date,

        -- enums / categories (normalised)
        {{ normalise_churn_reason('reason_code') }} as reason_code,

        -- numeric
        safe_cast(refund_amount_usd as float64) as refund_amount_usd,

        -- booleans/flags
        safe_cast(preceding_upgrade_flag as boolean) as preceding_upgrade_flag,
        safe_cast(preceding_downgrade_flag as boolean) as preceding_downgrade_flag,
        safe_cast(is_reactivation as boolean) as is_reactivation,

        -- text
        lower(trim(feedback_text)) as feedback_text
    from source
)
select

    *
from cleaned