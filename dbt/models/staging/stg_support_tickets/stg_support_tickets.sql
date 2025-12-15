{{ config(
    materialized='view',
    schema='stg'
) }}


with source as (
    select *
    from {{ source('ravenstack_raw', 'ravenstack_support_tickets') }}
),

cleaned as (

    select 
        -- keys
        safe_cast(ticket_id as string) as ticket_id,
        safe_cast(account_id as string) as account_id,

        -- dates, timestamps and hours
        safe_cast(submitted_at as timestamp) as submitted_at,
        safe_cast(closed_at as timestamp) as closed_at,
        safe_cast(resolution_time_hours as float64) as resolution_time_hours,

        -- enums / categories (normalised)
        {{ normalise_ticket_priority('priority') }} as priority_level,

        -- numeric
        safe_cast(first_response_time_minutes as int64) as first_response_time_minutes,
        safe_cast(satisfaction_score as float64) as satisfaction_score,

        -- booleans (no nulls in staging)
        coalesce(escalation_flag, false) as escalation_flag
    from source
)

select * from cleaned
