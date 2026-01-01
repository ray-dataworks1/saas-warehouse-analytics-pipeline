{{ config(
    materialized = 'table',
    schema       = 'core',
    tags         = ['core', 'dimensional']
) }}

-- 1) base account data
with accounts as (
    select *
    from {{ ref('stg_accounts') }}
),

-- 2) latest subscription row per account
latest_subs as (
    select
        subscription_id,
        account_id,
        plan_tier           as current_plan_tier,
        start_date,
        end_date,
        row_number() over (
            partition by account_id
            order by end_date desc nulls last, start_date desc
        ) as rn
    from {{ ref('stg_subscriptions') }}
),

-- 3) keep only the most-recent row (rn = 1)
current_plan as (
    select account_id, current_plan_tier
    from latest_subs
    where rn = 1
),

-- 4) final dim table
dim as (
    select
        a.account_id,
        a.account_name                                      as account_name,
        a.industry,
        a.country,
        a.signup_date,
        date_trunc(a.signup_date, month)                     as signup_month,
        a.referral_source,
        a.plan_tier                                          as signup_plan_tier,
        c.current_plan_tier,
        a.seats,
        a.is_trial,
        a.churn_flag
    from accounts a
    left join current_plan c
      using (account_id)
)

select * from dim