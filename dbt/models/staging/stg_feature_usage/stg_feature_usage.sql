{{ config(
    materialized='view',
    schema='stg'
) }}

with source as (
    select *
    from {{ source('ravenstack_raw', 'ravenstack_feature_usage') }}
),

typed as (
    select
        safe_cast(usage_id as string) as usage_id,
        safe_cast(subscription_id as string) as subscription_id,
        safe_cast(usage_date as date) as usage_date,
        feature_name,
        coalesce(is_beta_feature, false) as is_beta_feature,

        safe_cast(usage_count as int64) as usage_count,
        safe_cast(usage_duration_secs as int64) as usage_duration_secs,
        safe_cast(error_count as int64) as error_count
    from source
),

aggregated as (
    select
        subscription_id,
        usage_date,
        feature_name,
        is_beta_feature,

        sum(coalesce(usage_count, 0)) as usage_count,
        sum(coalesce(usage_duration_secs, 0)) as usage_duration_secs,
        sum(coalesce(error_count, 0)) as error_count,

        array_agg(usage_id ignore nulls) as source_usage_ids,
        array_length(array_agg(usage_id ignore nulls)) as source_row_count
    from typed
    group by 1,2,3,4
)

select
    {{ dbt_utils.generate_surrogate_key([
      'subscription_id',
      'cast(usage_date as string)',
      'feature_name',
      'cast(is_beta_feature as string)'
    ]) }} as feature_usage_sk,

    *
from aggregated
