{% snapshot dim_date_snapshot %}

{{
    config(
        target_database = target.database,
        target_schema   = 'snapshots',
        unique_key      = 'date',
        strategy        = 'check',
        check_cols      = ['*']
    )
}}

select * from {{ ref('dim_date') }}

{% endsnapshot %}
