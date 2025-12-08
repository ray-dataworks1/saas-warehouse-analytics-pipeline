{% macro normalise_referral_source(column_name) -%}
    -- standardise referral source to clean, lowecase values
    case lower(trim({{ column_name}}))
        when 'organic' then 'organic'
        when 'partner' then 'partner'
        when 'ads' then 'ads'
        when 'event' then 'event'
        else 'other'
    end
{%- endmacro %}

{% macro normalise_plan_tier(column_name) -%}
    -- standardise plan tier to clean, lowercase values
    case lower(trim({{ column_name}}))
        when 'free' then 'Free'
        when 'basic' then 'Basic'
        when 'pro' then 'Pro'
        when 'enterprise' then 'Enterprise'
        else 'Free'
    end
{%- endmacro %}