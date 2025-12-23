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

{% macro normalise_ticket_priority(column_name) -%}
    -- standardise ticket priority to clean, lowercase values
    case lower(trim({{ column_name}}))
        when 'low' then 'low'
        when 'medium' then 'medium'
        when 'high' then 'high'
        when 'urgent' then 'urgent'
        else 'medium'
    end
{%- endmacro %}

{% macro normalise_billing_frequency(column_name) -%}
    -- standardise billing frequency to clean, lowercase values
    case lower(trim({{ column_name}}))
        when 'monthly' then 'monthly'
        when 'quarterly' then 'quarterly'
        when 'annual' then 'annual'
        else 'monthly'
    end
{%- endmacro %}

{% macro normalise_churn_reason(column_name) -%}
    -- standardise churn reason to clean, lowercase values
    case lower(trim({{ column_name}}))
        when 'pricing' then 'pricing'
        when 'features' then 'features'
        when 'competition' then 'competition'
        when 'support' then 'support'
        when 'budget' then 'budget'
        when 'service' then 'service'
        when 'other' then 'other'
        when 'unknown' then 'unknown'
        else 'other'
    end
{%- endmacro %}