{% test name_starts_with_feature(model, column_name) %}

-- Fail if any non-null feature_name does NOT match ^feature_[1-9][0-9]*$
select 
  {{ column_name }} as feature_name
from {{ model }}
where {{column_name}} is not NULL
    and not regexp_contains({{ column_name }}, r'^feature_[1-9[0-9]*$')

{% endtest %}