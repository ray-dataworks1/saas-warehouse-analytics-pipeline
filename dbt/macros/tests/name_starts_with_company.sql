{% test name_starts_with_company(model, column_name) %}

-- Fail if any non-null account_name does NOT match ^Company_[1-9][0-9]*$
select 
  {{ column_name }} as account_name
from {{ model }}
where {{column_name}} is not NULL
    and not regexp_contains({{ column_name }}, r'^Company_[1-9[0-9]*$')

{% endtest %}