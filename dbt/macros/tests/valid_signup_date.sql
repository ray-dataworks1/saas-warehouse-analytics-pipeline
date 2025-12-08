{% test valid_signup_date(model, column_name) %}

-- Fail if any signup date is before 2000-01-01 or in the future
select 
  {{ column_name }} as signup_date
from {{ model }}
where ({{ column_name }} < date '2000-01-01' 
    or {{ column_name }} > current_date)

{% endtest %}

-- assuming signup_dates are not nullable in the accounts table!