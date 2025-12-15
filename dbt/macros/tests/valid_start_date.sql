{% test valid_start_date(model, column_name) %}
-- Fail if any start_date is before 2000-01-01 or in the future}
select 
  {{ column_name }} as start_date
from {{ model }}
where ({{ column_name }} < date '2000-01-01' 
    or {{ column_name }} > current_date)
{% endtest %}   
-- assuming start_dates are not nullable in the relevant table!