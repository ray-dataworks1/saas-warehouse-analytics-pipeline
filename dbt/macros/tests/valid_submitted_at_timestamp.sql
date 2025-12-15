{% test valid_submitted_at_timestamp(model, column_name) %}

-- Fail if any submitted_at timestamp is before 2000-01-01 or in the future
select 
  {{ column_name }} as submitted_at
from {{ model }}
where ({{ column_name }} < timestamp '2000-01-01 00:00:00' 
    or {{ column_name }} > current_timestamp)

{% endtest %}

-- assuming submitted_at are not nullable in the support tickets table!