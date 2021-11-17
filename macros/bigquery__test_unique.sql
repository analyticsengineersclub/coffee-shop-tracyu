{% test bigquery__test_unique(model, column_name) %}

with dbt_test__target as (
  
  select {{ column_name }} as unique_field
  from {{ model }}
  where {{ column_name }} is not null
  
)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1

{% endtest %}