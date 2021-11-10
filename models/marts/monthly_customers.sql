{{ config(materialized='view') }}

select date_trunc(first_order_at, month) month
, count(distinct customer_id) number_of_new_customers
from {{ ref('customers') }}
group by 1
