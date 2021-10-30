
{{ config(materialized='table') }}

select *
from {{ source('coffee_shop', 'customers') }}
