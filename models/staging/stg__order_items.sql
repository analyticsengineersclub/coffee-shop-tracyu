
{{ config(materialized='table') }}

select *
from {{ source('coffee_shop', 'order_items') }}
