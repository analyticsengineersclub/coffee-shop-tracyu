{{ config(materialized='table') }}

select *
from {{ source('coffee_shop', 'product_prices') }}
