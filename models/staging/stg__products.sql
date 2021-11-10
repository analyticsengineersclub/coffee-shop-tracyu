{{ config(materialized='table') }}

select id
    , name	
    , category	
    , cast(created_at as TIMESTAMP) as created_at
from {{ source('coffee_shop', 'products') }}
