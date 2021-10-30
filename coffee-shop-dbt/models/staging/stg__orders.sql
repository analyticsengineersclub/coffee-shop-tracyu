
{{ config(materialized='table') }}

select id
    , customer_id
    , total
    , address
    , state
    , zip
    , created_at
from {{ source('coffee_shop', 'orders') }}
