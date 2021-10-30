{{ config(materialized='table') }}

with order_detail as (
    select order_items.id as order_items_id
         , order_items.order_id
         , orders.created_at as order_created_at
         , orders.customer_id
         , order_items.product_id 
    from {{ ref('stg__order_items') }} order_items
    left join {{ ref('stg__orders') }} orders
        on order_items.order_id=orders.id
)

, customer_orders as (
    select customer_id
        , id as order_id
        , created_at
        , row_number() over (partition by customer_id order by created_at asc) as customer_order_number
    from {{ ref('stg__orders') }} orders
)

select order_detail.*
    , customer_order_number
    , product_prices.price as product_price_at_time_of_order
from order_detail 
left join customer_orders 
    on customer_orders.order_id = order_detail.order_id
left join {{ ref('stg__product_prices') }} product_prices
    on order_detail.product_id = product_prices.product_id
    and order_detail.order_created_at between product_prices.created_at and product_prices.ended_at
