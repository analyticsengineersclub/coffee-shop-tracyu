
{{ config(materialized='table') }}

WITH orders AS (
    SELECT customer_id
    , MIN(created_at) first_order_at
    , COUNT(DISTINCT id) number_of_orders
    FROM {{ source('coffee_shop', 'orders') }}
    GROUP BY 1
)

SELECT o.customer_id
, c.name
, c.email
, o.first_order_at
, o.number_of_orders
FROM {{ source('coffee_shop', 'customers') }} c
LEFT JOIN orders o ON o.customer_id=c.id
ORDER BY first_order_at asc
