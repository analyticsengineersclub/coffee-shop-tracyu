{{ config(materialized='view') }}

-- weekly sales by product category
with product_categories as (select id as product_id
, category as product_category
from {{ ref('stg__products') }}
)

select 
  date_trunc(order_created_at, month) as date_month,
{{ sum_sales_by_category(product_categories=['coffee beans', 'merch', 'brewing supplies']) }}
from {{ ref('revenue') }}
left join product_categories using (product_id)
group by 1