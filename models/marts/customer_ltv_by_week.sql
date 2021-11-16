{{ config(materialized='table') }}

-- for each order_items id, determine the week of the order
-- and the week the customer was acquired
with weekly_revenue as (
    select *
    , cast(date_trunc(order_created_at, week) as date) as order_week
    , cast(min(date_trunc(order_created_at, week))
        over (partition by customer_id
        order by order_created_at asc) as date) as week_acquired
    from {{ ref('revenue')}}
)

-- for each customer_id, determine the week in their cusomter lifetime
-- with the first week starting at 1; total revenue for each customer and week
, weeks_counted as (
    select customer_id
    , date_diff(order_week, week_acquired, week)+1 as week_count
    , sum(product_price_at_time_of_order) as revenue
    from weekly_revenue
    group by 1,2
    order by customer_id, week_count desc
)

-- for each customer, determine the max number of weeks 
-- and generate a count from to max number of weeks 
, max_weeks as ( 
    select *
    from (select customer_id
                , max(week_count) as max_week_count
         from weeks_counted
         group by 1
         ) wc 
    join unnest(generate_array(1, wc.max_week_count)) as week_num
)

-- for each customer, list id, week, revenue for the week
-- and cumulative revenue
select max_weeks.customer_id
, max_weeks.week_num as week
, coalesce(revenue, 0) as revenue
, sum(coalesce(revenue, 0)) over (partition by max_weeks.customer_id order by week_num) as cumulative_revenue
from max_weeks
left join weeks_counted 
    on max_weeks.customer_id=weeks_counted.customer_id
    and max_weeks.week_num=weeks_counted.week_count
order by customer_id, week
