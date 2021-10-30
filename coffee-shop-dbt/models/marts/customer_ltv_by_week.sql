{{ config(materialized='table') }}

with weekly_revenue as (
    select *
    , cast(date_trunc(order_created_at, week) as date) as order_week
    , cast(min(date_trunc(order_created_at, week))
        over (partition by customer_id
        order by order_created_at asc) as date) as week_acquired
    from {{ ref('revenue')}}
)

, weeks_counted as (
    select customer_id
    , date_diff(order_week, week_acquired, week)+1 as week_count
    , sum(product_price_at_time_of_order) as revenue
    from weekly_revenue
    group by 1,2
    order by customer_id, week_count desc
)

, max_weeks as ( 
    select *
    from (select customer_id, max(week_count) as max_week_count
            FROM weeks_counted
            group by 1
            ) wc join
            unnest(generate_array(1, wc.max_week_count)) week_num
)

select max_weeks.customer_id
, max_weeks.week_num as week
, coalesce(revenue, 0) as revenue
, sum(revenue) over (partition by max_weeks.customer_id order by week_num) as cumulative_revenue
from max_weeks
left join weeks_counted 
    on max_weeks.customer_id=weeks_counted.customer_id
    and weeks_counted.week_count=max_weeks.week_num
