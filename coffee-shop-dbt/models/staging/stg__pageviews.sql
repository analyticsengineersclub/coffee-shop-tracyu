{{ config(materialized='table') }}

with first_visitor_id as (
    select pageviews.id
    , first_value(visitor_id ignore nulls) over (partition by customer_id order by timestamp asc) as visitor_id
    , pageviews.device_type
    , pageviews.timestamp	
    , pageviews.page
    , pageviews.customer_id
    from {{ source('web_tracking', 'pageviews')}}
    where pageviews.customer_id is not null 

    union distinct

    select id
    , visitor_id
    , device_type
    , timestamp	
    , page
    , customer_id
    from {{ source('web_tracking', 'pageviews')}}
    where pageviews.customer_id is null 

, pageview_times as (
    select *
    , lag(timestamp) over (partition by visitor_id, device_type order by timestamp asc) as previous_timestamp
    from first_visitor_id
    order by visitor_id, device_type, timestamp
    limit 30
)

, new_sessions as (
    select * 
    , date_diff(timestamp, previous_timestamp, minute) as time_bt
    , case when previous_timestamp is null then 1 
        when date_diff(timestamp, previous_timestamp, minute) > 30 then 1 
        else 0 end as is_new_session
    from pageview_times
)

select id 
, visitor_id
, device_type
, timestamp 
, page
, customer_id
, SUM(is_new_session) OVER (ORDER BY visitor_id, device_type timestamp) AS session_id
from new_sessions 
