{{ config(materialized='table') }}

-- user stitiching: for customer_id with multiple visitor ids, 
-- take the first visitor_id seen for the customer
with users_stiched as (
    select id
    , case when customer_id is not null 
            then first_value(visitor_id ignore nulls) over (partition by pageviews.customer_id order by timestamp asc)
            when customer_id is null then visitor_id
            else null end as visitor_id
    , device_type
    , timestamp	
    , page
    , customer_id
    from {{ source('web_tracking', 'pageviews')}}
)
-- add timestamp of previous pageviews
, pageview_times as (
    select *
    , lag(timestamp) over (partition by visitor_id, device_type order by timestamp asc) as previous_timestamp
    from users_stiched
    order by visitor_id, device_type, timestamp
)

-- determine if this pageview is start of new session
-- if first visit (previous_timestamp is null) then yes
-- if more than 30 min since last view then yes 
, new_sessions as (
    select * 
    , date_diff(timestamp, previous_timestamp, minute) as time_bt
    , case when previous_timestamp is null then 1 
            when date_diff(timestamp, previous_timestamp, minute) > 30 then 1 
            else 0 end as is_new_session
    from pageview_times
)

-- add session_id
select id 
, visitor_id
, device_type
, timestamp 
, page
, customer_id
, sum(is_new_session) over (ORDER BY visitor_id, timestamp) AS session_id
from new_sessions 