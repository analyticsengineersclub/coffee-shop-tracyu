{{ config(materialized='view') }}

with pageview_times as (
    select *
    , lag(timestamp) over (partition by visitor_id, device_type order by timestamp asc) as previous_timestamp
    from {{ ref('stg__pageviews') }} 
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
