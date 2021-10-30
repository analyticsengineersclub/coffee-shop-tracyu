{{ config(materialized='table') }}

with visitor_ids as (
    select customer_id
    , visitor_id
    , row_number() over (partition by customer_id order by timestamp asc) as visitor_dup
    from {{ source('web_tracking', 'pageviews')}}
    where customer_id is not null
) 

, first_visitor_ids as (
    select customer_id
    , visitor_id as first_visitor_id 
    from visitor_ids
    where visitor_dup = 1
)

select pageviews.id
, first_visitor_id as visitor_id
, pageviews.device_type
, pageviews.timestamp	
, pageviews.page
, pageviews.customer_id
from {{ source('web_tracking', 'pageviews')}} pageviews
left join first_visitor_ids 
    on pageviews.customer_id = first_visitor_ids.customer_id
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
