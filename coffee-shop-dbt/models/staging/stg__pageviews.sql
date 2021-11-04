{{ config(materialized='table') }}

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