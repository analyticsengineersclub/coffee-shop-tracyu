-- A customer_id will sometimes be associated with multiple visitor_ids
-- The stg table takes the first visitor_id seen for a given customer_id
-- so that there were only be one visitor_id for each customer_id 
-- Therefore return records where this isn't true to make the test fail

select customer_id, count(distinct visitor_id) as visitor_id_count
from {{ ref('stg__pageviews')}}
where customer_id is not null
group by 1 
having (visitor_id_count > 1)
