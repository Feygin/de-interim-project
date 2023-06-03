--dim_event_type
--truncate dds.dim_event_types cascade;

INSERT INTO dds.dim_event_types
(event_type)
select distinct case
			when page_url_path = '/confirmation' then 'purchase'
			when page_url_path  = '/payment' then 'payment'
			when page_url_path  = '/home' then 'home'
			when page_url_path  = '/cart' then 'cart'		
			else 'browse'
	   end as event_type
from stg.events e 
where date(e.event_timestamp) = %(load_date)
on conflict (event_type) do nothing

--select count(*) from dds.dim_event_types det 