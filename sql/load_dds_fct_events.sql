------------------------------------------------------------------
--dds.fct_events 
--truncate dds.fct_events cascade;

INSERT INTO dds.fct_events
(event_ts, page_url_id, referer_url_id, user_id, event_type_id, event_id)
with cte_events as (	
	SELECT event_id, event_timestamp, event_type, page_url, page_url_path, referer_url, 
		referer_url_scheme, referer_url_port, referer_medium, utm_medium, utm_source, utm_content, utm_campaign, 
		click_id, geo_latitude, geo_longitude, geo_country, geo_timezone, geo_region_name, ip_address, 
		browser_name, browser_user_agent, browser_language, os, os_name, os_timezone, device_type, device_is_mobile, user_custom_id, user_domain_id
		, case
			when page_url_path = '/confirmation' then 'purchase'
			when page_url_path  = '/payment' then 'payment'
			when page_url_path  = '/home' then 'home'
			when page_url_path  = '/cart' then 'cart'		
			else 'browse'
	   end as event_type_name
	FROM stg.events)
select 
	e.event_timestamp as event_ts, 
	dpu.id as page_url_id,
	dru.id as referer_url_id,
	du.id as user_id,
	det.id as event_type_id,
	e.event_id as event_id
from cte_events e 
inner join dds.dim_page_urls dpu on e.page_url = dpu.page_url 
inner join dds.dim_referer_urls dru on e.referer_url = dru.referer_url 
inner join dds.dim_users du on e.user_domain_id = du.user_domain_id 
inner join dds.dim_event_types det on e.event_type_name = det.event_type