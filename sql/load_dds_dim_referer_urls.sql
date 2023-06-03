--dim_referer_urls
--truncate dds.dim_referer_urls cascade;

INSERT INTO dds.dim_referer_urls
(referer_url, referer_url_scheme, referer_url_port, referer_medium)
select distinct referer_url, referer_url_scheme, referer_url_port, referer_medium
from stg.events e 
where date(e.event_timestamp) = %(load_date)
on conflict (page_url) do nothing
--select count(*) from dds.dim_referer_urls