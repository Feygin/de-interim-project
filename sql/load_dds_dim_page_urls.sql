--dim_page_urls
--truncate dds.dim_page_urls cascade;

insert into dds.dim_page_urls (page_url, page_url_path)
select distinct page_url, page_url_path
from stg.events e 
where date(e.event_timestamp) = %(load_date)
on conflict (page_url) do nothing
--select count(*) from dds.dim_page_urls