-----------------------------------------------
--CDM
create schema if not exists cdm;

--1. распределение событий по часам; 
--v_count_events_hour
create or replace view cdm.v_count_events_hour as
select 
	det.event_type,
	count(fe.event_id),
	date_trunc('hour', fe.event_ts) as "hour"
from dds.fct_events fe 
left join dds.dim_event_types det on fe.event_type_id = det.id
group by det.event_type, "hour";

--2. количество купленных товаров в разрезе часа;
--v_count_sales_hour
create or replace view cdm.v_count_sales_hour as
select 
	date_trunc('hour',fe.event_ts) "hour"
	,count(*)
from dds.fct_events fe
left join dds.dim_event_types det on fe.event_type_id = det.id
where det.event_type = 'purchase'
group by "hour"
order by "hour"