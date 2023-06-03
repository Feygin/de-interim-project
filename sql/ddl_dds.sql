--1. распределение событий по часам; 
--2. количество купленных товаров в разрезе часа;
--3. топ-10 посещённых страниц, с которых был переход в покупку — список ссылок с количеством покупок.
---------------------------------
create schema if not exists dds;

drop table dds.dim_event_types;
drop table dds.dim_users;
drop table dds.dim_page_urls;
drop table dds.fct_events;


--dim_event_type
create table if not exists dds.dim_event_types
(
	id serial primary key,
	event_type varchar(100) not null  	
);

--dim_users
create table if not exists dds.dim_users
(
  	id serial primary key,
  	user_domain_id varchar(50) unique,
  	user_custom_id varchar(50) not null
);

--dim_page_urls
create table if not exists dds.dim_page_urls
(
	id serial primary key,
  	page_url varchar(250) not null,
  	page_url_path varchar(150) not null
  	--referer_url varchar(250) not null
);

--dim_referer_urls
create table if not exists dds.dim_referer_urls
(
	id serial primary key,  	
  	referer_url varchar(250) not null,
  	referer_url_scheme varchar(50) not null,
  	referer_url_port varchar(2) not null,
  	referer_medium varchar(50) not null
);

--fct_events
create table if not exists dds.fct_events(
	event_ts timestamp not null,
  	page_url_id int4 not null references dds.dim_page_urls(id),
  	referer_url_id int4 not null references dds.dim_referer_urls(id),
  	user_id int4 not null references dds.dim_users(id),
 	event_type_id int4 not null references dds.dim_event_types(id),
  	event_id varchar(50) not null
)

