--dim_users
--truncate dds.dim_users cascade;

insert into dds.dim_users (user_domain_id, user_custom_id)
select distinct user_domain_id, user_custom_id 
from stg.events e 

--select count(*) from dds.dim_users du 