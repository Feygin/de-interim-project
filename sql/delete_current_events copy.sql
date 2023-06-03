-- удаляем события за день из stg для идемпотентности
delete from stg.events
where date(event_timestamp) = %(load_date);
