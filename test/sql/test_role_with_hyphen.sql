select 1 from pg_create_logical_replication_slot('realtime', 'wal2json', false);

create role "has-hyphen" nologin noinherit;
grant usage on schema public to "has-hyphen";
alter default privileges in schema public grant all on tables to "has-hyphen";
alter default privileges in schema public grant all on functions to "has-hyphen";
alter default privileges in schema public grant all on sequences to "has-hyphen";



create table public.notes(
    id int primary key
);

insert into realtime.subscription(subscription_id, entity, claims)
select
    seed_uuid(1),
    'public.notes',
    jsonb_build_object(
        'role', 'has-hyphen',
        'email', 'example@example.com',
        'sub', seed_uuid(1)::text
    );

select clear_wal();
insert into public.notes(id) values (1);

select
    rec,
    is_rls_enabled,
    subscription_ids,
    errors
from
   walrus;


drop table public.notes;
select pg_drop_replication_slot('realtime');
truncate table realtime.subscription;
