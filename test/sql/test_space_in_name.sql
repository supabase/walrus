select 1 from pg_create_logical_replication_slot('realtime', 'wal2json', false);

create table public."User Notes"(
    id int primary key
);

insert into realtime.subscription(subscription_id, entity, claims)
select
    seed_uuid(1),
    '"User Notes"'::regclass,
    jsonb_build_object(
        'role', 'authenticated',
        'email', 'example@example.com',
        'sub', seed_uuid(1)::text
    );

select clear_wal();
insert into public."User Notes"(id) values (1);

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
