select 1 from pg_create_logical_replication_slot('realtime', 'wal2json', false);

create table public.notes(
    id int primary key,
    body text,
    nullable_body text
);

alter table public.notes replica identity full;

insert into realtime.subscription(subscription_id, entity, claims, filters)
select
    seed_uuid(id),
    'public.notes',
    jsonb_build_object(
        'role', 'authenticated',
        'email', 'example@example.com',
        'sub', seed_uuid(id)::text
    ),
    array[(column_name, op, value, negate)::realtime.user_defined_filter]
from
    (
        values
            -- like: matches 'hello world'             → visible
            (1, 'body', 'like',  '%world%', false),
            -- like: does not match                    → not visible
            (2, 'body', 'like',  '%xyz%',   false),
            -- ilike: case-insensitive match            → visible
            (3, 'body', 'ilike', '%WORLD%', false),
            -- NOT LIKE: row does not match pattern    → visible
            (4, 'body', 'like',  '%xyz%',   true),
            -- NOT LIKE: row matches pattern           → not visible
            (5, 'body', 'like',  '%world%', true),
            -- NOT IN: 'hello world' outside the list → visible
            (6, 'body', 'in',    '{foo,bar}',          true),
            -- NOT IN: 'hello world' inside the list  → not visible
            (7, 'body', 'in',    '{hello world,other}', true),
            -- is null on nullable_body (null row)    → visible
            (8, 'nullable_body', 'is', 'null', false),
            -- is not null on nullable_body (null row)→ not visible
            (9, 'nullable_body', 'is', 'null', true)
    ) f(id, column_name, op, value, negate);


select clear_wal();
insert into public.notes(id, body, nullable_body) values (1, 'hello world', null);

delete from public.notes;

select subscription_id, filters from realtime.subscription order by subscription_id;

-- Expected visible subscription_ids: 1, 3, 4, 6, 8
select
    rec,
    is_rls_enabled,
    subscription_ids,
    errors
from
   walrus;


-- Confirm is with invalid value is rejected at subscription time
insert into realtime.subscription(subscription_id, entity, claims, filters)
select
    seed_uuid(10),
    'public.notes',
    jsonb_build_object('role', 'authenticated', 'email', 'example@example.com', 'sub', seed_uuid(10)::text),
    array[('body', 'is', 'invalid_value', false)::realtime.user_defined_filter];

-- Confirm in with more than 100 entries is rejected
insert into realtime.subscription(subscription_id, entity, claims, filters)
select
    seed_uuid(11),
    'public.notes',
    jsonb_build_object('role', 'authenticated', 'email', 'example@example.com', 'sub', seed_uuid(11)::text),
    array[('body', 'in', array[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1], true)::realtime.user_defined_filter];

drop table public.notes;
select pg_drop_replication_slot('realtime');
truncate table realtime.subscription;
