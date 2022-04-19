create function norm(jsonb) returns text
    language sql
    strict
as $$
    -- Normalizes timestamps and pretty prints
    select
        jsonb_pretty(
            regexp_replace(
                $1::text,
                '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+-\d+',
                '2000-01-01 01:01:01.000000-07',
                'g'
            )::jsonb
        );
$$;


create function clear_wal() returns void
    language plpgsql
as $$
begin
    perform pg_logical_slot_get_changes('realtime', null, null);
end;
$$;


create view walrus as
    select
        norm(x.data::jsonb) w2j_data,
        norm(xyz.wal) rec,
        xyz.is_rls_enabled,
        xyz.subscription_ids,
        xyz.errors
    from
        pg_logical_slot_get_changes(
            'realtime', null, null,
            'include-pk', '1',
            'include-transaction', 'false',
            'include-timestamp', 'true',
            'include-type-oids', 'true',
            'format-version', '2',
            'actions', 'insert,update,delete'
        ) x,
        lateral (
            select
                *
            from
                realtime.apply_rls(
                    wal := norm(x.data::jsonb)::jsonb,
                    max_record_bytes := 1048576
                )
        ) xyz(wal, is_rls_enabled, subscription_ids, errors);


create function seed_uuid(seed int)
    returns uuid
    language sql
as $$
    select
        extensions.uuid_generate_v5(
            'fd62bc3d-8d6e-43c2-919c-802ba3762271',
            seed::text
        )
$$;



-- This is the query used for polling for changes while respecting a publication
create view polling_query as
    with pub as (
        select
            concat_ws(
                ',',
                case when bool_or(pubinsert) then 'insert' else null end,
                case when bool_or(pubupdate) then 'update' else null end,
                case when bool_or(pubdelete) then 'delete' else null end
            ) as w2j_actions,
            coalesce(
                string_agg(
                    realtime.quote_wal2json(format('%I.%I', schemaname, tablename)::regclass),
                    ','
                ) filter (where ppt.tablename is not null),
                ''
            ) w2j_add_tables
        from
            pg_publication pp
            left join pg_publication_tables ppt
                on pp.pubname = ppt.pubname
        where
            pp.pubname = 'supabase_realtime'
        group by
            pp.pubname
        limit 1
    ),
    w2j as (
        select
            x.*, pub.w2j_add_tables
        from
             pub, -- always returns 1 row. possibly null entries
             pg_logical_slot_get_changes(
                'realtime', null, null,
                'include-pk', '1',
                'include-transaction', 'false',
                'include-type-oids', 'true',
                'include-timestamp', 'true',
                'write-in-chunks', 'true',
                'format-version', '2',
                'actions', pub.w2j_actions,
                'add-tables', pub.w2j_add_tables
            ) x
    )
    select
        xyz.wal,
        xyz.is_rls_enabled,
        xyz.subscription_ids,
        xyz.errors
    from
        w2j,
        realtime.apply_rls(
            wal := w2j.data::jsonb,
            max_record_bytes := 1048576
        ) xyz(wal, is_rls_enabled, subscription_ids, errors)
    where
        -- filter from w2j instead of pub to force `pg_logical_get_slots` to be called
        w2j.w2j_add_tables <> ''
        and xyz.subscription_ids[1] is not null
