create function realtime.is_in_publication(
    schema_name text,
    table_name text,
    publication_name text
)
    returns bool
    language sql
as $$
    select
        exists(
            select
                1
            from
                pg_publication_tables ppt
            where
                ppt.pubname = publication_name
                and ppt.schemaname = schema_name
                and ppt.tablename = table_name
            limit 1
        )
$$;

create function realtime.is_rls_enabled(table_oid oid)
    returns bool
    language sql
as $$
    select
        relrowsecurity
    from
        pg_class
    where
        oid = table_oid
    limit 1;
$$;


create function realtime.selectable_columns(
    table_oid oid,
    role_name text
)
    returns jsonb[]
    language sql
as $$
    select
        coalesce(
            array_agg(
                jsonb_build_object(
                    'name', pa.attname::text,
                    'type', pt.typname::text
                )
                order by pa.attnum asc
            ),
            array[]::jsonb[]
        )
    from
        pg_class e
        join pg_attribute pa
            on e.oid = pa.attrelid
        join pg_type pt
            on pa.atttypid = pt.oid
    where
        e.oid = table_oid --format('%I.%I', $1, $2)::regclass
        and pa.attnum > 0
        and pg_catalog.has_column_privilege(role_name, table_oid, pa.attname, 'SELECT')
        and not pa.attisdropped
$$;


create function realtime.to_table_name(regclass)
    returns text
    language sql
    immutable
as
$$
    with x(maybe_quoted_name) as (
         select
            coalesce(nullif(split_part($1::text, '.', 2), ''), $1::text)
    )
    select
        case
            when x.maybe_quoted_name like '"%"' then substring(
                x.maybe_quoted_name,
                2,
                character_length(x.maybe_quoted_name)-2
            )
            else x.maybe_quoted_name
        end
    from
        x
$$;

create function realtime.to_schema_name(regclass)
    returns text
    language sql
    immutable
as
$$
    with x(maybe_quoted_name) as (
         select
            relnamespace::regnamespace::text
        from pg_class
        where oid = $1
        limit 1
    )
    select
        case
            when maybe_quoted_name like '"%"' then substring(
                maybe_quoted_name,
                2,
                character_length(maybe_quoted_name)-2
            )
            else maybe_quoted_name
        end
    from
        x
$$;


create function realtime.is_visible_through_filters(
    columns jsonb,
    ids int8[] -- realtime.subscription.id
)
    returns int8[]
    language plpgsql
as $$
declare
    cols realtime.wal_column[];
    visible_to_subscription_ids int8[] = '{}';
    subscription_id int8;
    filters realtime.user_defined_filter[];
    subscription_has_access bool;
begin
    cols = (
        select
            array_agg(
                (
                    c ->> 'name',
                    c ->> 'type_name',
                    c ->> 'type_oid',
                    c -> 'value',
                    c ->> 'is_pkey',
                    c ->> 'is_selectable'
                )::realtime.wal_column
            )
        from
            jsonb_array_elements(columns) c
    );

    for subscription_id, filters in (
        select
            subs.id,
            subs.filters
        from
            realtime.subscription subs
        where
            subs.id = any(ids)
        )
    loop

        subscription_has_access = realtime.is_visible_through_filters(
            columns := cols,
            filters := filters
        );

        if subscription_has_access then
            visible_to_subscription_ids = visible_to_subscription_ids || subscription_id;
        end if;
    end loop;

    return visible_to_subscription_ids;
end;
$$;


create function realtime.is_visible_through_rls(
    table_oid oid,
    columns jsonb,
    ids int8[]
)
    returns int8[]
    language plpgsql
as $$
declare
    entity_ regclass = table_oid::regclass;
    cols realtime.wal_column[];
    visible_to_subscription_ids int8[] = '{}';
    subscription_id int8;
    subscription_has_access bool;
    claims jsonb;
begin
    cols = (
        select
            array_agg(
                (
                    c ->> 'name',
                    c ->> 'type_name',
                    c ->> 'type_oid',
                    c -> 'value',
                    c ->> 'is_pkey',
                    c ->> 'is_selectable'
                )::realtime.wal_column
            )
        from
            jsonb_array_elements(columns) c
    );

    -- Create the prepared statement
    if (select 1 from pg_prepared_statements where name = 'walrus_rls_stmt' limit 1) > 0 then
        deallocate walrus_rls_stmt;
    end if;

    execute realtime.build_prepared_statement_sql('walrus_rls_stmt', entity_, cols);

    for subscription_id, claims in (
        select
            subs.id,
            subs.claims
        from
            realtime.subscription subs
        where
            subs.id = any(ids)
        )
    loop
        -- Check if RLS allows the role to see the record
        perform
            set_config('role', (claims ->> 'role')::text, true),
            set_config('request.jwt.claims', claims::text, true);

        execute 'execute walrus_rls_stmt' into subscription_has_access;

        if subscription_has_access then
            visible_to_subscription_ids = visible_to_subscription_ids || subscription_id;
        end if;
    end loop;

    perform set_config('role', null, true);

    return visible_to_subscription_ids;
end;
$$;


create function realtime.get_table_oid(
    schema_name text,
    table_name text
)
    returns oid
    language sql
as $$
    select format('%I.%I', schema_name, table_name)::regclass::oid;
$$;

alter table realtime.subscription add column schema_name text generated always as (realtime.to_schema_name(entity)) stored;
alter table realtime.subscription add column table_name text generated always as (realtime.to_table_name(entity)) stored;
alter table realtime.subscription add column claims_role_name text generated always as (realtime.to_regrole((claims ->> 'role'::text))) stored;

alter table realtime.subscription alter schema_name set not null;
alter table realtime.subscription alter table_name set not null;
alter table realtime.subscription alter claims_role_name set not null;

create index ix_realtime_subscription_subscription_id on realtime.subscription (subscription_id);
