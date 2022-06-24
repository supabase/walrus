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
                pg_publication pp
                left join pg_publication_tables ppt
                    on pp.pubname = ppt.pubname
            where
                pp.pubname = publication_name
                and ppt.schemaname = schema_name
                and ppt.tablename = table_name
            limit 1
        )
$$;

create function realtime.is_rls_enabled(schema_name text, table_name text)
    returns bool
    language sql
as $$
    select
        relrowsecurity
    from
        pg_class
    where
        oid = format('%I.%I', schema_name, table_name)::regclass
    limit 1;
$$;

create function realtime.subscribed_roles(
    schema_name text,
    table_name text
)
    returns text[]
    language sql
as $$
    select
        coalesce(array_agg(distinct claims_role), '{}')
    from
        realtime.subscription s
    where
        s.entity = format('%I.%I', schema_name, table_name)::regclass
    limit 1
$$;

create function realtime.selectable_columns(
    schema_name text,
    table_name text,
    role_name text
)
    returns text[]
    language sql
as $$
    select
        coalesce(
            array_agg(
                pa.attname::text
                order by pa.attnum asc
            ),
            array['abc']
        )
    from
        pg_class e
        join pg_attribute pa
            on e.oid = pa.attrelid
    where
        e.oid = format('%I.%I', $1, $2)::regclass
        and pa.attnum > 0
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
                1,
                character_length(maybe_quoted_name)-2
            )
            else maybe_quoted_name
        end
    from
        x
$$;


create function realtime.is_visible_through_filters(
    columns jsonb,
    subscription_ids uuid[]
)
    returns uuid[]
    language plpgsql
as $$
declare
    cols realtime.wal_column[];
    visible_to_subscription_ids uuid[] = '{}';
    subscription_id uuid;
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
            subs.subscription_id,
            subs.filters
        from
            realtime.subscription subs
        where
            subs.subscription_id = any(subscription_ids)
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
    schema_name text,
    table_name text,
    columns jsonb,
    subscription_ids uuid[]
)
    returns uuid[]
    language plpgsql
as $$
declare
    entity_ regclass = format('%I.%I', schema_name, table_name)::regclass;
    cols realtime.wal_column[];
    visible_to_subscription_ids uuid[] = '{}';
    subscription_id uuid;
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
            subs.subscription_id,
            subs.claims
        from
            realtime.subscription subs
        where
            subs.subscription_id = any(subscription_ids)
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
$$

alter table realtime.subscription add column schema_name text generated always as (realtime.to_schema_name(entity)) stored;
alter table realtime.subscription add column table_name text generated always as (realtime.to_table_name(entity)) stored;
alter table realtime.subscription add column claims_role_name text generated always as (realtime.to_regrole((claims ->> 'role'::text))) stored;

alter table realtime.subscription alter schema_name set not null;
alter table realtime.subscription alter table_name set not null;
alter table realtime.subscription alter claims_role_name set not null;
-- alter table realtime.subscription alter filters drop default;
-- alter table realtime.subscription alter filters type jsonb using to_jsonb(filters);
-- alter table realtime.subscription alter filters set default '[]';
