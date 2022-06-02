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
            coalesce(
                nullif(split_part($1::text, '.', 1), ''),
                (
                    select relnamespace::regnamespace::text
                    from pg_class
                    where oid = $1
                    limit 1
                )
            )
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


create function realtime.get_subscriptions(
)
    returns jsonb[]
    language sql
as $$
    select
        coalesce(
            array_agg(
                jsonb_build_object(
                    'schema_name',  realtime.to_schema_name(entity),
                    'table_name', realtime.to_table_name(entity),
                    'subscription_id', subscription_id,
                    'filters', filters,
                    'claims_role', claims_role
                )
            ),
            '{}'
        )
    from
        realtime.subscription s
    limit 1
$$;
