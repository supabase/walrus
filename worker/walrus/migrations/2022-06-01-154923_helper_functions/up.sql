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

create function realtime.is_subscribed_to(
    schema_name text,
    table_name text
)
    returns bool
    language sql
as $$
    select
        exists(
            select
                1
            from
                realtime.subscription s
            where
                s.entity = format('%I.%I', schema_name, table_name)::regclass
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


create function realtime.get_subscription_ids(
    schema_name text,
    table_name text
)
    returns uuid[]
    language sql
as $$
    select
        coalesce(array_agg(subscription_id), '{}')
    from
        realtime.subscription s
    where
        s.entity = format('%I.%I', schema_name, table_name)::regclass
    limit 1
$$;

create function realtime.get_subscription_ids_by_role(
    schema_name text,
    table_name text,
    role_name text
)
    returns uuid[]
    language sql
as $$
    select
        coalesce(array_agg(subscription_id), '{}')
    from
        realtime.subscription s
    where
        s.entity = format('%I.%I', schema_name, table_name)::regclass
        and claims_role = role_name::regrole
    limit 1
$$;


create function realtime.get_subscriptions(
    schema_name text,
    table_name text
)
    returns jsonb[]
    language sql
as $$
    select
        coalesce(
            array_agg(
                jsonb_build_object(
                    'subscription_id', subscription_id,
                    'filters', filters,
                    'claims_role', claims_role
                )
            ),
            '{}'
        )
    from
        realtime.subscription s
    where
        s.entity = format('%I.%I', schema_name, table_name)::regclass
    limit 1
$$;
