/*
    WALRUS:
        Write Ahead Log Realtime Unified Security
*/

create schema cdc;
grant usage on schema cdc to postgres;
grant usage on schema cdc to authenticated;


create or replace function cdc.get_schema_name(entity regclass)
returns text
immutable
language sql
as $$
    SELECT nspname::text
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS ns
      ON c.relnamespace = ns.oid
    WHERE c.oid = entity;
$$;


create or replace function cdc.get_table_name(entity regclass)
returns text
immutable
language sql
as $$
    SELECT c.relname::text
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS ns
      ON c.relnamespace = ns.oid
    WHERE c.oid = entity;
$$;


create or replace function cdc.selectable_columns(
    entity regclass,
    role_ text default 'authenticated'
)
returns text[]
language sql
stable
as $$
/*
Returns a text array containing the column names in *entity* that *role_* has select access to
*/
    select
        coalesce(
            array_agg(rcg.column_name order by c.ordinal_position),
            '{}'::text[]
        )
    from
        information_schema.role_column_grants rcg
        inner join information_schema.columns c
            on rcg.table_schema = c.table_schema
            and rcg.table_name = c.table_name
            and rcg.column_name = c.column_name
    where
        -- INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
        rcg.privilege_type = 'SELECT'
        and rcg.grantee = role_
        and rcg.table_schema = cdc.get_schema_name(entity)
        and rcg.table_name = cdc.get_table_name(entity);
$$;


create or replace function cdc.get_column_type(entity regclass, column_name text)
    returns regtype
    language sql
as $$
    select atttypid::regtype
    from pg_catalog.pg_attribute
    where attrelid = entity
    and attname = column_name
$$;


-- Subset from https://postgrest.org/en/v4.1/api.html#horizontal-filtering-rows
create type cdc.equality_op as enum(
    'eq', 'neq', 'lt', 'lte', 'gt', 'gte'
);

create type cdc.user_defined_filter as (
    column_name text,
    op cdc.equality_op,
    value text
);


create table cdc.subscription (
    -- Tracks which users are subscribed to each table
    id bigint not null generated always as identity,
    user_id uuid not null references auth.users(id),
    entity regclass not null,
    filters cdc.user_defined_filter[] not null default '{}',
    created_at timestamp not null default timezone('utc', now()),

    constraint pk_subscription primary key (id),
    unique (user_id, entity, filters)
);

create function cdc.subscription_check_filters()
    returns trigger
    language plpgsql
as $$
/*
Validates that the user defined filters for a subscription:
- refer to valid columns that "authenticated" may access
- values are coercable to the correct column type
*/
declare
    col_names text[] = cdc.selectable_columns(new.entity);
    filter cdc.user_defined_filter;
    col_type text;
begin
    for filter in select * from unnest(new.filters) loop
        -- Filtered column is valid
        if not filter.column_name = any(col_names) then
            raise exception 'invalid column for filter %', filter.column_name;
        end if;

        -- Type is sanitized and safe for string interpolation
        col_type = (cdc.get_column_type(new.entity, filter.column_name))::text;
        if col_type is null then
            raise exception 'failed to lookup type for column %', filter.column_name;
        end if;
        -- raises an exception if value is not coercable to type
        perform format('select %s::%I', filter.value, col_type);
    end loop;

    -- Apply consistent order to filters so the unique constraint on
    -- (user_id, entity, filters) can't be tricked by a different filter order
    new.filters = coalesce(
        array_agg(f order by f.column_name, f.op, f.value),
        '{}'
    ) from unnest(new.filters) f;

    return new;
end;
$$;

create trigger tr_check_filters
    before insert or update on cdc.subscription
    for each row
    execute function cdc.subscription_check_filters();


grant all on cdc.subscription to postgres;
grant select on cdc.subscription to authenticated;


create or replace function  cdc.is_rls_enabled(entity regclass)
    returns boolean
    stable
    language sql
as $$
/*
Is Row Level Security enabled for the entity
*/
    select
        relrowsecurity
    from
        pg_class
    where
        oid = entity;
$$;



create or replace function cdc.cast_to_array_text(arr jsonb)
    returns text[]
    language 'sql'
    stable
as $$
/*
Cast an jsonb array of text to a native postgres array of text

Example:
    select cdc.cast_to_array_text('{"hello", "world"}'::jsonb)
*/
    select
        array_agg(xyz.v)
    from
        jsonb_array_elements_text(
            case
                when jsonb_typeof(arr) = 'array' then arr
                else '[]'::jsonb
            end
        ) xyz(v)
$$;

create or replace function cdc.cast_to_jsonb_array_text(arr text[])
    returns jsonb
    language 'sql'
    stable
as $$
/*
Cast an jsonb array of text to a native postgres array of text

Example:
    select cdc.cast_to_jsonb_array_text('{"hello", "world"}'::text[])
*/
    select
        coalesce(jsonb_agg(xyz.v), '{}'::jsonb)
    from
        unnest(arr) xyz(v);
$$;


create or replace function cdc.random_slug(n_chars int default 10)
    returns text
    language sql
    volatile
as $$
/*
Random string of *n_chars* length that is valid as a sql identifier without quoting
*/
  select string_agg(chr((ascii('a') + round(random() * 25))::int), '') from generate_series(1, n_chars)
$$;


create or replace function cdc.check_equality_op(
    op cdc.equality_op,
    type_ regtype,
    val_1 text,
    val_2 text
)
    returns bool
    immutable
    language plpgsql
as $$
/*
Casts *val_1* and *val_2* as type *type_* and check the *op* condition for truthiness
*/
declare
    op_symbol text = (
        case
            when op = 'eq' then '='
            when op = 'neq' then '!='
            when op = 'lt' then '<'
            when op = 'lte' then '<='
            when op = 'gt' then '>'
            when op = 'gte' then '>='
            else 'UNKNOWN OP'
        end
    );
    res boolean;
begin
    execute format('select %L::'|| type_::text || ' ' || op_symbol || ' %L::'|| type_::text, val_1, val_2) into res;
    return res;
end;
$$;


create type cdc.kind as enum('insert', 'update', 'delete');

create type cdc.wal_column as (
    name text,
    type text,
    value text,
    is_pkey boolean
);

create or replace function cdc.build_prepared_statement_sql(
    prepared_statement_name text,
    entity regclass,
    columns cdc.wal_column[]
)
    returns text
    language sql
as $$
/*
Builds a sql string that, if executed, creates a prepared statement to
tests retrive a row from *entity* by its primary key columns.

Example
    select cdc.build_prepared_statment_sql('public.notes', '{"id"}'::text[], '{"bigint"}'::text[])
*/
    select
'prepare ' || prepared_statement_name || ' as
select
    count(*) > 0
from
    ' || entity || '
where
    ' || string_agg(quote_ident(pkc.name) || '=' || quote_nullable(pkc.value) , ' and ') || ';'
    from
        unnest(columns) pkc
    where
        pkc.is_pkey
    group by
        entity
$$;


create type cdc.wal_rls as (
    wal jsonb,
    is_rls_enabled boolean,
    users uuid[],
    errors text[]
);



create or replace function cdc.is_visible_through_filters(columns cdc.wal_column[], filters cdc.user_defined_filter[])
    returns bool
    language sql
    immutable
as $$
/*
Should the record be visible (true) or filtered out (false) after *filters* are applied
*/
    select
        -- Default to allowed when no filters present
        coalesce(
            sum(
                cdc.check_equality_op(
                    op:=f.op,
                    type_:=col.type::regtype,
                    val_1:=col.value,
                    val_2:=f.value
                )::int
            ) = count(1),
            true
        )
    from
        unnest(filters) f
        join unnest(columns) col
            on f.column_name = col.name;
$$;


create or replace function cdc.apply_rls(wal jsonb)
    returns cdc.wal_rls
    language plpgsql
    volatile
as $$
/*
Append keys describing user visibility to each change

"security": {
    "visible_to": ["31b93c49-5435-42bf-97c4-375f207824d4"],
    "is_rls_enabled": true,
}

Example *change*:
{
    "change": [
        {
            "pk": [
                {
                    "name": "id",
                    "type": "bigint"
                }
            ],
            "table": "notes",
            "action": "I",
            "schema": "public",
            "columns": [
                {
                    "name": "id",
                    "type": "bigint",
                    "value": 28
                },
                {
                    "name": "user_id",
                    "type": "uuid",
                    "value": "31b93c49-5435-42bf-97c4-375f207824d4"
                },
                {
                    "name": "body",
                    "type": "text",
                    "value": "take out the trash"
                }
            ],

        }
    ]
}
*/
declare
    -- Regclass of the table e.g. public.notes
    entity_ regclass = (
        quote_ident(wal ->> 'schema')
        || '.'
        || quote_ident(wal ->> 'table')
    )::regclass;

    -- I, U, D, T: insert, update ...
    action char = wal ->> 'action';

    -- Check if RLS is enabled for the table
    is_rls_enabled bool = cdc.is_rls_enabled(entity_);

    -- UUIDs of subscribed users who may view the change
    user_id uuid;
    user_has_access bool;
    visible_to_user_ids uuid[] = '{}';

    -- Which columns does the "authenticated" role have permission to select (view)
    selectable_columns text[] = cdc.selectable_columns(entity_);

    -- user subscriptions to the wal record's table
    subscriptions cdc.subscription[] =
            array_agg(sub)
        from
            cdc.subscription sub
        where
            sub.entity = entity_;

    -- structured info for wal's columns
    columns cdc.wal_column[] =
        array_agg(
            (
                x->>'name',
                x->>'type',
                x->>'value',
                (pks ->> 'name') is not null
            )::cdc.wal_column
        )
        from
            jsonb_array_elements(wal -> 'columns') x
            left join jsonb_array_elements(wal -> 'pk') pks
                on (x ->> 'name') = (pks ->> 'name');

    filters cdc.user_defined_filter[];
    allowed_by_filters boolean;
    errors text[] = '{}';
begin

    -- Truncates are considered public
    if action = 'T' then
        -- Example wal: {"table": "notes", "action": "T", "schema": "public"}
        return (
            wal,
            is_rls_enabled,
            -- visible to all subscribers
            (select array_agg(s.user_id) from unnest(subscriptions) s),
            -- errors is empty
            errors
        )::cdc.wal_rls;
    end if;

    -- Deletes are public but only expose primary key info
    if action = 'D' then
        -- Example wal input: {"action":"D","schema":"public","table":"notes","identity":[{"name":"id","type":"bigint","value":1}],"pk":[{"name":"id","type":"bigint"}]}
        -- Filters may have been applied to
        for user_id, filters in select subs.user_id, subs.filters from unnest(subscriptions) subs
        loop
            -- Check if filters exclude the record
            allowed_by_filters = cdc.is_visible_through_filters(columns, filters);
            if allowed_by_filters then
                visible_to_user_ids = visible_to_user_ids || user_id;
            end if;
        end loop;

        return (
            -- Remove 'pk'
            (wal #- '{pk}'),
            is_rls_enabled,
            visible_to_user_ids,
            errors
        )::cdc.wal_rls;
    end if;

    -- create a prepared statement to check the existence of the wal record by primray key
    if (select count(*) from pg_prepared_statements where name = 'walrus_rls_stmt') > 0 then
        deallocate walrus_rls_stmt;
    end if;
    execute cdc.build_prepared_statement_sql('walrus_rls_stmt', entity_, columns);

    -- Set role to "authenticated"
    perform set_config('role', 'authenticated', true);

    -- For each subscribed user
    for user_id, filters in select subs.user_id, subs.filters from unnest(subscriptions) subs
    loop
        -- Check if the user defined filters exclude the current record
        allowed_by_filters = cdc.is_visible_through_filters(columns, filters);

        -- If the user defined filters did not exclude the record
        if allowed_by_filters then

            -- Check if the user has access
            -- Deletes are public
            if is_rls_enabled and action <> 'D' then
                -- Impersonate the subscribed user
                perform set_config('request.jwt.claim.sub', user_id::text, true);
                -- Lookup record the record as subscribed user
                execute 'execute walrus_rls_stmt' into user_has_access;
            else
                user_has_access = true;
            end if;

            if user_has_access then
                visible_to_user_ids = visible_to_user_ids || user_id;
            end if;
        end if;

    end loop;

    -- If the "authenticated" role does not have permission to see all columns in the table
    if array_length(selectable_columns, 1) < array_length(columns, 1) then

        -- Filter the columns to only the ones that are visible to "authenticated"
        wal = wal || (
            select
                jsonb_build_object(
                    'columns',
                    jsonb_agg(col_doc)
                )
            from
                jsonb_array_elements(wal -> 'columns') r(col_doc)
            where
                (col_doc ->> 'name') = any(selectable_columns)
        );
    end if;

    perform (
        set_config('role', null, true)
    );

    -- return the change object without primary key info
    return (
        (wal #- '{pk}'),
        is_rls_enabled,
        visible_to_user_ids,
        errors
    )::cdc.wal_rls;
end;
$$;
