create schema if not exists realtime;


-- Temporary functions to assist with idempotency
create or replace function realtime.type_exists(schema_name text, type_name text)
    returns bool
    language plpgsql
as $$
begin
    begin
        perform format('%I.%I', schema_name, type_name)::regtype;
        return true;
    exception when others then
        return false;
    end;
end
$$;

create or replace function realtime.table_exists(schema_name text, table_name text)
    returns bool
    language plpgsql
as $$
begin
    begin
        perform format('%I.%I', schema_name, table_name)::regclass;
        return true;
    exception when others then
        return false;
    end;
end
$$;


-- realtime.equality_op
do $$
    begin
        if not realtime.type_exists('realtime', 'equality_op') then

            create type realtime.equality_op as enum(
                'eq', 'neq', 'lt', 'lte', 'gt', 'gte'
            );

        end if;
    end
$$;

-- realtime.user_defined_filter
do $$
    begin
        if not realtime.type_exists('realtime', 'user_defined_filter') then

            create type realtime.user_defined_filter as (
                column_name text,
                op realtime.equality_op,
                value text
            );

        end if;
    end
$$;


-- realtime.action
do $$
    begin
        if not realtime.type_exists('realtime', 'action') then

            create type realtime.action as enum ('INSERT', 'UPDATE', 'DELETE', 'ERROR');

        end if;
    end
$$;

create or replace function realtime.cast(val text, type_ regtype)
    returns jsonb
    immutable
    language plpgsql
as $$
declare
    res jsonb;
begin
    execute format('select to_jsonb(%L::'|| type_::text || ')', val)  into res;
    return res;
end
$$;



create or replace function realtime.to_regrole(role_name text)
    returns regrole
    immutable
    language sql
    -- required to allow use in generated clause
as $$ select role_name::regrole $$;


-- realtime.subscription
do $$
    begin
        if not realtime.table_exists('realtime', 'subscription') then

            create table realtime.subscription (
                -- Tracks which subscriptions are active
                id bigint generated always as identity primary key,
                subscription_id uuid not null,
                entity regclass not null,
                filters realtime.user_defined_filter[] not null default '{}',
                claims jsonb not null,
                claims_role regrole not null generated always as (realtime.to_regrole(claims ->> 'role')) stored,
                created_at timestamp not null default timezone('utc', now()),

                unique (subscription_id, entity, filters)
            );
            create index ix_realtime_subscription_entity on realtime.subscription using hash (entity);

        end if;
    end
$$;


create or replace function realtime.subscription_check_filters()
    returns trigger
    language plpgsql
as $$
/*
Validates that the user defined filters for a subscription:
- refer to valid columns that the claimed role may access
- values are coercable to the correct column type
*/
declare
    col_names text[] = coalesce(
            array_agg(c.column_name order by c.ordinal_position),
            '{}'::text[]
        )
        from
            information_schema.columns c
        where
            format('%I.%I', c.table_schema, c.table_name)::regclass = new.entity
            and pg_catalog.has_column_privilege(
                (new.claims ->> 'role'),
                format('%I.%I', c.table_schema, c.table_name)::regclass,
                c.column_name,
                'SELECT'
            );
    filter realtime.user_defined_filter;
    col_type regtype;
begin
    for filter in select * from unnest(new.filters) loop
        -- Filtered column is valid
        if not filter.column_name = any(col_names) then
            raise exception 'invalid column for filter %', filter.column_name;
        end if;

        -- Type is sanitized and safe for string interpolation
        col_type = (
            select atttypid::regtype
            from pg_catalog.pg_attribute
            where attrelid = new.entity
                  and attname = filter.column_name
        );
        if col_type is null then
            raise exception 'failed to lookup type for column %', filter.column_name;
        end if;
        -- raises an exception if value is not coercable to type
        perform realtime.cast(filter.value, col_type);
    end loop;

    -- Apply consistent order to filters so the unique constraint on
    -- (subscription_id, entity, filters) can't be tricked by a different filter order
    new.filters = coalesce(
        array_agg(f order by f.column_name, f.op, f.value),
        '{}'
    ) from unnest(new.filters) f;

    return new;
end;
$$;

-- tr_check_filters on realtime.subscription
do $$
    begin
        if not exists(
            select 1
            from pg_trigger
            where
                tgrelid = 'realtime.subscription'::regclass::oid
                and tgname = 'tr_check_filters'
        ) then

            create trigger tr_check_filters
                before insert or update on realtime.subscription
                for each row
                execute function realtime.subscription_check_filters();

        end if;
    end
$$;

create or replace function realtime.quote_wal2json(entity regclass)
    returns text
    language sql
    immutable
    strict
as $$
    select
        (
            select string_agg('\' || ch,'')
            from unnest(string_to_array(nsp.nspname::text, null)) with ordinality x(ch, idx)
            where
                not (x.idx = 1 and x.ch = '"')
                and not (
                    x.idx = array_length(string_to_array(nsp.nspname::text, null), 1)
                    and x.ch = '"'
                )
        )
        || '.'
        || (
            select string_agg('\' || ch,'')
            from unnest(string_to_array(pc.relname::text, null)) with ordinality x(ch, idx)
            where
                not (x.idx = 1 and x.ch = '"')
                and not (
                    x.idx = array_length(string_to_array(nsp.nspname::text, null), 1)
                    and x.ch = '"'
                )
        )
    from
        pg_class pc
        join pg_namespace nsp
            on pc.relnamespace = nsp.oid
    where
        pc.oid = entity
$$;


create or replace function realtime.check_equality_op(
    op realtime.equality_op,
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

drop type if exists realtime.wal_column cascade;

-- realtime.wal_column
do $$
    begin
        if not realtime.type_exists('realtime', 'wal_column') then

            create type realtime.wal_column as (
                name text,
                type_name text,
                type_oid oid,
                value jsonb,
                is_pkey boolean,
                is_selectable boolean
            );

        end if;
    end
$$;


create or replace function realtime.build_prepared_statement_sql(
    prepared_statement_name text,
    entity regclass,
    columns realtime.wal_column[]
)
    returns text
    language sql
as $$
/*
Builds a sql string that, if executed, creates a prepared statement to
tests retrive a row from *entity* by its primary key columns.

Example
    select realtime.build_prepared_statment_sql('public.notes', '{"id"}'::text[], '{"bigint"}'::text[])
*/
    select
'prepare ' || prepared_statement_name || ' as
    select
        exists(
            select
                1
            from
                ' || entity || '
            where
                ' || string_agg(quote_ident(pkc.name) || '=' || quote_nullable(pkc.value #>> '{}') , ' and ') || '
        )'
    from
        unnest(columns) pkc
    where
        pkc.is_pkey
    group by
        entity
$$;

-- realtime.wal_rls
do $$
    begin
        if not realtime.type_exists('realtime', 'wal_rls') then

            create type realtime.wal_rls as (
                wal jsonb,
                is_rls_enabled boolean,
                subscription_ids uuid[],
                errors text[]
            );

        end if;
    end
$$;



create or replace function realtime.is_visible_through_filters(columns realtime.wal_column[], filters realtime.user_defined_filter[])
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
                realtime.check_equality_op(
                    op:=f.op,
                    type_:=col.type_oid::regtype,
                    -- cast jsonb to text
                    val_1:=col.value #>> '{}',
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


create or replace function realtime.apply_rls(wal jsonb, max_record_bytes int = 1024 * 1024)
    returns setof realtime.wal_rls
    language plpgsql
    volatile
as $$
declare
    -- Regclass of the table e.g. public.notes
    entity_ regclass = (quote_ident(wal ->> 'schema') || '.' || quote_ident(wal ->> 'table'))::regclass;

    -- I, U, D, T: insert, update ...
    action realtime.action = (
        case wal ->> 'action'
            when 'I' then 'INSERT'
            when 'U' then 'UPDATE'
            when 'D' then 'DELETE'
            else 'ERROR'
        end
    );

    -- Is row level security enabled for the table
    is_rls_enabled bool = relrowsecurity from pg_class where oid = entity_;

    subscriptions realtime.subscription[] = array_agg(subs)
        from
            realtime.subscription subs
        where
            subs.entity = entity_;

    -- Subscription vars
    roles regrole[] = array_agg(distinct us.claims_role)
        from
            unnest(subscriptions) us;

    working_role regrole;
    claimed_role regrole;
    claims jsonb;

    subscription_id uuid;
    subscription_has_access bool;
    visible_to_subscription_ids uuid[] = '{}';

    -- structured info for wal's columns
    columns realtime.wal_column[];
    -- previous identity values for update/delete
    old_columns realtime.wal_column[];

    error_record_exceeds_max_size boolean = octet_length(wal::text) > max_record_bytes;

    -- Primary jsonb output for record
    output jsonb;

begin
    perform set_config('role', null, true);

    columns =
        array_agg(
            (
                x->>'name',
                x->>'type',
                x->>'typeoid',
                realtime.cast(
                    (x->'value') #>> '{}',
                    (x->>'typeoid')::regtype
                ),
                (pks ->> 'name') is not null,
                true
            )::realtime.wal_column
        )
        from
            jsonb_array_elements(wal -> 'columns') x
            left join jsonb_array_elements(wal -> 'pk') pks
                on (x ->> 'name') = (pks ->> 'name');

    old_columns =
        array_agg(
            (
                x->>'name',
                x->>'type',
                x->>'typeoid',
                realtime.cast(
                    (x->'value') #>> '{}',
                    (x->>'typeoid')::regtype
                ),
                (pks ->> 'name') is not null,
                true
            )::realtime.wal_column
        )
        from
            jsonb_array_elements(wal -> 'identity') x
            left join jsonb_array_elements(wal -> 'pk') pks
                on (x ->> 'name') = (pks ->> 'name');

    for working_role in select * from unnest(roles) loop

        -- Update `is_selectable` for columns and old_columns
        columns =
            array_agg(
                (
                    c.name,
                    c.type_name,
                    c.type_oid,
                    c.value,
                    c.is_pkey,
                    pg_catalog.has_column_privilege(working_role, entity_, c.name, 'SELECT')
                )::realtime.wal_column
            )
            from
                unnest(columns) c;

        old_columns =
                array_agg(
                    (
                        c.name,
                        c.type_name,
                        c.type_oid,
                        c.value,
                        c.is_pkey,
                        pg_catalog.has_column_privilege(working_role, entity_, c.name, 'SELECT')
                    )::realtime.wal_column
                )
                from
                    unnest(old_columns) c;

        if action <> 'DELETE' and count(1) = 0 from unnest(columns) c where c.is_pkey then
            return next (
                jsonb_build_object(
                    'schema', wal ->> 'schema',
                    'table', wal ->> 'table',
                    'type', action
                ),
                is_rls_enabled,
                -- subscriptions is already filtered by entity
                (select array_agg(s.subscription_id) from unnest(subscriptions) as s where claims_role = working_role),
                array['Error 400: Bad Request, no primary key']
            )::realtime.wal_rls;

        -- The claims role does not have SELECT permission to the primary key of entity
        elsif action <> 'DELETE' and sum(c.is_selectable::int) <> count(1) from unnest(columns) c where c.is_pkey then
            return next (
                jsonb_build_object(
                    'schema', wal ->> 'schema',
                    'table', wal ->> 'table',
                    'type', action
                ),
                is_rls_enabled,
                (select array_agg(s.subscription_id) from unnest(subscriptions) as s where claims_role = working_role),
                array['Error 401: Unauthorized']
            )::realtime.wal_rls;

        else
            output = jsonb_build_object(
                'schema', wal ->> 'schema',
                'table', wal ->> 'table',
                'type', action,
                'commit_timestamp', to_char(
                    (wal ->> 'timestamp')::timestamptz,
                    'YYYY-MM-DD"T"HH24:MI:SS"Z"'
                ),
                'columns', (
                    select
                        jsonb_agg(
                            jsonb_build_object(
                                'name', pa.attname,
                                'type', pt.typname
                            )
                            order by pa.attnum asc
                        )
                    from
                        pg_attribute pa
                        join pg_type pt
                            on pa.atttypid = pt.oid
                    where
                        attrelid = entity_
                        and attnum > 0
                        and pg_catalog.has_column_privilege(working_role, entity_, pa.attname, 'SELECT')
                )
            )
            -- Add "record" key for insert and update
            || case
                when error_record_exceeds_max_size then jsonb_build_object('record', '{}'::jsonb)
                when action in ('INSERT', 'UPDATE') then
                    jsonb_build_object(
                        'record',
                        (select jsonb_object_agg((c).name, (c).value) from unnest(columns) c where (c).is_selectable)
                    )
                else '{}'::jsonb
            end
            -- Add "old_record" key for update and delete
            || case
                when error_record_exceeds_max_size then jsonb_build_object('old_record', '{}'::jsonb)
                when action in ('UPDATE', 'DELETE') then
                    jsonb_build_object(
                        'old_record',
                        (select jsonb_object_agg((c).name, (c).value) from unnest(old_columns) c where (c).is_selectable)
                    )
                else '{}'::jsonb
            end;

            -- Create the prepared statement
            if is_rls_enabled and action <> 'DELETE' then
                if (select 1 from pg_prepared_statements where name = 'walrus_rls_stmt' limit 1) > 0 then
                    deallocate walrus_rls_stmt;
                end if;
                execute realtime.build_prepared_statement_sql('walrus_rls_stmt', entity_, columns);
            end if;

            visible_to_subscription_ids = '{}';

            for subscription_id, claims in (
                    select
                        subs.subscription_id,
                        subs.claims
                    from
                        unnest(subscriptions) subs
                    where
                        subs.entity = entity_
                        and subs.claims_role = working_role
                        and realtime.is_visible_through_filters(columns, subs.filters)
            ) loop

                if not is_rls_enabled or action = 'DELETE' then
                    visible_to_subscription_ids = visible_to_subscription_ids || subscription_id;
                else
                    -- Check if RLS allows the role to see the record
                    perform
                        set_config('role', working_role::text, true),
                        set_config('request.jwt.claims', claims::text, true);

                    execute 'execute walrus_rls_stmt' into subscription_has_access;

                    if subscription_has_access then
                        visible_to_subscription_ids = visible_to_subscription_ids || subscription_id;
                    end if;
                end if;
            end loop;

            perform set_config('role', null, true);

            return next (
                output,
                is_rls_enabled,
                visible_to_subscription_ids,
                case
                    when error_record_exceeds_max_size then array['Error 413: Payload Too Large']
                    else '{}'
                end
            )::realtime.wal_rls;

        end if;
    end loop;

    perform set_config('role', null, true);
end;
$$;


drop function realtime.type_exists;
drop function realtime.table_exists;
