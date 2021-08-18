/*
    WAL_RLS:
        Write Ahead Log Row Level Security
*/



create schema cdc;
grant usage on schema cdc to postgres;
grant usage on schema cdc to authenticated;

create table cdc.subscription (
	-- Tracks which users are subscribed to each table
	id bigint not null generated always as identity,
	user_id uuid not null references auth.users(id),
	entity regclass not null,
    -- Format. Equality only {"col_1": "1", "col_2": 4 }
	filters jsonb,
    constraint pk_subscription primary key (id),
    created_at timestamp not null default timezone('utc', now())
);
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


create or replace function cdc.impersonate(user_id uuid)
    returns void
    volatile
    language sql
as $$
/*
Updates the current transaction's config so queries can by made as a user
authenticated as *user_id* 
*/
    select
        set_config('request.jwt.claim.sub', user_id::text, true),
        set_config('role', 'authenticated', true)
$$;


create or replace function cdc.build_prepared_statement_sql(
    prepared_statement_name text,
	entity regclass,
	-- primary key column names
	-- this could be looked up internaly
	pkey_cols text[],
	pkey_types text[]
)
    returns text
    language sql
as $$
/*
Builds a sql string that, if executed, creates a prepared statement to impersonatea user
and tests if that user has access to a data row described by *entity* and an array of
it'd primray key values.

Example
    select cdc.build_prepared_statment_sql('public.notes', '{"id"}'::text[], '{"bigint"}'::text[])
*/
	select
'prepare ' || prepared_statement_name ||'(uuid, ' || string_agg('text', ', ') || ') as
with imp as (
	select cdc.impersonate($1)
)
select
	count(*) > 0
from
	' || entity || '
where
	' || string_agg(quote_ident(col) || '=$' || (1+col_ix)::text || '::' || type_ , ' and ') || ';'
	from
		unnest(pkey_cols) with ordinality pkc(col, col_ix),
		lateral unnest(pkey_types) with ordinality pkt(type_, type_ix)
	where
		col_ix = type_ix
	group by
		entity
$$;



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
        jsonb_agg(xyz.v)
    from
        unnest(arr) xyz(v);
$$;



create type cdc.kind as enum('insert', 'update', 'delete');


create or replace function cdc.rls(change jsonb)
    returns jsonb
    language plpgsql
    volatile
as $$
/*
Append keys describing user visibility to each change

"security": {
    "visible_to": [],
    "is_rls_enabled": true,
    "visible_columns": [
        "id",
        "user_id",
        "body"
    ]
}

Example *change:
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
	table_name text;
	schema_name text;
	entity_ regclass;
    action char;
    is_rls_enabled bool;
	
	-- UUIDs of subscribed users who may view the change
	visible_to_user_ids text[];
	
	-- Internal state vars
	res_agg jsonb[] = '{}';
	query_has_access text;
	user_id uuid;
	user_has_access bool;

    prev_role text = current_setting('role');
    prev_search_path text = current_setting('search_path');

    selectable_columns text[];

    pkey_cols text[];
    pkey_types text[];
    pkey_vals text[];

    prep_stmt_sql text;
    prep_stmt_executor_sql text;
    prep_stmt_executor_sql_template text;
    prep_stmt_params text[];
    -- might make this dynamic
    prep_stmt_name text = 'xyz';

begin
    -- Without nulling out search path, casting a table prefixed with a schema that is
    -- contained in the search path will cause the schema to be omitted.
    -- e.g. 'public.post'::reglcass:text -> 'post' (vs 'public.post')
    perform (
        set_config('search_path', '', true)
    );

    -- Filter out nonsense "begin" and "commit" records
    -- TODO: find a way to remove them compeltely
    if (change ->> 'action')::char = any('{"B", "C"}'::char[]) then
        return null;
    end if;


    -- Regclass of the table e.g. public.notes
    schema_name = (change ->> 'schema');
    table_name = (change ->> 'table');
    entity_ = (quote_ident(schema_name)|| '.' || quote_ident(table_name))::regclass;

    -- Array tracking which user_ids have been approved to view the change
    visible_to_user_ids = '{}';

    -- Check if RLS is enabled for the table
    is_rls_enabled = cdc.is_rls_enabled(entity_);

    -- Which columns does the "authenticated" role have permission to select (view)
    selectable_columns = cdc.selectable_columns(entity_);

    -- If RLS is enabled for the table, check each subscribed user to see if they should see the change
    if is_rls_enabled then

        select
            array_agg(pks.pk_info ->> 'name' order by pk_ix) pk_names,
            array_agg(pks.pk_info ->> 'type' order by pk_ix) pk_types,
            array_agg(cols.col_info ->> 'value' order by pk_ix) pk_vals
        from
            jsonb_array_elements(change -> 'pk') with ordinality pks(pk_info, pk_ix),
            lateral jsonb_array_elements(change -> 'columns') cols(col_info)
        where
            (col_info ->> 'name') = (pks.pk_info ->> 'name')
        into
            pkey_cols, pkey_types, pkey_vals;

        -- Setup a prepared statement for this record
        prep_stmt_name = lower(schema_name) || '_' || lower(table_name) || '_wal_rls';
        -- Collect sql string for prepared statment
        prep_stmt_sql = cdc.build_prepared_statement_sql(prep_stmt_name, entity_, pkey_cols, pkey_types);
        -- Create the prepared statement
        execute prep_stmt_sql;

        -- For each subscribed user
        for user_id in select sub.user_id from cdc.subscription sub where sub.entity = entity_
        loop
            -- TODO: handle exceptions (permissions) here
            prep_stmt_executor_sql_template = 'execute %I(''%s'', ' || string_agg('''%s''', ', ') || ')' from generate_series(1,array_length(pkey_vals, 1) );
            -- Assemble all arguments into an array to pass into the template
            prep_stmt_params = '{}'::text[] || prep_stmt_name || user_id::text || pkey_vals;
            execute format(prep_stmt_executor_sql_template, variadic prep_stmt_params) into user_has_access;

            if user_has_access then
                visible_to_user_ids = visible_to_user_ids || user_id::text;
            end if;
            
        end loop;

        -- Delete the prepared statemetn
        execute format('deallocate %I', prep_stmt_name);

    end if;
    
        
    -- Cast the array of subscribed users to a jsonb array and add it to the change
    change = change || (
        select
            jsonb_build_object(
                'security',
                jsonb_build_object(
                    'is_rls_enabled',
                    is_rls_enabled,
                    'visible_to',
                    coalesce(cdc.cast_to_jsonb_array_text(visible_to_user_ids), '[]'),
                    'visible_columns',
                    cdc.cast_to_jsonb_array_text(selectable_columns)
                )
            )
    );
    
    -- Restore previous configuration
    perform (
        set_config('request.jwt.claim.sub', null, true),
        set_config('role', prev_role, true),
        set_config('search_path', prev_search_path, true)
    );
	
	return change;
end;
$$;
