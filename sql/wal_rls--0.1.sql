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
	filters jsonb,
    constraint pk_subscription primary key (id)
);
--alter table cdc.subscription replica identity default
grant all on cdc.subscription to postgres;
grant select on cdc.subscription to authenticated;


create or replace function  cdc.is_rls_enabled(entity regclass)
returns boolean
stable
language sql
as $$
    select
        relrowsecurity
    from
        pg_class
    where
        oid = entity;
$$;


create or replace function  cdc.is_rls_enabled(entity regclass)
returns boolean
stable
language sql
as $$
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
/*
Example
    select cdc.build_prepared_statment_sql('public.notes', '{"id"}'::text[], '{"bigint"}'::text[])
*/
as $$
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


create type cdc.kind as enum('insert', 'update', 'delete');


create or replace function  cdc.rls(dat jsonb)
returns jsonb
language plpgsql
volatile -- required for prepared statements
as $$
/*
	Append keys describing user visibility to each change

    "security": {
        "is_rls_enabled": true,
        "visible_to": ["296d893b-3031-4008-99cb-dc01cce74586"]
    }

    Example *dat*:
    {
        "change": [
            {
                "kind": "insert",
                "table": "notes",
                "schema": "public",
                "columnnames": [
                    "id",
                    "user_id",
                    "body"
                ],
                "columntypes": [
                    "bigint",
                    "uuid",
                    "text"
                ],
                "columnvalues": [
                    3,
                    "296d893b-3031-4008-99cb-dc01cce74586",
                    "water the plants"
                ]
            }
        ]
    }
*/
declare
	table_name text;
	schema_name text;
	entity_ regclass;
    change jsonb;
    kind cdc.kind;
    is_rls_enabled bool;
	
	-- UUIDs of subscribed users who may view the change
	visible_to_user_ids uuid[];
	
	-- Internal state vars
	res_agg jsonb[] = '{}';
	query_has_access text;
	user_id uuid;
	user_has_access bool;

    prev_role text = current_setting('role');
    prev_search_path text = current_setting('search_path');

    plan_exists bool;

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

	-- For each change in the replication data
	for change in select * from jsonb_array_elements(dat -> 'change')
	loop

        -- Regclass of the table e.g. public.notes
        schema_name = (change ->> 'schema');
        table_name = (change ->> 'table');
		entity_ = (quote_ident(schema_name)|| '.' || quote_ident(table_name))::regclass;

        -- insert, update, delete
        -- TODO: alter behavior on deletes
        kind = (change ->> 'kind')::cdc.kind;

        -- Array tracking which user_ids have been approved to view the change
		visible_to_user_ids = '{}';

        -- Check if RLS is enabled for the table
        is_rls_enabled = cdc.is_rls_enabled(entity_);

        -- If RLS is enabled for the table, check each subscribed user to see if they should see the change
        if is_rls_enabled then


            select array_agg(col_name) from jsonb_array_elements_text(change -> 'pk' -> 'pknames') x(col_name) into pkey_cols;
            select array_agg(col_name) from jsonb_array_elements_text(change -> 'pk' -> 'pktypes') x(col_name) into pkey_types;
            select
                array_agg(col_val)
            from
                jsonb_array_elements_text(change -> 'columnnames') with ordinality col_n(col_name, col_ix),
                lateral jsonb_array_elements_text(change -> 'columnvalues') with ordinality col_v(col_val, val_ix),
                lateral jsonb_array_elements_text(change -> 'pk' -> 'pknames') pkeys(pkey_col)
            where
                col_ix = val_ix
                and col_name::text = pkey_col
            group by
                entity_
            into pkey_vals;

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
                    visible_to_user_ids = visible_to_user_ids || user_id;
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
                        coalesce(jsonb_agg(v), '[]')
                    )
                )
            from
                unnest(visible_to_user_ids) x(v)
        );
		
		-- Add the updated row to the result set
		res_agg = res_agg || change;
	end loop;

    -- Restore previous configuration
    perform (
        set_config('request.jwt.claim.sub', null, true),
        set_config('role', prev_role, true),
        set_config('search_path', prev_search_path, true)
    );
	
	return jsonb_build_object(
		'change',
		(select jsonb_agg(v) from unnest(res_agg) x(v))
	);
end;
$$;
