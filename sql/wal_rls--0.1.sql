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
	entity_ regclass;
    change jsonb;

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
		entity_ = (quote_ident(change ->> 'schema')|| '.'|| quote_ident(change ->> 'table'))::regclass;

        -- Array tracking which user_ids have been approved to view the change
		visible_to_user_ids = '{}';

        -- Check if RLS is enabled for the table
        is_rls_enabled = cdc.is_rls_enabled(entity_);

        -- If RLS is enabled for the table, check each subscribed user to see if they should see the change
        if is_rls_enabled then
		
            /* 
            Create a SQL string, embedding the fixed components like table and column names
            that can not be part of a prepared statement

            Example *query_has_access*
                select count(1) > 0 from public.post where id='2'
            */
            select
                '
                with imp as (
                    select cdc.impersonate(%L)
                )
                select
                    count(*) > 0
                from
                    ' || entity_ || '
                where
                    ' || string_agg(quote_ident(col_name) || '=' || quote_literal(col_val), ' and ')
            from
                jsonb_array_elements_text(change -> 'columnnames') with ordinality col_n(col_name, col_ix),
                lateral jsonb_array_elements_text(change -> 'columnvalues') with ordinality col_v(col_val, val_ix),
                lateral jsonb_array_elements_text(change -> 'pk' -> 'pknames') pkeys(pkey_col)
            where
                col_ix = val_ix
                and col_name::text = pkey_col
            group by
                entity_
            into
                query_has_access;
        
            -- For each subscribed user
            for user_id in select sub.user_id from cdc.subscription sub where sub.entity = entity_
            loop
                -- TODO: handle exceptions (permissions) here
                -- Set authenticated as the user we're checking
                execute format(query_has_access, user_id, entity_) into user_has_access;

                if user_has_access then
                    visible_to_user_ids = visible_to_user_ids || user_id;
                end if;
                
            end loop;
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
