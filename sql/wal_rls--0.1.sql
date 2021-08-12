create schema cdc;

create table cdc.subscription (
	-- Tracks which users are subscribed to each table
	id bigint generated always as identity,
	user_id uuid not null references auth.users(id),
	entity regclass not null,
	filters jsonb
);
grant all on cdc.subscription to postgres;


create or replace function  cdc.rls(dat jsonb)
returns jsonb
language plpgsql
as $$
/*
	Append a "visible_to" key containing an array of subscribed user_id uuids to each change

*

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
	
	-- UUIDs of subscribed users who may view the change
	visible_to_user_ids uuid[];
	
	-- Internal state vars
	res_agg jsonb[] = '{}';
	query_has_access text;
	user_id uuid;
	user_has_access bool;

    prev_role text = current_setting('role');
    prev_search_path text = current_setting('search_path');

begin
    -- Without nulling out search path, casting a table prefixed with a schema that is
    -- contained in the search path will cause the schema to be omitted.
    -- e.g. 'public.post'::reglcass:text -> 'post' (vs 'public.post')
    perform set_config('search_path', '', true);

	-- For each change in the replication data
	for change in select * from jsonb_array_elements(dat -> 'change')
	loop

        /*
        Example *change*:
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
        */

        -- Regclass of the table e.g. public.notes
		entity_ = (quote_ident(change ->> 'schema')|| '.'|| quote_ident(change ->> 'table'))::regclass;

        -- Array tracking which user_ids have been approved to view the change
		visible_to_user_ids = '{}';
		
		/* 
        Create a SQL string, embedding the fixed components like table and column names
        that can not be part of a prepared statement

        Example *query_has_access*
		    select count(1) > 0 from public.post where id='2'
        */
		select
			(
				'select count(*) > 0 from '|| entity_::text 
				|| ' where ' || string_agg(quote_ident(col_name) || '=' || quote_literal(col_val), ' and ')
			)
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
            execute format(
                $c$
                    with impersonate as (
                        select
                            set_config('request.jwt.claim.sub', '%s', true),
                            set_config('role', 'authenticated', true)
                    )
                $c$,
                user_id
            ) || query_has_access
            into user_has_access;

			if user_has_access then
				visible_to_user_ids = visible_to_user_ids || user_id;
			end if;
			
		end loop;
		
		
			
		-- Cast the array of subscribed users to a jsonb array and add it to the change
		change = change || (
            select
                jsonb_build_object(
                    'visible_to',
                    coalesce(jsonb_agg(v), '[]')
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
