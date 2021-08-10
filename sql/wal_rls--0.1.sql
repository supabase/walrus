---------------------------------------
-- Realtime Row Level Security (RLS) --
---------------------------------------
/*
	The folowing is a proof of concept showing how row level security
	rules can be applied to a realtime replicaiton stream.
	
	There are sample outputs & tests throghout this document. Read it as
	a blog post with embedded code.
	
	Steps:
	 - Create a table named public.note
	 - Create a RLS policy such that each row is visible to exactly 1 user
	 - Create a new note owned by 1 of 10 users
	 - View the standard wal2json replication output for the note
	 - Pass the wal2json replication output through a security function
	 		to add an array of user_ids representing users who are subscribed
			to the table & pass the RLS policy to have visibility of the change
*/
-----------------------------------
-- Realtime RLS Helper Utilities --
-----------------------------------
/*
	Convenience functions to keep the central business logic clear
*/

-- WAL2JSON Settings
--UPDATE pg_settings SET setting = true WHERE name = 'include-xids';
/*
set include_timestamp TO true;
set include-origin to true;
set include-origin to true;
set write-in-chunks to true;
*/
create schema cdc;
-----------------------
-- Replication Setup --
-----------------------
/*
	- Create a table to track which users are subscribed to each table
	- Create a replication slot
	- Create some change to inspect
*/

create table cdc.subscription (
	-- Tracks which users are subscribed to each table
	id bigint generated always as identity,
	user_id uuid not null references auth.users(id),
	entity regclass not null,
	filters jsonb
);
grant all on cdc.subscription to postgres;

-- Subscribe all users to the public.note table
--insert into cdc.subscription(user_id, entity) select id, 'public.note' from auth.users;

-- Create slot
--select * from pg_create_logical_replication_slot('rls_poc', 'wal2json');

-- Create a new public.note to produce some logical replication data
--insert into public.note(user_id, body)
--select id, 'water the plants' from auth.users order by id limit 1;


------------
-- Review --
------------
/*
	Look at the standard wal2json contents for the record
	Note:
		"peek" lets us review the change without consuming it
		change "peek" to "get" for consume changes 
*/
--select lsn, xid, data from pg_logical_slot_peek_changes('rls_poc', NULL, NULL);
/*
{
    "change": [
        {
            "kind": "insert",
            "table": "note",
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
                2,
                "296d893b-3031-4008-99cb-dc01cce74586",
                "water the plants"
            ]
        }
    ]
}
*/

------------------------------------
-- Realtime RLS Security Function --
------------------------------------
/*
	To this point, we have a SQL queryable view that gives us CDC
	as JSONB, with no RLS.
	
	Our goal is to append another jsonb key named "visible_to" to each row in the
	change data that shows an array of user_id uuids that have access to view
	the change
*/



create or replace function  cdc.rls(dat jsonb)
returns jsonb
language plpgsql
as $$
/*
	Append a "visible_to" key containing an array of subscribed user_id uuids to each change
*/
declare
	-- table/view name
	entity_ regclass;
	pkey_cols text[];
	
	-- UUIDs of subscribed users who may view the change
	visible_to_user_ids uuid[];
	
	-- Internal state vars
	res_agg jsonb[] = '{}';
	change_row jsonb;
	query_has_access text;
	user_id uuid;
	user_has_access bool;
	users_have_access uuid[];
	prev_role text;
    search_path text = current_setting('search_path');
begin
    -- Without nulling out search path, casting a table prefixed with a schema that is
    -- contained in the search path will cause the schema to be omitted.
    -- e.g. 'public.post'::reglcass:text -> 'post' (vs 'public.post')
    perform set_config('search_path', '', true);

	-- For each change in the replication data
	for change_row in select * from jsonb_array_elements(dat -> 'change')
	loop
		entity_ = (
            quote_ident(change_row ->> 'schema')
            || '.'
            || quote_ident(change_row ->> 'table')
        )::regclass;

        -- raise exception 'entity %, %', entity_, current_setting('search_path');
		users_have_access = '{}';
		
		-- Zip column names and values for the primary key cols into a sql query to check if
		-- each subscribed user has access
		-- Sample output: 
		--		select count(*) > 0 from public.post where id='2'
		select
			(
				'select count(*) > 0 from '|| entity_::text 
				|| ' where ' || string_agg(quote_ident(col_name) || '=' || quote_literal(col_val), ' and ')
			)
		from
			jsonb_array_elements_text(change_row -> 'columnnames') with ordinality col_n(col_name, col_ix),
			lateral jsonb_array_elements_text(change_row -> 'columnvalues') with ordinality col_v(col_val, val_ix),
			lateral jsonb_array_elements_text(change_row -> 'pk' -> 'pknames') pkeys(pkey_col)
		where
			col_ix = val_ix
			and col_name::text = pkey_col
		group by
			entity_
		into
            query_has_access;
    
        --raise exception 'entity %, %', entity_, query_has_access;
			
		-- For each subscribed user
		for user_id in select sub.user_id from cdc.subscription sub where sub.entity = entity_
		loop
			-- Set authenticated as the user we're checking
			perform	(
                set_config('role', 'authenticated', true),
                set_config('request.jwt.claim.sub', user_id::text, true)
            );

			-- Execute that access query as the user
            -- TODO: handle exceptions (permissions) here
            execute query_has_access into user_has_access;
			
			if user_has_access then
				users_have_access = users_have_access || user_id;
			end if;
			
		end loop;
		
		perform (
            set_config('role', null, true),
            set_config('request.jwt.claim.sub', null, true)
        );
			
		-- Cast the array of subscribed users to a jsonb array and add it to the change_row
		change_row = change_row || (
            select jsonb_build_object(
                'visible_to',
                coalesce(jsonb_agg(v), '[]')
            ) from unnest(users_have_access) x(v)
        );
		
		-- Add the updated row to the result set
		res_agg = res_agg || change_row;
	end loop;

    -- Restore previous configuration
    perform set_config('search_path', search_path, true);
	
	return jsonb_build_object(
		'change',
		(select jsonb_agg(v) from unnest(res_agg) x(v))
	);
end;
$$;


------------
-- Output --
------------
/*
	Finally, we pass the wal2json replication stream through
	our function to check if the update is visible to each subscribed user
	and append that data to the update
select
	cdc.change_append_visible_to(data::jsonb)
from
	pg_logical_slot_peek_changes('rls_poc', NULL, NULL);
{
    "change": [
        {
            "kind": "insert",
            "table": "note",
            "schema": "public",
LOOK HERE   "visible_to": [
LOOK HERE      "296d893b-3031-4008-99cb-dc01cce74586"
LOOK HERE   ],	
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
