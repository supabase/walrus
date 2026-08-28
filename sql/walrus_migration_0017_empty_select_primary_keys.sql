-- Allow an empty selected_columns array to mean "primary keys only" instead of
-- raising an error. apply_rls already emits just the primary key columns for an
-- empty array (a column matches when it is in selected_columns OR it is a pkey),
-- so the only work here is to stop rejecting '{}' and to stop the normalization
-- step from collapsing '{}' back to NULL (which would mean "all columns").
create or replace function realtime.subscription_check_filters()
    returns trigger
    language plpgsql
as $$
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
    filter_v2 realtime.user_defined_filter_v2;
    col_type regtype;
    in_val jsonb;
    selected_col text;
begin
    -- Legacy 3-field filters: only the original operators are evaluable by the
    -- legacy check_equality_op path. Reject the new operators here so they
    -- cannot be stored on this column and later crash apply_rls with UNKNOWN OP.
    for filter in select * from unnest(new.filters) loop
        if not filter.column_name = any(col_names) then
            raise exception 'invalid column for filter %', filter.column_name;
        end if;

        col_type = (
            select atttypid::regtype
            from pg_catalog.pg_attribute
            where attrelid = new.entity
                  and attname = filter.column_name
        );
        if col_type is null then
            raise exception 'failed to lookup type for column %', filter.column_name;
        end if;

        if filter.op in (
            'like'::realtime.equality_op, 'ilike'::realtime.equality_op,
            'is'::realtime.equality_op, 'match'::realtime.equality_op,
            'imatch'::realtime.equality_op, 'isdistinct'::realtime.equality_op
        ) then
            raise exception 'operator % is only supported on the filters_v2 column', filter.op::text;
        end if;

        if filter.op = 'in'::realtime.equality_op then
            in_val = realtime.cast(filter.value, (col_type::text || '[]')::regtype);
            if coalesce(jsonb_array_length(in_val), 0) > 100 then
                raise exception 'too many values for `in` filter. Maximum 100';
            end if;
        else
            -- raises an exception if value is not coercable to type
            perform realtime.cast(filter.value, col_type);
        end if;
    end loop;

    -- v2 filters: full validation including the new operators.
    for filter_v2 in select * from unnest(new.filters_v2) loop
        if not filter_v2.column_name = any(col_names) then
            raise exception 'invalid column for filter %', filter_v2.column_name;
        end if;

        col_type = (
            select atttypid::regtype
            from pg_catalog.pg_attribute
            where attrelid = new.entity
                  and attname = filter_v2.column_name
        );
        if col_type is null then
            raise exception 'failed to lookup type for column %', filter_v2.column_name;
        end if;

        if filter_v2.op = 'in'::realtime.equality_op then
            in_val = realtime.cast(filter_v2.value, (col_type::text || '[]')::regtype);
            if coalesce(jsonb_array_length(in_val), 0) > 100 then
                raise exception 'too many values for `in` filter. Maximum 100';
            end if;
        elsif filter_v2.op = 'is'::realtime.equality_op then
            -- `is` requires a keyword RHS rather than a typed literal
            if filter_v2.value not in ('null', 'true', 'false', 'unknown') then
                raise exception 'invalid value for is filter: must be null, true, false, or unknown';
            end if;
            -- IS NULL works for any type, but IS TRUE/FALSE/UNKNOWN require a
            -- boolean operand. Reject the non-null keywords on non-boolean
            -- columns here so they don't abort apply_rls at WAL time.
            if filter_v2.value <> 'null' and col_type <> 'boolean'::regtype then
                raise exception 'is % filter requires a boolean column, got %', filter_v2.value, col_type::text;
            end if;
        elsif filter_v2.op in ('like'::realtime.equality_op, 'ilike'::realtime.equality_op) then
            -- like/ilike apply the text pattern operator (~~); reject column
            -- types that have no such operator instead of failing at WAL time
            if not exists (
                select 1 from pg_catalog.pg_operator
                where oprname = '~~' and oprleft = col_type
            ) then
                raise exception 'operator % requires a text-compatible column type, got %', filter_v2.op::text, col_type::text;
            end if;
        elsif filter_v2.op in ('match'::realtime.equality_op, 'imatch'::realtime.equality_op) then
            -- match/imatch apply the regex operators ~ / ~*; reject column types
            -- that have no such operator (e.g. integer) instead of failing at WAL
            -- time, mirroring the like/ilike guard above.
            if not exists (
                select 1 from pg_catalog.pg_operator
                where oprname = case when filter_v2.op = 'imatch'::realtime.equality_op then '~*' else '~' end
                  and oprleft = col_type
                  and oprright = col_type
                  and oprresult = 'boolean'::regtype
            ) then
                raise exception 'operator % requires a text-compatible column type, got %', filter_v2.op::text, col_type::text;
            end if;
            -- validate the regex eagerly so a bad pattern is rejected here, not
            -- inside apply_rls where it would abort the WAL stream for the entity
            begin
                perform '' ~ filter_v2.value;
            exception when others then
                raise exception 'invalid regular expression for % filter: %', filter_v2.op::text, sqlerrm;
            end;
        else
            -- eq/neq/lt/lte/gt/gte/isdistinct: value must be coercable to the type
            perform realtime.cast(filter_v2.value, col_type);
        end if;
    end loop;

    -- Reject arrays with NULL elements which bypass column validation
    if new.selected_columns is not null and array_position(new.selected_columns, null::text) is not null then
        raise exception 'selected_columns cannot contain null values.';
    end if;

    -- Validate that selected_columns reference columns the role can SELECT
    if new.selected_columns is not null then
        for selected_col in select * from unnest(new.selected_columns) loop
            if not selected_col = any(col_names) then
                raise exception 'invalid column for select %', selected_col;
            end if;
        end loop;
    end if;

    -- Apply consistent order to filters so the unique constraint can't be
    -- tricked by a different filter order. negate is part of the v2 sort key.
    new.filters = coalesce(
        array_agg(f order by f.column_name, f.op, f.value),
        '{}'
    ) from unnest(new.filters) f;

    new.filters_v2 = coalesce(
        array_agg(f order by f.column_name, f.op, f.value, f.negate),
        '{}'
    ) from unnest(new.filters_v2) f;

    -- Normalize selected_columns order so ARRAY['a','b'] and ARRAY['b','a'] are
    -- treated as the same subscription group in apply_rls. Preserve an empty
    -- array as '{}' ("primary keys only") so it stays distinct from NULL ("all
    -- columns"); array_agg over an empty set would otherwise collapse it to NULL.
    if new.selected_columns is not null then
        new.selected_columns = coalesce(
            (
                select array_agg(c order by c)
                from unnest(new.selected_columns) c
            ),
            '{}'::text[]
        );
    end if;

    return new;
end;
$$;
