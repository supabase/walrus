# `walrus`
<p>

<a href=""><img src="https://img.shields.io/badge/postgresql-12+-blue.svg" alt="PostgreSQL version" height="18"></a>
<a href="https://github.com/supabase/wal_rls/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/markdown-subtemplate.svg" alt="License" height="18"></a>


</p>

---

**Source Code**: <a href="https://github.com/supabase/walrus" target="_blank">https://github.com/supabase/walrus</a>

---

Write Ahead Log Realtime Unified Security (WALRUS) is a utility for managing realtime subscriptions to tables and applying row level security rules to those subscriptions.

The subscription stream is based on logical replication slots.

## Summary
### Managing Subscriptions

User subscriptions are managed through a table

```sql
create table realtime.subscription (
    id uuid primary key,
    entity regclass not null,
    filters realtime.user_defined_filter[] not null default '{}',
    claims jsonb not null,
    claims_role regrole not null generated always as (realtime.to_regrole(claims ->> 'role')) stored,
    created_at timestamp not null default timezone('utc', now())
);
```
where `realtime.user_defined_filter` is
```sql
create type realtime.user_defined_filter as (
    column_name text,
    op realtime.equality_op,
    value text
);
```
and `realtime.equality_op`s are a subset of [postgrest ops](https://postgrest.org/en/v4.1/api.html#horizontal-filtering-rows). Specifically:
```sql
create type realtime.equality_op as enum(
    'eq', 'neq', 'lt', 'lte', 'gt', 'gte'
);
```

For example, to subscribe to a table named `public.notes` where the `id` is `6` as the `authenticated` role:
```sql
insert into realtime.subscription(id, entity, filters, claims)
values ('832bd278-dac7-4bef-96be-e21c8a0023c4', 'public.notes', array[('id', 'eq', '6')], '{"role", "authenticated"}');
```


### Reading WAL

This package exposes 1 public SQL function `realtime.apply_rls(jsonb)`. It processes the output of a `wal2json` decoded logical replication slot and returns:

- `wal`: (jsonb) The WAL record as JSONB in the form
- `is_rls_enabled`: (bool) If the entity (table) the WAL record represents has row level security enabled
- `subscription_ids`: (uuid[]) An array subscription ids that should be notified about the WAL record
- `errors`: (text[]) An array of errors

The jsonb WAL record is in the following format for inserts.
```json
{
    "type": "INSERT",
    "schema": "public",
    "table": "todos",
    "columns": [
        {
            "name": "id",
            "type": "int8",
        },
        {
            "name": "details",
            "type": "text",
        },
        {
            "name": "user_id",
            "type": "int8",
        }
    ],
    "commit_timestamp": "2021-09-29T17:35:38Z",
    "record": {
        "id": 1,
        "user_id": 1,
        "details": "mow the lawn"
    }
}
```

updates:
```json
{
    "type": "UPDATE",
    "schema": "public",
    "table": "todos",
    "columns": [
        {
            "name": "id",
            "type": "int8",
        },
        {
            "name": "details",
            "type": "text",
        },
        {
            "name": "user_id",
            "type": "int8",
        }
    ],
    "commit_timestamp": "2021-09-29T17:35:38Z",
    "record": {
        "id": 2,
        "user_id": 1,
        "details": "mow the lawn"
    },
    "old_record": {
        "id": 1,
    }
}
```


deletes:
```json
{
    "type": "DELETE",
    "schema": "public",
    "table": "todos",
    "columns": [
        {
            "name": "id",
            "type": "int8",
        },
        {
            "name": "details",
            "type": "text",
        },
        {
            "name": "user_id",
            "type": "int8",
        }
    ],
    "old_record": {
        "id": 1
    }
}
```

Important Notes:

- Row level security is not applied to delete statements
- The key/value pairs displayed in the `old_record` field include the table's identity columns for the record being updated/deleted. To display all values in `old_record` set the replica identity for the table to full
- When a delete occurs, the contents of `old_record` will be broadcast to all subscribers to that table so ensure that each table's replica identity only contains information that is safe to expose publicly

## Error States

### Error 400: Bad Request, no primary key
If a WAL record for a table that does not have a primary key is passed through `realtime.apply_rls`, an error is returned

Ex:
```sql
(
    null,                            -- wal
    true,                            -- is_rls_enabled
    [...],                           -- subscription_ids,
    array['Error 400: Bad Request, no primary key'] -- errors
)::realtime.wal_rls;
```

### Error 401: Unauthorized
If a WAL record is passed through `realtime.apply_rls` and the subscription's `clams_role` does not have permission to `select` the primary key columns in that table, an `Unauthorized` error is returned with no WAL data.

Ex:
```sql
(
    null,                            -- wal
    true,                            -- is_rls_enabled
    [...],                           -- subscription_ids,
    array['Error 401: Unauthorized'] -- errors
)::realtime.wal_rls;
```

### Error 413: Payload Too Large
When the size of the wal2json record exceeds `max_record_bytes` the `record` and `old_record` keys are set as empty objects `{}` and the `errors` output array will contain the string `"Error 413: Payload Too Large"`

Ex:
```sql
(
    {..., "record": {}, "old_record": {}}, -- wal
    true,                                  -- is_rls_enabled
    [...],                                 -- subscription_ids,
    array['Error 413: Payload Too Large']  -- errors
)::realtime.wal_rls;
```

## How it Works

Each WAL record is passed into `realtime.apply_rls(jsonb)` which:

- impersonates each subscribed user by setting the appropriate role and `request.jwt.claims` that RLS policies depend on
- queries for the row using its primary key values
- applies the subscription's filters to check if the WAL record is filtered out
- filters out all columns that are not visible to the user's role

## Usage

Given a `wal2json` replication slot with the name `realtime`
```sql
select * from pg_create_logical_replication_slot('realtime', 'wal2json')
```

A complete list of config options can be found [here](https://github.com/eulerto/wal2json):

The stream can be polled with

```sql
set search_path = '';

select
    xyz.wal,
    xyz.is_rls_enabled,
    xyz.subscription_ids,
    xyz.errors
from
    pg_logical_slot_get_changes(
        'realtime', null, null,
        'include-pk', '1',
        'include-transaction', 'false',
        'include-timestamp', 'true',
        'write-in-chunks', 'true',
        'format-version', '2',
        'actions', 'insert,update,delete',
        'filter-tables', 'realtime.*'
    ),
    lateral (
        select
            x.wal,
            x.is_rls_enabled,
            x.subscription_ids,
            x.errors
        from
            realtime.apply_rls(data::jsonb) x(wal, is_rls_enabled, subcription_ids, errors)
    ) xyz
where
    xyz.subscription_ids[1] is not null
```

Or, if the stream should be filtered according to a publication:

```sql
set search_path = '';

with pub as (
    select
        concat_ws(
            ',',
            case when bool_or(pubinsert) then 'insert' else null end,
            case when bool_or(pubupdate) then 'update' else null end,
            case when bool_or(pubdelete) then 'delete' else null end
        ) as w2j_actions,
        string_agg(realtime.quote_wal2json(format('%I.%I', schemaname, tablename)::regclass), ',') w2j_add_tables
    from
        pg_publication pp
        join pg_publication_tables ppt
            on pp.pubname = ppt.pubname
    where
        pp.pubname = 'supabase_realtime'
    group by
        pp.pubname
    limit 1
)
select
    xyz.wal,
    xyz.is_rls_enabled,
    xyz.subscription_ids,
    xyz.errors
from
    pub,
    lateral (
          select
            *
          from
             pg_logical_slot_get_changes(
                'realtime', null, null,
                'include-pk', '1',
                'include-transaction', 'false',
                'include-timestamp', 'true',
                'write-in-chunks', 'true',
                'format-version', '2',
                'actions', coalesce(pub.w2j_actions, ''),
                'add-tables', pub.w2j_add_tables
            )
    ) w2j,
    lateral (
        select
            x.wal,
            x.is_rls_enabled,
            x.subscription_ids,
            x.errors
        from
            realtime.apply_rls(
                wal := w2j.data::jsonb,
                max_record_bytes := 1048576
            ) x(wal, is_rls_enabled, subscription_ids, errors)
    ) xyz
where
    coalesce(pub.w2j_add_tables, '') <> ''
    and xyz.subscription_ids[1] is not null
```

## Configuration

### max_record_bytes

`max_record_bytes` (default 1MB): Controls the maximum size of a WAL record that will be emitted with complete `record` and `old_record` data. When the size of the wal2json record exceeds `max_record_bytes` the `record` and `old_record` keys are set as empty objects `{}` and the `errors` output array will contain the string `"Error 413: Payload Too Large"`

Ex:
```sql
realtime.apply_rls(wal := w2j.data::jsonb, max_record_bytes := 1024*1024) x(wal, is_rls_enabled, subscription_ids, errors)
```


## Installation

The project is SQL only and can be installed by executing the contents of `sql/walrus--0.1.sql` in a database instance.

## Tests

Requires

- Python 3.6+
- docker-compose

```shell
pip install -e .

pytest
```

## RFC Process

To open an request for comment (RFC), open a [github issue against this repo and select the RFC template](https://github.com/supabase/walrus/issues/new/choose).
