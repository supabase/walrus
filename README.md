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
create table cdc.subscription (
    id bigint not null generated always as identity,
    user_id uuid not null references auth.users(id),
    entity regclass not null,
    filters cdc.user_defined_filter[],
    created_at timestamp not null default timezone('utc', now()),
    constraint pk_subscription primary key (id)
);
```
where `cdc.user_defined_filter` is
```sql
create type cdc.user_defined_filter as (
    column_name text,
    op cdc.equality_op,
    value text
);
```
and `cdc.equality_op`s are a subset of [postgrest ops](https://postgrest.org/en/v4.1/api.html#horizontal-filtering-rows). Specifically:
```sql
create type cdc.equality_op as enum(
    'eq', 'neq', 'lt', 'lte', 'gt', 'gte'
);
```

For example, to subscribe a user to table named `public.notes` where the `id` is `6`:
```sql
insert into cdc.subscription(user_id, entity, filters)
values ('832bd278-dac7-4bef-96be-e21c8a0023c4', 'public.notes', array[('id', 'eq', '6')]);
```


### Reading WAL

This package exposes 1 public SQL function `cdc.apply_rls(jsonb)`. It processes the output of a `wal2json` decoded logical repliaction slot and returns:

- `wal`: (jsonb) The WAL record as JSONB in the form
- `is_rls_enabled`: (bool) If the entity (table) the WAL record represents has row level security enabled
- `users`: (uuid[]) An array users who should be notified about the WAL record
- `errors`: (text[]) An array of errors

The jsonb WAL record is in the following format for inserts and updates.
```json
{
    "schema": "public",
    "table": "notes",
    "action": "I",
    "columns": [
        {
            "name": "id",
            "type": "bigint",
            "value": 28
        },
        {
            "name": "body",
            "type": "text",
            "value": "take out the trash"
        }
    ]
}
```
where `action` may be `I` or `U` for insert, updated, and delete respectively.

When the WAL record represents a truncate (`action` = `T`) no column information is included and it should be sent to all subscribed users, regardless of their user defined filters e.g.
```json
{
    "schema": "public",
    "table": "notes",
    "action": "T"
}
```

For deletes (`action` = `D`)
- Only identity column data is included in an `identity` field
- Row level security is not applied
- User defined filters are only applied if they filter on identity columns

Since row level security is not applied, data composing a table's identity should be considered public.
e.g.
```json
{
    "schema": "public",
    "table": "note",
    "action": "D",
    "identity": [
        {
            "name": "id",
            "type": "bigint",
            "value": 1
        }
    ]
}
```

## How it Works

Each WAL record is passed into `cdc.apply_rls(jsonb)` which:

- impersonates each subscribed user by setting the role to `authenticated` and `request.jwt.claim.sub` to the subcribed user's id
- queries for the row using its primary key values
- applies the subscription's filters to check if the WAL record is filtered out
- filters out all columns that are not visible to the `authenticated` role

## Usage

Given a `wal2json` replication slot with the name `realtime`
```sql
select * from pg_create_logical_replication_slot('realtime', 'wal2json')
```

The stream can be accessed via

```sql
select
    xyz.wal,
    xyz.is_rls_enabled,
    xyz.users,
    xyz.errors
from
    pg_logical_slot_get_changes(
        'realtime',
        -- Required Config
        null, null,
        'include-pk', '1',
        'include-transaction', 'false',
        'format-version', '2',
        'filter-tables', 'cdc.*',
        -- Optional Config
        'actions', 'insert,update,delete,truncate'
    ),
    lateral (
        select
            x.wal,
            x.is_rls_enabled,
            x.users,
            x.errors
        from
            cdc.apply_rls(data::jsonb) x(wal, is_rls_enabled, users, errors)
    ) xyz
```

A complete list of config options can be found [here](https://github.com/eulerto/wal2json):

## Installation

The project is SQL only and can be installed by executing the contents of `sql/walrus--0.1.sql` in a database instance.
## Roadmap

### Release Blockers
- [x] Filter WAL columns exposed on delete records to include only identity columns

### Non-blockers
- [x] Sanitize filters on write
- [x] Ensure columns referenced by filters exist
- [x] Ensure filter value is coercable to the column's type
- [ ] Ensure user defined equality operations are valid for the filters data type
- [x] Ensure user has visibility on the column they're filtering (column security)

## Tests

Requires

- Python 3.6+
- Docker

```shell
pip install -e .

docker build -t pg_wal_rls -f Dockerfile .

pytest
```

## RFC Process

To open an request for comment (RFC), open a [github issue against this repo and select the RFC template](https://github.com/supabase/walrus/issues/new/choose).
