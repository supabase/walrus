# `walrus`
<p>

<a href=""><img src="https://img.shields.io/badge/postgresql-12+-blue.svg" alt="PostgreSQL version" height="18"></a>
<a href="https://github.com/supabase/wal_rls/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/markdown-subtemplate.svg" alt="License" height="18"></a>
<a href="https://github.com/supabase/wal_rls/actions"><img src="https://github.com/supabase/wal_rls/actions/workflows/main.yml/badge.svg" alt="Tests" height="18"></a>

</p>

---

**Source Code**: <a href="https://github.com/supabase/wal_rls" target="_blank">https://github.com/supabase/wal_rls</a>

---
## Summary

Write Ahead Log Realtime Unified Security (WALRUS) is a tool for applying user defined row level security rules to a PostgreSQL logical replication stream.


This repo provides a SQL function `cdc.wal_rls(jsonb)` that post-processes the output of a `wal2json` decoded logical repliaction slot to add the following info

```json
{
    "security": {
        "is_rls_enabled": true,
        "visible_to": [
            "dcd7936b-213c-4efb-b5de-a425f5573027",
            "6dd09788-758c-4c02-ae1a-81d1d318b056"
        ]
    }
}
```
where `is_rls_enabled` is a `bool` to check if row level security is applied for the current WAL record and `visible_to` is a list of `uuid` representing users who:

- Have permission to view the record accoreding to RLS rules associated with the table
- Have subscribed to the table that the WAL record came from
- Have user-defined filters associated with the subscription which do not exclude the record


## How it Works

For each subscribed user, ever WAL record is passed into `cdc.wal_rls(jsonb)` which:

- impersonates that user by setting the current transaction's role to `authenticated` and `request.jwt.claim.sub` to the subcribed user's id
See 

- queries for the row described by the WAL record using its primary key values to ensure uniqueness and performance
- Applies user defined filters associated with each user's subscription to the row to remove records were not requested
- Checks which columns of the WAL record are visible to the `authenticated` role and filters out any 


## Usage

### TL;DR

If you're familiar with logical replication, `pg_logical_slot_get_changes` and `wal2json`:

```sql
select
    cdc.wal_rls(data::jsonb)
from
    pg_logical_slot_get_changes(
        'realtime_slot', null, null,
        'include-pk', '1',
        'include-transaction', 'false',
        'format-version', '2',
        -- TODO: delete + truncate
        'actions', 'insert,update',
        'filter-tables', 'cdc.*,auth.*'
    )
```

### Explanation

`pg_logical_get_changes` is a function returning WAL records associated with a logical replication slot (in the example above the slot name is `realtime_slot`).

To create a replication slots, we have to choose an [output plugin](https://wiki.postgresql.org/wiki/Logical_Decoding_Plugins) that will determine the output format. Since 

We can create a new replication slot with the 
```
select * from pg_create_logical_replication_slot('realtime_slot', 'wal2json')
```





Currently standalone but will likely be added to supautils for deployment

## Limitations

- No implementation for truncate/delete statement. There are options, just need to pick a strategy  
- RLS not applied to delete visibility. i.e. everyone sees deletes and their primary key (TODO)

### Run the Tests

Requires:

- Python 3.6+
- Docker 

```shell
pip intall -e .

docker build -t pg_wal_rls -f Dockerfile .

pytest
```
