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

- impersonates that user by setting the role to `authenticated` and `request.jwt.claim.sub` to the subcribed user's id
- queries for the row described by the WAL record using its primary key values to ensure uniqueness and performance
- Applies user defined filters associated with each user's subscription to the row to remove records that are not of interest




## Usage





Currently standalone but will likely be added to supautils for deployment

## Limitations

- truncate statements
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
