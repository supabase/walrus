# `wal_rls`

<p>

<a href=""><img src="https://img.shields.io/badge/postgresql-12+-blue.svg" alt="PostgreSQL version" height="18"></a>
<a href="https://github.com/supabase/wal_rls/blob/master/LICENSE"><img src="https://img.shields.io/pypi/l/markdown-subtemplate.svg" alt="License" height="18"></a>
<a href="https://github.com/supabase/wal_rls/actions"><img src="https://github.com/supabase/wal_rls/actions/workflows/main.yml/badge.svg" alt="Tests" height="18"></a>

</p>

---

**Source Code**: <a href="https://github.com/supabase/wal_rls" target="_blank">https://github.com/supabase/wal_rls</a>

---


## Summary 

Research & Test setup for applying RLS rules to WAL replication for realtime security.

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
