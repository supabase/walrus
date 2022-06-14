# stream_logical

stream_logical is an option for listening to the logical replication stream from postgres without the existing dependencies on:
- pg_recvlogical (local dependency)
- wal2json (server dependency)

The dependency on pg_recvlogical requires that postgres is installed on the same machine as the walrus server. The wal2json dependency is not installed by default on many postgres setups which reduces compatibility.

This solution would enable walrus to run against unmodified postgres.

### Current State

- Connects to postgres (from parent dir docker-compose)
- Creates a publication and temporary replication slot
- Creates a small amount of WAL
- Receives the WAL messages and prints the message type

### Known TODOS

- A keepalive message needs to be sent on a schedule
- Tracking table state
- Convert logical replication stream to a useable format
- Serialize messages


### Key Commands / Notes

```shell
cargo run --bin stream_logical
```

Output
```text
lsn 0/173C578
keep alive
begin
relation
insert
commit
keep alive
keep alive
```


