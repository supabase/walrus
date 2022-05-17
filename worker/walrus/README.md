# WALRUS Worker

WALRUS background worker runs next to a PostgreSQL instance. It applies row level security to the write ahead log (WAL) and optionally forwards those records to external services.


```
walrus 0.1.0

USAGE:
    walrus [OPTIONS]

OPTIONS:
        --connection <CONNECTION>    [default: postgresql://postgres@localhost:5432/postgres]
    -h, --help                       Print help information
        --slot <SLOT>                [default: realtime]
    -V, --version                    Print version information
```


## Features

### Realtime (no polling)

The worker wraps [pg_recvlogical](https://www.postgresql.org/docs/current/app-pgrecvlogical.html), a lightweight tool that ships with PostgreSQL, making use of the [streaming replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html#:~:text=The%20logical%20replication%20protocol%20sends,Start%20and%20Stream%20Stop%20messages.) to recieve new WAL messages.

`walrus` processes each message as soon as they are forwarded from `pg_recvlogical`. That change reduces message latency (up-to) 100ms when compared with the current implementation of polling for changes through [pg_logical_get_changes](https://www.postgresql.org/docs/14/logicaldecoding-example.html)

### Self-Configuring

#### Replication Slot
On startup, the requested replication slot if created with the correct wal2json  settings if it doesn't exist.

#### Migrations (Embedded)

The `walrus` executable contains embedded migrations from `./migrations` and applies any unapplied migrations at process startup.


## Try it Out


Requires:
- rust/cargo
- docker-compose
- postgres installed locally (for `pg_recvlogical`)

Clone and Navigate
```sh
git clone https://github.com/supabase/walrus.git
cd walrus
git checkout worker
cd worker
```

Start the DB
```sh
docker-compose up
```

Run the walrus worker
```sh
cd walrus
cargo run -- --connection=postgresql://postgres:password@localhost:5501/postgres

# Note: if you have jq installed, the output is more readable with
#    cargo run -- --connection=postgresql://postgres:password@localhost:5501/postgres | jq
```

Connect to the database at `postgresql://postgres:password@localhost:5501/postgres`

and execute the following SQL to create a subscription and a WAL record.

```sql
-- Create a table we can subscribe to
create table book(
    id int primary key,
    title text
);

-- Create a dummy subscription to our new table
insert into realtime.subscription(subscription_id, entity, claims)
select
    gen_random_uuid(),
    'public.book',
    jsonb_build_object(
        'role', 'postgres',
        'email', 'o@r.com',
        'sub', gen_random_uuid()
    );

-- Create a record
insert into book(id, title)
values (1, 'Foo');
```


Now, looking back out the output from the `cargo run` command, you see the following printed to stdout
```json
> cargo run -- --connection=postgresql://postgres:password@localhost:5501/postgres
...
{
   "wal":{
      "columns":[
         {
            "name":"id",
            "type":"int4"
         },
         {
            "name":"title",
            "type":"text"
         }
      ],
      "commit_timestamp":"2022-04-29T19:04:23Z",
      "record":{
         "id":1,
         "title":"Foo"
      },
      "schema":"public",
      "table":"book",
      "type":"INSERT"
   },
   "is_rls_enabled":false,
   "subscription_ids":[
      "af68a1b5-fbb3-4154-84cc-a2ee1a7048f9",
      "93ba49e4-b8e9-4ab9-bedd-9332a6397806"
   ],
   "errors":[]
}
```


## Possible Enhancements

- Rate limiting
- Some work that is currently being performed in SQL could be shuffled out rust for a performance improvement
    - Reshaping the WAL records
    - Any state we want to track between calls to `realtime.apply_rls`
