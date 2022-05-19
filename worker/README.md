# WALRUS Worker + Realtime Transport

Example showing how to stream WAL from postgres, apply row level security, and push changes to supabase realtime.

- `walrus/` is responsible for receiving WAL, formatting messages, and applying row level security
- `realtime/` is the websocket trasport layer for supabase realtime.

More info about each component can be found in their directories' README.md.

## Example:

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

Run the `walrus` worker, piping its output to `realtime` transport
```sh
cargo run --bin walrus -- \
    --connection=postgresql://postgres:password@localhost:5501/postgres |
cargo run --bin realtime -- \
    --url=wss://sendwal.fly.dev/socket \
    --header=apikey=<apikey>
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