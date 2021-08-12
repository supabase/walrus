from sqlalchemy import text, func, select, literal_column, column, literal


REPLICATION_SLOT_FUNC = func.pg_logical_slot_get_changes("rls_poc", None, None, "include-pk", "1")

# Replication slot w/o RLS
SLOT = select([(column('data').op('::')(literal_column('jsonb'))).label("data")]).select_from(REPLICATION_SLOT_FUNC)

# Replication slot with RLS
RLS_SLOT = select([(func.cdc.rls(column('data').op('::')(literal_column('jsonb')))).label("data")]).select_from(REPLICATION_SLOT_FUNC)


def setup_users(sess):
    sess.execute(text("""
insert into auth.users(id)
select extensions.uuid_generate_v4() from generate_series(1,1000);
    """))
    sess.commit()

def setup_note(sess):
    sess.execute(text("""
create table public.note(
    id bigserial primary key,
    user_id uuid not null references auth.users(id),
    body text not null
);
create index ix_note_user_id on public.note (user_id);

-- Access policy so only the owning user_id may see each row
create policy rls_note_select
on public.note
to authenticated
using (auth.uid() = user_id);

alter table note enable row level security;
    """))
    sess.commit()
    # Flush any wal we created
    data = sess.execute(SLOT).all()
    sess.commit()

def setup_subscriptions(sess):
    sess.execute(text("""
insert into cdc.subscription(user_id, entity)
select id, 'public.note' from auth.users limit 2;
    """))
    sess.commit()
    # Flush any wal we created
    data = sess.execute(SLOT).all()
    sess.commit()


def test_read_wal(sess):
    setup_users(sess);
    setup_note(sess);
    sess.execute("""
insert into public.note(user_id, body)
select id, 'take out the trash' from auth.users order by id limit 1;
    """)
    sess.commit()
    data = sess.execute(SLOT).scalar()
    assert 'change' in data
    assert data['change'][0]['table'] == 'note'
    assert 'visible_to' not in data['change'][0]


def test_check_wal2json_settings(sess):
    setup_users(sess);
    setup_note(sess);
    sess.execute("""
insert into public.note(user_id, body)
select id, 'take out the trash' from auth.users order by id limit 1;
    """)
    sess.commit()
    data = sess.execute(SLOT).scalar()
    assert data['change'][0]['table'] == 'note'
    # include-pk setting
    assert 'pk' in data["change"][0]


def test_read_wal_w_visible_to(sess):
    setup_users(sess);
    setup_note(sess);
    setup_subscriptions(sess);
    sess.execute("""
insert into public.note(user_id, body)
select id, 'take out the trash' from auth.users order by id limit 1;
    """)
    sess.commit()
    data = sess.execute(RLS_SLOT).scalar()
    assert 'change' in data
    assert data['change'][0]['table'] == 'note'
    assert 'visible_to' in data['change'][0]
    assert len(data["change"][0]["visible_to"]) == 2
