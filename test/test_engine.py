from sqlalchemy import text

def test_connect(sess):
    (x,) = sess.execute("select 1").fetchone()
    assert x == 1


def setup_note(sess):
    sess.execute(text("""
insert into auth.users(id)
select extensions.uuid_generate_v4() from generate_series(1,10);

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

    # Flush any wal we created
    sess.execute("""
    select lsn, xid, data::jsonb from pg_logical_slot_get_changes('rls_poc', NULL, NULL);
    """)
    sess.commit()




def test_read_wal(sess):
    setup_note(sess);

    sess.execute("""
insert into public.note(user_id, body)
select id, 'take out the trash' from auth.users order by id limit 1;
    """)
    sess.commit()

    lsn, xid, data = sess.execute("""
        select lsn, xid, data::jsonb from pg_logical_slot_peek_changes('rls_poc', NULL, NULL);
            """).fetchone()

    assert 'change' in data
    assert data['change'][0]['table'] == 'note'
    assert 'visible_to' not in data['change'][0]


def test_read_wal_w_visible_to(sess):
    setup_note(sess);

    sess.execute("""
insert into public.note(user_id, body)
select id, 'take out the trash' from auth.users order by id limit 1;
    """)
    sess.commit()

    lsn, xid, data = sess.execute("""
        select lsn, xid, cdc.change_append_visible_to(data::jsonb)
from
	pg_logical_slot_peek_changes('rls_poc', NULL, NULL);
            """).fetchone()

    assert 'change' in data
    assert data['change'][0]['table'] == 'note'
    assert 'visible_to' in data['change'][0]
