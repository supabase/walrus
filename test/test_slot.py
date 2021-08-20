import os
from typing import Dict, Any

import pytest
from sqlalchemy import column, func, literal, literal_column, select, text

REPLICATION_SLOT_FUNC = func.pg_logical_slot_get_changes(
    "rls_poc",
    None,
    None,
    # WAL2JSON settings
    "include-pk",
    "1",
    "format-version",
    "2",
    "actions",
    "insert,update,delete",
    "filter-tables",
    "cdc.*,auth.*",
)

# Replication slot w/o RLS
SLOT = (
    select([(column("data").op("::")(literal_column("jsonb"))).label("data")])
    .select_from(REPLICATION_SLOT_FUNC)
    .where(
        (column("data").op("::")(literal_column("jsonb")))
        .op("->>")("action")
        .not_in(["B", "C"])
    )
)

# Replication slot with RLS
RLS_SLOT = (
    select(
        [(func.cdc.rls(column("data").op("::")(literal_column("jsonb")))).label("data")]
    )
    .select_from(REPLICATION_SLOT_FUNC)
    .where(
        (column("data").op("::")(literal_column("jsonb")))
        .op("->>")("action")
        .not_in(["B", "C"])
    )
)


def setup_note(sess):
    sess.execute(
        text(
            """
create table public.note(
    id bigserial primary key,
    user_id uuid not null references auth.users(id),
    body text not null
);
create index ix_note_user_id on public.note (user_id);

    """
        )
    )
    sess.commit()
    # Flush any wal we created
    data = sess.execute(SLOT).all()
    sess.commit()


def setup_note_rls(sess):
    sess.execute(
        text(
            """
-- Access policy so only the owning user_id may see each row
create policy rls_note_select
on public.note
to authenticated
using (auth.uid() = user_id);

alter table note enable row level security;
    """
        )
    )
    sess.commit()
    # Flush any wal we created
    data = sess.execute(SLOT).all()
    sess.commit()


def insert_users(sess, n=10):
    sess.execute(
        text(
            """
insert into auth.users(id)
select extensions.uuid_generate_v4() from generate_series(1,:n);
    """
        ),
        {"n": n},
    )
    sess.commit()


def insert_subscriptions(sess, filters: Dict[str, Any] = {}, n=1):
    sess.execute(
        text(
            """
insert into cdc.subscription(user_id, entity)
select id, 'public.note' from auth.users order by id limit :lim;
    """
        ),
        {"lim": n},
    )
    sess.commit()


def insert_notes(sess, body="take out the trash", n=1):
    sess.execute(
        text(
            """
insert into public.note(user_id, body)
select id, :body from auth.users order by id limit :n;
    """
        ),
        {"n": n, "body": body},
    )
    sess.commit()


def clear_wal(sess):
    data = sess.execute(SLOT).scalar()
    sess.commit()


def test_read_wal(sess):
    setup_note(sess)
    insert_users(sess)
    clear_wal(sess)
    insert_notes(sess, 1)
    data = sess.execute(SLOT).scalar()
    assert data["table"] == "note"
    assert "security" not in data


def test_check_wal2json_settings(sess):
    insert_users(sess)
    setup_note(sess)
    insert_notes(sess, 1)
    sess.commit()
    data = sess.execute(SLOT).scalar()
    assert data["table"] == "note"
    # include-pk setting
    assert "pk" in data


def test_read_wal_w_visible_to_no_rls(sess):
    setup_note(sess)
    insert_users(sess)
    insert_subscriptions(sess)
    clear_wal(sess)
    insert_notes(sess)
    data = sess.execute(RLS_SLOT).scalar()
    assert data["table"] == "note"
    assert "security" in data

    security = data["security"]
    assert not security["is_rls_enabled"]
    # visible_to is empty when no rls enabled
    assert len(security["visible_to"]) == 0
    assert len(security["visible_columns"]) == 3


def test_read_wal_w_visible_to_has_rls(sess):
    insert_users(sess)
    setup_note(sess)
    setup_note_rls(sess)
    insert_subscriptions(sess, n=2)
    clear_wal(sess)
    insert_notes(sess, n=1)
    data = sess.execute(RLS_SLOT).scalar()
    assert data["table"] == "note"
    assert "security" in data

    security = data["security"]
    assert security["is_rls_enabled"]
    # 2 permitted users
    assert len(security["visible_to"]) == 1
    assert len(security["visible_to"][0]) > 10
    assert security["visible_columns"] == ["id", "user_id", "body"]


@pytest.mark.performance
def test_performance_on_n_recs_n_subscribed(sess):
    insert_users(sess, n=10000)
    setup_note(sess)
    setup_note_rls(sess)
    clear_wal(sess)

    with open("perf.tsv", "w") as f:
        f.write("n_notes\tn_subscriptions\texec_time\n")

    for n_subscriptions in [
        1,
        2,
        3,
        5,
        10,
        25,
        50,
        100,
        250,
        500,
        1000,
        2000,
        5000,
        10000,
    ]:
        insert_subscriptions(sess, n=n_subscriptions)
        for n_notes in [1, 2, 3, 4, 5]:
            clear_wal(sess)
            insert_notes(sess, n=n_notes)

            data = sess.execute(
                text(
                    """
            explain analyze
            select
                cdc.rls(data::jsonb)
            from
                pg_logical_slot_peek_changes(
                    'rls_poc', null, null, 
                    'include-pk', '1',
                    'format-version', '2',
                    'actions', 'insert,update,delete',
                    'filter-tables', 'cdc.*,auth.*'
                )
            """
                )
            ).scalar()

            exec_time = float(
                data[data.find("time=") :].split(" ")[0].split("=")[1].split("..")[1]
            )

            with open("perf.tsv", "a") as f:
                f.write(f"{n_notes}\t{n_subscriptions}\t{exec_time}\n")

            # Confirm that the data is correct
            data = sess.execute(RLS_SLOT).all()
            assert len(data) >= 1

            # Accumulate the visible_to person for each change and confirm it matches
            # the number of notes
            all_visible_to = []
            for (row,) in data:
                for visible_to in row["security"]["visible_to"]:
                    all_visible_to.append(visible_to)
            try:
                assert (
                    len(all_visible_to)
                    == len(set(all_visible_to))
                    == min(n_notes, n_subscriptions)
                )
            except:
                print(
                    "n_notes",
                    n_notes,
                    "n_subscriptions",
                    n_subscriptions,
                    all_visible_to,
                )
                raise

            sess.execute(
                text(
                    """
            truncate table public.note;
            """
                )
            )

        sess.execute(
            text(
                """
            truncate table cdc.subscription;
            """
            )
        )
