from typing import Any, Dict
from uuid import UUID

import pytest
from sqlalchemy import text

QUERY = text(
    """
select
    data::jsonb,
    xyz.wal,
    xyz.is_rls_enabled,
    xyz.users,
    xyz.errors
from
    pg_logical_slot_get_changes(
        'rls_poc', null, null,
        'include-pk', '1',
        'include-transaction', 'false',
        'include-timestamp', 'true',
        'write-in-chunks', 'true',
        'format-version', '2',
        'actions', 'insert,update,delete,truncate',
        'filter-tables', 'cdc.*,auth.*'
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
"""
)


def clear_wal(sess):
    data = sess.execute(
        "select * from pg_logical_slot_get_changes('rls_poc', null, null)"
    ).scalar()
    sess.commit()


def setup_note(sess):
    sess.execute(
        text(
            """
create table public.note(
    id bigserial primary key,
    user_id uuid not null references auth.users(id),
    body text not null,

    -- dummy column with revoked select for "authenticated"
    dummy text

);
create index ix_note_user_id on public.note (user_id);

revoke select on public.note from authenticated;
grant select (id, user_id, body) on public.note to authenticated;

    """
        )
    )
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


def test_read_wal(sess):
    setup_note(sess)
    insert_users(sess)
    clear_wal(sess)
    insert_notes(sess, 1)
    raw, *_ = sess.execute(QUERY).one()
    assert raw["table"] == "note"


def test_check_wal2json_settings(sess):
    insert_users(sess)
    setup_note(sess)
    clear_wal(sess)
    insert_notes(sess, 1)
    sess.commit()
    raw, *_ = sess.execute(QUERY).one()
    assert raw["table"] == "note"
    # include-pk setting in wal2json output
    assert "pk" in raw
    # column position
    # assert "position" in raw["columns"][0]


def test_read_wal_w_visible_to_no_rls(sess):
    setup_note(sess)
    insert_users(sess)
    insert_subscriptions(sess)
    clear_wal(sess)
    insert_notes(sess)
    raw, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    assert wal["table"] == "note"
    assert not is_rls_enabled
    # visible_to includes subscribed user when no rls enabled
    assert len(users) == 1


def test_read_wal_w_visible_to_has_rls(sess):
    insert_users(sess)
    setup_note(sess)
    setup_note_rls(sess)
    insert_subscriptions(sess, n=2)
    clear_wal(sess)
    insert_notes(sess, n=1)
    raw, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    assert wal["table"] == "note"

    # pk info was filtered out
    assert "pk" not in wal
    # position info was filtered out
    assert "position" not in wal["columns"][0]

    assert is_rls_enabled
    # 2 permitted users
    assert len(users) == 1
    # check user_id
    assert isinstance(users[0], UUID)
    # check the "dummy" column is not present in the columns due to
    # role secutiry on "authenticated" role
    columns_in_output = [x["name"] for x in wal["columns"]]
    for col in ["id", "user_id", "body"]:
        assert col in columns_in_output
    assert "dummy" not in columns_in_output


def test_wal_truncate(sess):
    insert_users(sess)
    setup_note(sess)
    setup_note_rls(sess)
    insert_subscriptions(sess, n=2)
    insert_notes(sess, n=1)
    clear_wal(sess)
    sess.execute("truncate table public.note;")
    sess.commit()
    raw, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    for key, value in {
        "table": "note",
        "action": "TRUNCATE",
        "schema": "public",
    }.items():
        assert wal[key] == value
    assert wal["commit_timestamp"].startswith("2")

    assert is_rls_enabled
    assert len(users) == 2


def test_wal_delete(sess):
    insert_users(sess)
    setup_note(sess)
    setup_note_rls(sess)
    insert_subscriptions(sess, n=2)
    insert_notes(sess, n=1)
    clear_wal(sess)
    sess.execute("delete from public.note;")
    sess.commit()
    raw, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    for key, value in {
        "action": "DELETE",
        "schema": "public",
        "table": "note",
        "old_record": {"id": 1},
    }.items():
        assert wal[key] == value
    assert wal["commit_timestamp"].startswith("2")

    assert is_rls_enabled
    assert len(users) == 2


@pytest.mark.parametrize(
    "filter_str,is_true",
    [
        # The WAL record body is "bbb"
        ("('body', 'eq', 'bbb')", True),
        ("('body', 'eq', 'aaaa')", False),
        ("('body', 'eq', 'cc')", False),
        ("('body', 'neq', 'bbb')", False),
        ("('body', 'neq', 'cat')", True),
        ("('body', 'lt', 'aa')", False),
        ("('body', 'lt', 'ccc')", True),
        ("('body', 'lt', 'bbb')", False),
        ("('body', 'lte', 'aa')", False),
        ("('body', 'lte', 'ccc')", True),
        ("('body', 'lte', 'bbb')", True),
        ("('body', 'gt', 'aa')", True),
        ("('body', 'gt', 'ccc')", False),
        ("('body', 'gt', 'bbb')", False),
        ("('body', 'gte', 'aa')", True),
        ("('body', 'gte', 'ccc')", False),
        ("('body', 'gte', 'bbb')", True),
    ],
)
def test_user_defined_eq_filter(filter_str, is_true, sess):
    insert_users(sess)
    setup_note(sess)
    setup_note_rls(sess)

    # Test does not match
    sess.execute(
        f"""
insert into cdc.subscription(user_id, entity, filters)
select
    id,
    'public.note',
    array[{filter_str}]::cdc.user_defined_filter[]
from
    auth.users order by id
limit 1;
    """
    )
    sess.commit()
    clear_wal(sess)

    insert_notes(sess, n=1, body="bbb")
    raw, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    assert len(users) == (1 if is_true else 0)


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
                cdc.apply_rls(data::jsonb)
            from
                pg_logical_slot_peek_changes(
                    'rls_poc', null, null,
                    'include-pk', '1',
                    'include-transaction', 'false',
                    'format-version', '2',
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
            data = sess.execute(QUERY).all()
            assert len(data) == n_notes

            # Accumulate the visible_to person for each change and confirm it matches
            # the number of notes
            all_visible_to = []
            for (raw, wal, is_rls_enabled, users, errors) in data:
                for visible_to in users:
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
            clear_wal(sess)

        sess.execute(
            text(
                """
            truncate table cdc.subscription;
            """
            )
        )
