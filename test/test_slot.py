import re
from typing import Any, Dict, List, Literal
from uuid import UUID

import pytest
from pydantic import BaseModel, Extra, Field, validator
from sqlalchemy import text


def validate_iso8601(text: str) -> bool:
    """Validates a timestamp string matches iso8601 format"""
    # datetime.datetime.fromisoformat does not handle timezones correctly
    regex = r"^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$"
    match_iso8601 = re.compile(regex).match
    try:
        if match_iso8601(text) is not None:
            return True
    except:
        pass
    return False


class BaseWAL(BaseModel):
    table: str
    schema_: str = Field(..., alias="schema")
    commit_timestamp: str

    class Config:
        extra = Extra.forbid

    @validator("commit_timestamp")
    def validate_commit_timestamp(cls, v):
        validate_iso8601(v)
        return v


class Column(BaseModel):
    name: str
    type: str


ColValDict = Dict[str, Any]
Columns = List[Column]


class DeleteWAL(BaseWAL):
    type: Literal["DELETE"]
    columns: Columns
    old_record: ColValDict


class InsertWAL(BaseWAL):
    type: Literal["INSERT"]
    columns: Columns
    record: ColValDict


class UpdateWAL(BaseWAL):
    type: Literal["UPDATE"]
    record: ColValDict
    columns: Columns
    old_record: ColValDict


QUERY = text(
    """
with pub as (
    select
        concat_ws(
            ',',
            case when bool_or(pubinsert) then 'insert' else null end,
            case when bool_or(pubupdate) then 'update' else null end,
            case when bool_or(pubdelete) then 'delete' else null end
        ) as w2j_actions,
        string_agg(realtime.quote_wal2json(format('%I.%I', schemaname, tablename)::regclass), ',') w2j_add_tables
    from
        pg_publication pp
        join pg_publication_tables ppt
          on pp.pubname = ppt.pubname
    where
        pp.pubname = 'supabase_realtime'
    group by
        pp.pubname
    limit 1
)
select
    w2j.data::jsonb raw,
    xyz.wal,
    xyz.is_rls_enabled,
    xyz.users,
    xyz.errors
from
    pub,
    lateral (
      select
        *
      from
        pg_logical_slot_get_changes(
            'realtime', null, null,
            'include-pk', '1',
            'include-transaction', 'false',
            'include-timestamp', 'true',
            'write-in-chunks', 'true',
            'format-version', '2',
            'actions', coalesce(pub.w2j_actions, ''),
            'add-tables', pub.w2j_add_tables
        )
    ) w2j,
      lateral (
        select
            x.wal,
            x.is_rls_enabled,
            x.users,
            x.errors
        from
            realtime.apply_rls(
                wal := w2j.data::jsonb,
                max_record_bytes := 1048576
            ) x(wal, is_rls_enabled, users, errors)
    ) xyz
where
    coalesce(pub.w2j_add_tables, '') <> ''
"""
)


def clear_wal(sess):
    data = sess.execute(
        "select * from pg_logical_slot_get_changes('realtime', null, null)"
    ).scalar()
    sess.commit()


def setup_note(sess):
    sess.execute(
        text(
            """
revoke select on public.note from authenticated;
grant select (id, user_id, body, arr_text, arr_int) on public.note to authenticated;
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

alter table public.note enable row level security;
    """
        )
    )
    sess.commit()


def insert_subscriptions(sess, filters: Dict[str, Any] = {}, n=1):
    sess.execute(
        text(
            """
insert into realtime.subscription(user_id, entity)
select extensions.uuid_generate_v4(), 'public.note' from generate_series(1,:n);
    """
        ),
        {"n": n},
    )
    sess.commit()


def insert_notes(sess, body="take out the trash", n=1):
    sess.execute(
        text(
            """
insert into public.note(user_id, body)
select user_id, :body from realtime.subscription order by id limit :n;
    """
        ),
        {"n": n, "body": body},
    )
    sess.commit()


def test_read_wal(sess):
    setup_note(sess)
    insert_subscriptions(sess)
    clear_wal(sess)
    insert_notes(sess, 1)
    raw, *_ = sess.execute(QUERY).one()
    assert raw["table"] == "note"


def test_check_wal2json_settings(sess):
    setup_note(sess)
    insert_subscriptions(sess)
    clear_wal(sess)
    insert_notes(sess, 1)
    sess.commit()
    raw, *_ = sess.execute(QUERY).one()
    assert raw["table"] == "note"
    # include-pk setting in wal2json output
    assert "pk" in raw


def test_read_wal_w_visible_to_no_rls(sess):
    setup_note(sess)
    insert_subscriptions(sess)
    clear_wal(sess)
    insert_notes(sess)
    _, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    InsertWAL.parse_obj(wal)
    assert errors == []
    assert not is_rls_enabled
    # visible_to includes subscribed user when no rls enabled
    assert len(users) == 1

    assert [x for x in wal["columns"] if x["name"] == "id"][0]["type"] == "int8"


def test_unauthorized_returns_error(sess):
    sess.execute(
        text(
            """
revoke select on public.unauthorized from authenticated;
    """
        )
    )
    sess.execute(
        text(
            """
insert into realtime.subscription(user_id, entity)
select extensions.uuid_generate_v4(), 'public.unauthorized';
    """
        )
    )
    sess.commit()
    clear_wal(sess)
    sess.execute(
        text(
            """
insert into public.unauthorized(id)
values (1)
    """
        )
    )
    sess.commit()
    _, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    assert (wal, is_rls_enabled, users) == (None, None, [])
    assert len(errors) == 1
    assert errors[0] == "Error 401: Unauthorized"


def test_read_wal_w_visible_to_has_rls(sess):
    setup_note(sess)
    setup_note_rls(sess)
    insert_subscriptions(sess, n=2)
    clear_wal(sess)
    insert_notes(sess, n=1)
    sess.commit()
    _, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    InsertWAL.parse_obj(wal)
    assert errors == []
    assert wal["record"]["id"] == 1
    assert wal["record"]["arr_text"] == ["one", "two"]
    assert wal["record"]["arr_int"] == [1, 2]
    assert [x for x in wal["columns"] if x["name"] == "arr_text"][0]["type"] == "_text"
    assert [x for x in wal["columns"] if x["name"] == "arr_int"][0]["type"] == "_int4"

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


def test_wal_update(sess):
    setup_note(sess)
    setup_note_rls(sess)
    insert_subscriptions(sess, n=2)
    insert_notes(sess, n=1, body="old body")
    clear_wal(sess)
    sess.execute("update public.note set body = 'new body'")
    sess.commit()
    raw, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    UpdateWAL.parse_obj(wal)
    assert wal["record"]["id"] == 1
    assert wal["record"]["body"] == "new body"

    assert wal["old_record"]["id"] == 1
    # Only the identity of the previous
    assert "old_body" not in wal["old_record"]

    assert is_rls_enabled
    # 2 permitted users
    assert len(users) == 1
    # check the "dummy" column is not present in the columns due to
    # role secutiry on "authenticated" role
    columns_in_output = [x["name"] for x in wal["columns"]]
    for col in ["id", "user_id", "body"]:
        assert col in columns_in_output
    assert "dummy" not in columns_in_output
    assert [x for x in wal["columns"] if x["name"] == "id"][0]["type"] == "int8"


def test_wal_update_changed_identity(sess):
    setup_note(sess)
    setup_note_rls(sess)
    insert_subscriptions(sess, n=2)
    insert_notes(sess, n=1, body="some body")
    clear_wal(sess)
    sess.execute("update public.note set id = 99")
    sess.commit()
    _, wal, _, _, errors = sess.execute(QUERY).one()
    UpdateWAL.parse_obj(wal)
    assert errors == []
    assert wal["record"]["id"] == 99
    assert wal["record"]["body"] == "some body"
    assert wal["old_record"]["id"] == 1


def test_wal_delete(sess):
    setup_note(sess)
    setup_note_rls(sess)
    insert_subscriptions(sess, n=2)
    insert_notes(sess, n=1)
    clear_wal(sess)
    sess.execute("delete from public.note;")
    sess.commit()
    _, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    DeleteWAL.parse_obj(wal)
    assert errors == []
    assert wal["old_record"]["id"] == 1
    assert is_rls_enabled
    assert len(users) == 2


def test_error_413_payload_too_large(sess):
    setup_note(sess)
    insert_subscriptions(sess, n=2)
    insert_notes(sess, n=1)
    clear_wal(sess)
    sess.execute("update public.note set body = repeat('a', 5 * 1024 * 1024);")
    sess.commit()
    _, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    UpdateWAL.parse_obj(wal)
    assert any(["413" in x for x in errors])
    assert wal["old_record"] == {}
    assert wal["record"] == {}
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
    setup_note(sess)
    setup_note_rls(sess)

    # Test does not match
    sess.execute(
        f"""
insert into realtime.subscription(user_id, entity, filters)
select
    extensions.uuid_generate_v4(),
    'public.note',
    array[{filter_str}]::realtime.user_defined_filter[];
    """
    )
    sess.commit()
    clear_wal(sess)

    insert_notes(sess, n=1, body="bbb")
    raw, wal, is_rls_enabled, users, errors = sess.execute(QUERY).one()
    assert len(users) == (1 if is_true else 0)


@pytest.mark.performance
@pytest.mark.parametrize("rls_on", [False, True])
def test_performance_on_n_recs_n_subscribed(sess, rls_on: bool):
    setup_note(sess)
    if rls_on:
        setup_note_rls(sess)
    clear_wal(sess)

    if not rls_on:
        with open("perf.tsv", "w") as f:
            f.write("n_notes\tn_subscriptions\texec_time\trls_on\n")

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
                realtime.apply_rls(data::jsonb)
            from
                pg_logical_slot_peek_changes(
                    'realtime', null, null,
                    'include-pk', '1',
                    'include-transaction', 'false',
                    'format-version', '2',
                    'filter-tables', 'realtime.*'
                )
            """
                )
            ).scalar()

            exec_time = float(
                data[data.find("time=") :].split(" ")[0].split("=")[1].split("..")[1]
            )

            with open("perf.tsv", "a") as f:
                f.write(f"{n_notes}\t{n_subscriptions}\t{exec_time}\t{rls_on}\n")

            # Confirm that the data is correct
            data = sess.execute(QUERY).all()
            assert len(data) == n_notes

            # Accumulate the visible_to person for each change and confirm it matches
            # the number of notes
            all_visible_to = []
            for (raw, wal, is_rls_enabled, users, errors) in data:
                for visible_to in users:
                    all_visible_to.append(visible_to)

            if rls_on:
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
            else:
                assert n_subscriptions == len(set(all_visible_to))

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
            truncate table realtime.subscription;
            """
            )
        )
