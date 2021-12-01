from sqlalchemy import text

QUERY = text(
    """
with pub as (
    select
        pp.pubname pub_name,
        bool_or(puballtables) pub_all_tables,
        (
            select
                string_agg(act.name_, ',') actions
            from
                unnest(array[
                    case when bool_or(pubinsert) then 'insert' else null end,
                    case when bool_or(pubupdate) then 'update' else null end,
                    case when bool_or(pubdelete) then 'delete' else null end,
                    case when bool_or(pubtruncate) then 'truncate' else null end
                ]) act(name_)
        ) w2j_actions,
        case
            -- collect all tables
            when bool_and(puballtables) then (
                select
                    string_agg(cdc.quote_wal2json((schemaname || '.' || tablename)::regclass), ',')
                from
                    pg_tables
                where
                    schemaname not in ('cdc', 'pg_catalog', 'information_schema')
            )
            -- null when no tables are in the publication
            else string_agg(cdc.quote_wal2json(prrelid::regclass), ',')
        end w2j_add_tables
    from
        pg_publication pp
        left join pg_publication_rel ppr
            on pp.oid = ppr.prpubid
    where
        pp.pubname = 'test_pub'
    group by
        pp.pubname
    limit 1
)

select
    pub_all_tables,
    w2j_add_tables
from
    pub
"""
)


def test_pub_with_table(engine):
    """A publication with an associated table is included in w2j_add_tables"""
    engine.execute(
        text(
            """
    create table public.note (id serial primary key);
    """
        )
    )

    engine.execute(
        text(
            """
    create publication
        test_pub
    for table
        public.note
    with (
        publish = 'insert,update,delete,truncate'
    )
    """
        )
    )

    pub_all_tables, w2j_add_tables = engine.execute(QUERY).fetchone()

    assert (pub_all_tables, w2j_add_tables) == (False, r"\p\u\b\l\i\c.\n\o\t\e")

    # Cleanup
    engine.execute(
        text(
            """
    drop table public.note;
    drop publication test_pub;
    """
        )
    )


def test_pub_with_no_table(engine):
    """A publication with an associated table results in a null w2j_add_tables"""
    engine.execute(
        text(
            """
    create publication test_pub
    """
        )
    )

    pub_all_tables, w2j_add_tables = engine.execute(QUERY).fetchone()

    # Nulls are filtered out before hitting wal2json
    assert (pub_all_tables, w2j_add_tables) == (False, None)

    # Cleanup
    engine.execute(
        text(
            """
    drop publication test_pub;
    """
        )
    )


def test_pub_all_with_table(engine):
    """A publication for all tables includes a sample table"""
    engine.execute(
        text(
            """
    create table public.note (id serial primary key);
    """
        )
    )

    engine.execute(
        text(
            """
    create publication
        test_pub
    for all tables
    with (
        publish = 'insert,update,delete,truncate'
    )
    """
        )
    )

    pub_all_tables, w2j_add_tables = engine.execute(QUERY).fetchone()

    assert (pub_all_tables, w2j_add_tables) == (True, r"\p\u\b\l\i\c.\n\o\t\e")

    # Cleanup
    engine.execute(
        text(
            """
    drop table public.note;
    drop publication test_pub;
    """
        )
    )


def test_pub_all_with_no_table(engine):
    """A publication for all tables sets w2j_add_tables to null when no tables exist"""
    engine.execute(
        text(
            """
    create publication
        test_pub
    for all tables
    with (
        publish = 'insert,update,delete,truncate'
    )
    """
        )
    )

    pub_all_tables, w2j_add_tables = engine.execute(QUERY).fetchone()

    # Nulls are filtered out before hitting wal2json
    assert (pub_all_tables, w2j_add_tables) == (True, None)

    # Cleanup
    engine.execute(
        text(
            """
    drop publication test_pub;
    """
        )
    )
