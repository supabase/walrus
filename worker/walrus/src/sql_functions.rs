use cached::proc_macro::cached;
use cached::TimedSizedCache;
use diesel::*;

pub mod sql_functions {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.is_rls_enabled"]
        fn is_rls_enabled(schema_name: Text, table_name: Text) -> Bool;
    }

    sql_function! {
        #[sql_name = "realtime.is_in_publication"]
        fn is_in_publication(schema_name: Text, table_name: Text, publication_name: Text) -> Bool;
    }

    sql_function! {
        #[sql_name = "realtime.selectable_columns"]
        fn selectable_columns(schema_name: Text, table_name: Text, role_name: Text) -> Array<Text>;
    }

    sql_function! {
        #[sql_name = "realtime.is_visible_through_filters"]
        fn is_visible_through_filters(columns: Jsonb, ids: Array<Int8> ) -> Array<Int8>
    }

    sql_function! {
        #[sql_name = "realtime.is_visible_through_rls"]
        fn is_visible_through_rls(schema_name: Text, table_name: Text, columns: Jsonb, ids: Array<Int8>) -> Array<Int8>
    }
}

#[cached(
    type = "TimedSizedCache<String, Result<bool, String>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(250, 1)}",
    convert = r#"{ format!("{}.{}", schema_name, table_name) }"#,
    sync_writes = true
)]
pub fn is_rls_enabled(
    schema_name: &str,
    table_name: &str,
    conn: &mut PgConnection,
) -> Result<bool, String> {
    select(sql_functions::is_rls_enabled(schema_name, table_name))
        .first(conn)
        .map_err(|x| format!("{}", x))
}

#[cached(
    type = "TimedSizedCache<String, Result<bool, String>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(250, 1)}",
    convert = r#"{ format!("{}.{}-{}", schema_name, table_name, publication_name) }"#,
    sync_writes = true
)]
pub fn is_in_publication(
    schema_name: &str,
    table_name: &str,
    publication_name: &str,
    conn: &mut PgConnection,
) -> Result<bool, String> {
    select(sql_functions::is_in_publication(
        schema_name,
        table_name,
        publication_name,
    ))
    .first(conn)
    .map_err(|x| format!("{}", x))
}

#[cached(
    type = "TimedSizedCache<String, Result<Vec<String>, String>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(500, 1)}",
    convert = r#"{ format!("{}.{}-{}", schema_name, table_name, role_name) }"#,
    sync_writes = true
)]
pub fn selectable_columns(
    schema_name: &str,
    table_name: &str,
    role_name: &str,
    conn: &mut PgConnection,
) -> Result<Vec<String>, String> {
    select(sql_functions::selectable_columns(
        schema_name,
        table_name,
        role_name,
    ))
    .first(conn)
    .map_err(|x| format!("{}", x))
}

pub fn is_visible_through_filters(
    columns: &Vec<crate::walrus_fmt::WALColumn>,
    ids: &Vec<i64>,
    // TODO: convert this to use subscription_ids to reduce n calls
    conn: &mut PgConnection,
) -> Result<Vec<i64>, String> {
    select(sql_functions::is_visible_through_filters(
        serde_json::json!(columns),
        ids,
    ))
    .first(conn)
    .map_err(|x| format!("{}", x))
}

pub fn is_visible_through_rls(
    schema_name: &str,
    table_name: &str,
    columns: &Vec<crate::walrus_fmt::WALColumn>,
    ids: &Vec<i64>,
    conn: &mut PgConnection,
) -> Result<Vec<i64>, String> {
    select(sql_functions::is_visible_through_rls(
        schema_name,
        table_name,
        serde_json::to_value(columns).unwrap(),
        ids,
    ))
    .first(conn)
    .map_err(|x| format!("{}", x))
}
