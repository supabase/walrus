use cached::proc_macro::cached;
use cached::TimedSizedCache;
use diesel::*;

pub mod sql {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.is_rls_enabled"]
        fn is_rls_enabled(schema_name: Text, table_name: Text) -> Bool;
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
    select(sql::is_rls_enabled(schema_name, table_name))
        .first(conn)
        .map_err(|x| format!("{}", x))
}
