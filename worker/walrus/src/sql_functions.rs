use cached::proc_macro::cached;
use cached::TimedSizedCache;
use diesel::*;

pub mod sql_functions {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.selectable_columns"]
        fn selectable_columns(schema_name: Text, table_name: Text, role_name: Text) -> Array<Text>;
    }
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
