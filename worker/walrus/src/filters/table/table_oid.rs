use crate::errors;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use diesel::*;

pub mod sql {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.get_table_oid"]
        fn get_table_oid(schema_name: Text, table_name: Text) -> Oid;
    }
}

#[cached(
    type = "TimedSizedCache<String, Result<u32, errors::Error>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(10000, 1)}",
    convert = r#"{ format!("{}.{}", schema_name, table_name) }"#,
    sync_writes = true
)]
pub fn get_table_oid(
    schema_name: &str,
    table_name: &str,
    conn: &mut PgConnection,
) -> Result<u32, errors::Error> {
    select(sql::get_table_oid(schema_name, table_name))
        .first(conn)
        .map_err(|x| errors::Error::SQLFunction(format!("{}", x)))
}
