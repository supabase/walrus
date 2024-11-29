use crate::errors;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use diesel::*;

pub mod sql {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.is_in_publication"]
        fn is_in_publication(schema_name: Text, table_name: Text, publication_name: Text) -> Bool;
    }
}

#[cached(
    type = "TimedSizedCache<String, Result<bool, errors::Error>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(250, 1)}",
    convert = r#"{ format!("{}.{}-{}", schema_name, table_name, publication_name) }"#,
    sync_writes = true
)]
pub fn is_in_publication(
    schema_name: &str,
    table_name: &str,
    publication_name: &str,
    conn: &mut PgConnection,
) -> Result<bool, errors::Error> {
    select(sql::is_in_publication(
        schema_name,
        table_name,
        publication_name,
    ))
    .first(conn)
    .map_err(|x| errors::Error::SQLFunction(format!("{}", x)))
}
