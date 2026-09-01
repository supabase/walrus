use crate::errors;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use diesel::*;

pub mod sql {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.is_rls_enabled"]
        fn is_rls_enabled(table_oid: Oid) -> Bool;
    }
}

#[cached(
    type = "TimedSizedCache<String, Result<bool, errors::Error>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(250, 1)}",
    convert = r#"{ format!("{}", table_oid) }"#,
    sync_writes = true
)]
pub fn is_rls_enabled(table_oid: u32, conn: &mut PgConnection) -> Result<bool, errors::Error> {
    select(sql::is_rls_enabled(table_oid))
        .first(conn)
        .map_err(|x| errors::Error::SQLFunction(format!("{}", x)))
}
