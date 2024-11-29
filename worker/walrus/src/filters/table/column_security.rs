use crate::errors;
use crate::models::realtime;
use cached::proc_macro::cached;
use cached::TimedSizedCache;
use diesel::*;

pub mod sql {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.selectable_columns"]
        fn selectable_columns(table_oid: Oid, role_name: Text) -> Array<Jsonb>;
    }
}

#[cached(
    type = "TimedSizedCache<String, Result<Vec<realtime::Column>, errors::Error>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(500, 1)}",
    convert = r#"{ format!("{}-{}", table_oid, role_name) }"#,
    sync_writes = true
)]
pub fn selectable_columns(
    table_oid: u32,
    role_name: &str,
    conn: &mut PgConnection,
) -> Result<Vec<realtime::Column>, errors::Error> {
    let cols_as_json: Vec<serde_json::Value> =
        select(sql::selectable_columns(table_oid, role_name))
            .first(conn)
            .map_err(|x| errors::Error::SQLFunction(format!("{}", x)))?;

    let r: Result<Vec<realtime::Column>, errors::Error> = cols_as_json
        .into_iter()
        .map(|col_json| {
            serde_json::from_value(col_json)
                .map_err(|x| errors::Error::SQLFunction(format!("{}", x)))
        })
        .collect();
    r
}
