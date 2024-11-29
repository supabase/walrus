use crate::errors;
use crate::models::walrus;
use diesel::*;

pub mod sql {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.is_visible_through_rls"]
        fn is_visible_through_rls(table_oid: Oid, columns: Jsonb, ids: Array<Int8>) -> Array<Int8>
    }
}

pub fn is_visible_through_rls(
    table_oid: u32,
    columns: &Vec<walrus::Column>,
    ids: &Vec<i64>,
    conn: &mut PgConnection,
) -> Result<Vec<i64>, errors::Error> {
    select(sql::is_visible_through_rls(
        table_oid,
        serde_json::to_value(columns).unwrap(),
        ids,
    ))
    .first(conn)
    .map_err(|x| errors::Error::SQLFunction(format!("{}", x)))
}
