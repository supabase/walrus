use crate::models::walrus;
use diesel::*;

pub mod sql {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.is_visible_through_rls"]
        fn is_visible_through_rls(schema_name: Text, table_name: Text, columns: Jsonb, ids: Array<Int8>) -> Array<Int8>
    }
}

pub fn is_visible_through_rls(
    schema_name: &str,
    table_name: &str,
    columns: &Vec<walrus::Column>,
    ids: &Vec<i64>,
    conn: &mut PgConnection,
) -> Result<Vec<i64>, String> {
    select(sql::is_visible_through_rls(
        schema_name,
        table_name,
        serde_json::to_value(columns).unwrap(),
        ids,
    ))
    .first(conn)
    .map_err(|x| format!("{}", x))
}
