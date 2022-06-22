use cached::proc_macro::cached;
use cached::{SizedCache, TimedSizedCache};
use diesel::*;

pub mod sql_functions {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        fn is_rls_enabled(schema_name: Text, table_name: Text) -> Bool;
    }

    sql_function! {
        fn is_in_publication(schema_name: Text, table_name: Text, publication_name: Text) -> Bool;
    }

    sql_function! {
        fn selectable_columns(schema_name: Text, table_name: Text, role_name: Text) -> Array<Text>;
    }

    sql_function! {
        fn get_subscriptions() -> Array<Jsonb>;
    }

    sql_function! {
        fn get_subscription_by_id(id: BigInt) -> Jsonb;
    }

    sql_function! {
        fn is_visible_through_filters(columns: Jsonb, subscription_ids: Array<Uuid> ) -> Array<Uuid>
    }

    sql_function! {
        fn is_visible_through_rls(schema_name: Text, table_name: Text, columns: Jsonb, subscription_ids: Array<Uuid>) -> Array<Uuid>
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
    subscription_ids: &Vec<uuid::Uuid>,
    // TODO: convert this to use subscription_ids to reduce n calls
    conn: &mut PgConnection,
) -> Result<Vec<uuid::Uuid>, String> {
    select(sql_functions::is_visible_through_filters(
        serde_json::json!(columns),
        subscription_ids,
    ))
    .first(conn)
    .map_err(|x| format!("{}", x))
}

pub fn is_visible_through_rls(
    schema_name: &str,
    table_name: &str,
    columns: &Vec<crate::walrus_fmt::WALColumn>,
    subscription_ids: &Vec<uuid::Uuid>,
    conn: &mut PgConnection,
) -> Result<Vec<uuid::Uuid>, String> {
    select(sql_functions::is_visible_through_rls(
        schema_name,
        table_name,
        serde_json::to_value(columns).unwrap(),
        subscription_ids,
    ))
    .first(conn)
    .map_err(|x| format!("{}", x))
}

pub fn get_subscriptions(
    conn: &mut PgConnection,
) -> Result<Vec<crate::realtime_fmt::Subscription>, String> {
    let subs: Vec<serde_json::Value> = select(sql_functions::get_subscriptions())
        .first(conn)
        .map_err(|x| format!("{}", x))?;

    let mut res = vec![];

    for sub_json in subs {
        let sub: crate::realtime_fmt::Subscription =
            serde_json::from_value(sub_json).map_err(|x| format!("{}", x))?;
        res.push(sub);
    }

    Ok(res)
}

pub fn get_subscription_by_id(
    id: i64,
    conn: &mut PgConnection,
) -> Result<crate::realtime_fmt::Subscription, String> {
    let sub_value: serde_json::Value = select(sql_functions::get_subscription_by_id(id))
        .first(conn)
        .map_err(|x| format!("{}", x))?;

    let sub: crate::realtime_fmt::Subscription =
        serde_json::from_value(sub_value).map_err(|x| format!("{}", x))?;

    Ok(sub)
}
