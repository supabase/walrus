#[macro_use]
extern crate diesel;
use clap::Parser;
use diesel::prelude::*;
use diesel::*;
use env_logger;
use itertools::Itertools;
use log::{debug, error, info, warn};
use models::{realtime, wal2json, walrus};
use serde_json;
use sql::schema::realtime::subscription::dsl::*;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time;

mod errors;
mod filters;
mod models;
mod sql;
mod timestamp_fmt;

/// Write-Ahead-Log Realtime Unified Security (WALRUS) background worker
/// runs next to a PostgreSQL instance and forwards its Write-Ahead-Log
/// to external services
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "realtime")]
    slot: String,

    #[clap(long, default_value = "postgresql://postgres@localhost:5432/postgres")]
    connection: String,

    #[clap(long, default_value = "supabase_multiplayer")]
    publication: String,

    // Exit when no work remains
    #[clap(long)]
    exit_on_no_work: bool,
}

fn main() {
    // Parse command line arguments
    let args = Args::parse();

    // enable logger
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    loop {
        match run(&args) {
            Err(err) => {
                warn!("Error: {}", err);

                if args.exit_on_no_work {
                    return ();
                }
            }
            _ => continue,
        };
        info!("Stream interrupted. Restarting in 5 seconds");
        sleep(time::Duration::from_secs(5));
    }
}

fn run(args: &Args) -> Result<(), errors::Error> {
    // Connect to Postgres
    let conn_result = &mut PgConnection::establish(&args.connection);
    let publication = &args.publication;

    let conn = match conn_result {
        Ok(c) => c,
        Err(err) => {
            return Err(errors::Error::PostgresConnectionError(format!("{}", err)));
        }
    };

    // Run pending migrations
    sql::migrations::run_migrations(conn).expect("Pending migrations failed to execute");
    info!("Postgres connection established");

    // Empty search path
    sql_query("set search_path=''")
        .execute(conn)
        .expect("failed to set search path");

    let cmd = Command::new("pg_recvlogical")
        //&args
        .args(vec![
            "--file=-",
            "--plugin=wal2json",
            &format!("--dbname={}", args.connection),
            "--option=include-pk=1",
            "--option=include-transaction=false",
            "--option=include-timestamp=true",
            "--option=include-type-oids=true",
            "--option=format-version=2",
            "--option=actions=insert,update,delete,truncate",
            &format!("--slot={}", args.slot),
            "--create-slot",
            "--if-not-exists",
            "--start",
            "--no-loop",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();

    match cmd {
        Err(err) => Err(errors::Error::PgRecvLogicalError(format!("{}", err))),
        Ok(mut cmd) => {
            info!("pg_recvlogical started");
            // Reading from stdin
            let stdin = cmd.stdout.as_mut().unwrap();
            let stdin_reader = io::BufReader::new(stdin);
            let stdin_lines = stdin_reader.lines();

            // Load initial snapshot of subscriptions
            info!("Snapshot of subscriptions loading");
            let mut subscriptions = match subscription.load::<realtime::Subscription>(conn) {
                Ok(subscriptions) => subscriptions,
                Err(err) => {
                    cmd.kill().unwrap();
                    error!("Error loading subscriptions: {}", err);
                    return Err(errors::Error::Subscriptions(format!("{}", err)));
                }
            };
            info!("Snapshot of subscriptions loaded");

            // Iterate input data
            for input_line in stdin_lines {
                match input_line {
                    Ok(line) => {
                        let result_record = serde_json::from_str::<wal2json::Record>(&line);
                        match result_record {
                            Ok(wal2json_record) => {
                                // Update subscriptions if needed
                                realtime::update_subscriptions(
                                    &wal2json_record,
                                    &mut subscriptions,
                                    conn,
                                );

                                // New
                                let walrus = process_record(
                                    &wal2json_record,
                                    &subscriptions,
                                    publication,
                                    1024 * 1024,
                                    conn,
                                );

                                match walrus {
                                    Ok(rows) => {
                                        for row in rows {
                                            match serde_json::to_string(&row) {
                                                Ok(walrus_json) => {
                                                    println!("{}", walrus_json);
                                                    debug!("Pushed record for {}.{} with {} subscribers", row.wal.schema, row.wal.table, row.subscription_ids.len());
                                                }
                                                Err(err) => {
                                                    error!(
                                                        "Failed to serialize walrus result: {}",
                                                        err
                                                    )
                                                }
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        cmd.kill().unwrap();
                                        error!("WALRUS Error: {}", err);
                                        return Err(errors::Error::Walrus(format!("{}", err)));
                                    }
                                }
                            }
                            Err(err) => error!("Failed to parse: {}", err),
                        }
                    }
                    Err(err) => error!("Error: {}", err),
                }
            }
            match cmd.wait() {
                Ok(_) => Ok(()),
                Err(err) => Err(errors::Error::PgRecvLogicalError(format!("{}", err))),
            }
        }
    }
}

fn process_record<'a>(
    rec: &'a wal2json::Record,
    subscriptions: &Vec<realtime::Subscription>,
    publication: &str,
    max_record_bytes: usize,
    conn: &mut PgConnection,
) -> Result<Vec<realtime::WALRLS<'a>>, errors::Error> {
    /*
     *  Table Level Filters
     */

    // Will not be necessary after replacing wal2json
    let table_oid = crate::filters::table::table_oid::get_table_oid(rec.schema, rec.table, conn)?;

    let is_in_publication =
        filters::table::publication::is_in_publication(&rec.schema, &rec.table, publication, conn)?;

    // Subscriptions to the current entity
    let entity_subscriptions: Vec<&realtime::Subscription> = subscriptions
        .iter()
        .filter(|x| &x.schema_name == &rec.schema)
        .filter(|x| &x.table_name == &rec.table)
        .map(|x| x)
        .collect();

    let is_subscribed_to = entity_subscriptions.len() > 0;
    let is_rls_enabled = filters::table::row_level_security::is_rls_enabled(table_oid, conn)?;

    debug!(
        "Processing record: {}.{} inpub: {}, entity_subs {}, rls_on {}",
        &rec.schema, &rec.table, is_in_publication, is_subscribed_to, is_rls_enabled
    );

    let exceeds_max_size = serde_json::json!(rec).to_string().len() > max_record_bytes;
    let action = realtime::Action::from_wal2json(&rec.action);

    // If the table isn't in the publication or no one is subscribed, do no work
    if !(is_in_publication && is_subscribed_to && action != realtime::Action::TRUNCATE) {
        debug!("Early exit. Not in pub or no one listening");
        return Ok(vec![]);
    }

    // Postgres role names of subscribed users
    let subscribed_roles: Vec<&String> = entity_subscriptions
        .iter()
        .map(|x| &x.claims_role_name)
        .unique()
        .collect();

    let mut result: Vec<realtime::WALRLS> = vec![];

    // If the table has no primary key, return
    if action != realtime::Action::DELETE && !rec.has_primary_key() {
        let r = realtime::WALRLS {
            wal: realtime::Data {
                schema: rec.schema,
                table: rec.table,
                r#type: action.clone(),
                commit_timestamp: &rec.timestamp,
                columns: vec![],
                record: HashMap::new(),
                old_record: None,
            },
            is_rls_enabled,
            subscription_ids: subscriptions
                .iter()
                .map(|x| x.subscription_id.clone())
                .collect(),
            errors: vec!["Error 400: Bad Request, no primary key"],
        };
        result.push(r);
        return Ok(result);
    }

    for role in subscribed_roles {
        /*
         *  Role Level Filters
         */

        // Subscriptions to current entity + role
        let entity_role_subscriptions: Vec<&realtime::Subscription> = entity_subscriptions
            .iter()
            .filter(|x| &x.claims_role_name == role)
            .map(|x| *x)
            .collect();

        // realtime columns to output with correct type name conversion (e.g. interger -> int4)
        // and filtered to columns selectable by the current user
        let columns: Vec<realtime::Column> =
            filters::table::column_security::selectable_columns(table_oid, role, conn)?;

        let selectable_column_names: Vec<&str> = columns.iter().map(|x| x.name.as_ref()).collect();

        /*
         *  Record Level Filters
         */

        // If the role can not select any columns in the table, return
        if columns.len() == 0 {
            let r = realtime::WALRLS {
                wal: realtime::Data {
                    schema: rec.schema,
                    table: rec.table,
                    r#type: action.clone(),
                    commit_timestamp: &rec.timestamp,
                    columns: vec![],
                    record: HashMap::new(),
                    old_record: None,
                },
                is_rls_enabled,
                subscription_ids: entity_role_subscriptions
                    .iter()
                    .map(|x| x.subscription_id.clone())
                    .collect(),
                errors: vec!["Error 401: Unauthorized"],
            };
            result.push(r);
        } else {
            // Repr of columns for internal use
            let walcols: Vec<walrus::Column> = rec
                .columns
                .as_ref()
                .unwrap_or(&vec![])
                //.unwrap_or(rec.identity.as_ref().unwrap_or(&vec![]))
                .iter()
                .map(|col| walrus::Column {
                    name: col.name,
                    type_name: col.type_,
                    type_oid: col.typeoid,
                    value: col.value.clone(),
                    is_pkey: rec.pkey_cols().contains(&col.name),
                    is_selectable: selectable_column_names.contains(&col.name),
                })
                .collect();

            // Populates for realtime::Action::INSERT | realtime::Action::UPDATE
            let record_elem: HashMap<&str, serde_json::Value> = walcols
                .iter()
                // Column must be selectable by role
                .filter(|walcol| walcol.is_selectable)
                // Filter out large column values if the record exceeds maximum size
                .filter(|walcol| !exceeds_max_size || walcol.value.to_string().len() < 64)
                .map(|walcol| (walcol.name, walcol.value.clone()))
                .collect();

            // Populates for realtime::Action::UPDATE, realtime::Action::DELETE
            let old_record_elem: Option<HashMap<&str, serde_json::Value>> =
                rec.identity.as_ref().map(|cols| {
                    cols.iter()
                        .map(|col| walrus::Column {
                            name: col.name,
                            type_name: col.type_,
                            type_oid: col.typeoid,
                            value: col.value.clone(),
                            is_pkey: rec.pkey_cols().contains(&col.name),
                            is_selectable: selectable_column_names.contains(&col.name),
                        })
                        // Column must be selectable by role
                        .filter(|walcol| walcol.is_selectable)
                        // Filter out large column values if the record exceeds maximum size
                        .filter(|walcol| !exceeds_max_size || walcol.value.to_string().len() < 64)
                        .map(|walcol| (walcol.name, walcol.value.clone()))
                        .collect()
                });

            // User Defined Filters
            let mut visible_through_filters = vec![];
            let mut delegate_to_sql_filters = vec![];

            for sub in entity_role_subscriptions {
                match filters::record::user_defined::visible_through_filters(
                    &sub.filters,
                    rec.columns.as_ref().unwrap_or(&vec![]),
                ) {
                    Ok(true) => {
                        //debug!("Filters handled in rust: {:?}", &sub.filters);
                        visible_through_filters.push(sub);
                    }
                    Ok(false) => (),
                    // delegate to SQL when we can't handle the comparison in rust
                    Err(errors::FilterError::DelegateToSQL(_)) => {
                        //debug!(
                        //    "Filters delegated to SQL: {:?}. Error: {}",
                        //    &sub.filters, err
                        //);
                        delegate_to_sql_filters.push(sub);
                    }
                }
            }

            if delegate_to_sql_filters.len() > 0 {
                match filters::record::user_defined::is_visible_through_filters_sql(
                    &walcols,
                    &delegate_to_sql_filters.iter().map(|x| x.id).collect(),
                    conn,
                ) {
                    Ok(sub_ids) => {
                        for sub in delegate_to_sql_filters
                            .iter()
                            .filter(|x| sub_ids.contains(&x.id))
                        {
                            visible_through_filters.push(sub)
                        }
                    }
                    Err(err) => {
                        error!("Failed to deletegate some filters to SQL: {}", err)
                    }
                }
            }

            // Row Level Security
            let subscriptions_to_notify: Vec<&realtime::Subscription> = match (
                is_rls_enabled && visible_through_filters.len() > 0,
                vec![realtime::Action::DELETE, realtime::Action::TRUNCATE].contains(&action),
            ) {
                (false, _) | (true, true) => visible_through_filters,
                _ => {
                    match filters::record::row_level_security::is_visible_through_rls(
                        table_oid,
                        &walcols,
                        &visible_through_filters.iter().map(|x| x.id).collect(),
                        conn,
                    ) {
                        Ok(sub_ids) => visible_through_filters
                            .iter()
                            .filter(|x| sub_ids.contains(&x.id))
                            .map(|x| *x)
                            .collect(),
                        Err(err) => {
                            error!("Failed to delegate RLS to SQL: {}", err);
                            vec![]
                        }
                    }
                }
            };

            let r = realtime::WALRLS {
                wal: realtime::Data {
                    schema: rec.schema,
                    table: rec.table,
                    r#type: action.clone(),
                    commit_timestamp: &rec.timestamp,
                    columns,
                    record: record_elem,
                    old_record: old_record_elem,
                },
                is_rls_enabled,
                subscription_ids: subscriptions_to_notify
                    .iter()
                    .map(|x| x.subscription_id)
                    .unique()
                    .collect(),
                errors: match exceeds_max_size {
                    true => vec!["Error 413: Payload Too Large"],
                    false => vec![],
                },
            };
            result.push(r);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    extern crate diesel;
    use crate::models::{realtime, wal2json};
    use crate::realtime::{Subscription, UserDefinedFilter};
    use crate::sql::schema::realtime::subscription::dsl::*;
    use chrono::{TimeZone, Utc};
    use diesel::prelude::*;
    use diesel::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::collections::HashMap;
    use uuid;

    //const BOOLOID: i32 = 16;
    const INTEGER_OID: u32 = 23;
    //const INT8OID: i32 = 20;
    //const TEXTOID: i32 = 25;

    fn establish_connection() -> PgConnection {
        let database_url = "postgresql://postgres:password@localhost:5501/postgres";
        PgConnection::establish(&database_url).unwrap()
    }

    fn create_role(role: &str, conn: &mut PgConnection) {
        diesel::sql_query(format!(
            "
        DO
            $do$
            BEGIN
               IF EXISTS (
                 SELECT FROM pg_catalog.pg_roles
                   WHERE  rolname = '{}') THEN
                     RAISE NOTICE 'Role {} already exists. Skipping.';
                   ELSE
                     CREATE ROLE {};
               END IF;
             END
             $do$;",
            role, role, role
        ))
        .execute(conn)
        .unwrap();
    }

    fn create_auth_schema(conn: &mut PgConnection) {
        diesel::sql_query("create schema if not exists auth;")
            .execute(conn)
            .unwrap();

        diesel::sql_query(
            "
            create or replace function auth.uid()
                returns uuid
                language 'sql'
            AS $$
              select
                coalesce(
                    nullif(current_setting('request.jwt.claim.sub', true), ''),
                    (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub')
                )::uuid
            $$;
            ",
        )
        .execute(conn)
        .unwrap();
    }

    fn grant_all_on_schema(schema: &str, role: &str, conn: &mut PgConnection) {
        diesel::sql_query(format!("grant all on schema \"{}\" to {};", schema, role))
            .execute(conn)
            .unwrap();

        diesel::sql_query(format!(
            "grant select on all tables in schema \"{}\" to {};",
            schema, role
        ))
        .execute(conn)
        .unwrap();
    }

    fn truncate(schema: &str, table: &str, conn: &mut PgConnection) {
        diesel::sql_query(format!("truncate table \"{}\".\"{}\";", schema, table))
            .execute(conn)
            .unwrap();
    }

    fn drop_table(schema: &str, table: &str, conn: &mut PgConnection) {
        diesel::sql_query(format!(
            "drop table if exists \"{}\".\"{}\";",
            schema, table
        ))
        .execute(conn)
        .unwrap();
    }

    fn create_publication_for_all_tables(publication_name: &str, conn: &mut PgConnection) {
        diesel::sql_query(format!(
            "drop publication if exists \"{}\";",
            publication_name
        ))
        .execute(conn)
        .unwrap();

        diesel::sql_query(format!(
            "create publication \"{}\" for all tables;",
            publication_name
        ))
        .execute(conn)
        .unwrap();
    }

    #[test]
    fn test_no_one_listening() {
        let mut conn = establish_connection();

        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");

        drop_table("public", "notes1", &mut conn);
        diesel::sql_query("create table if not exists public.notes1(id int primary key);")
            .execute(&mut conn)
            .unwrap();

        let rec = wal2json::Record {
            action: wal2json::Action::I,
            schema: "public",
            table: "notes1",
            pk: Some(vec![wal2json::PrimaryKeyRef {
                name: "id",
                type_: "int4",
                typeoid: INTEGER_OID,
            }]),
            columns: Some(vec![wal2json::Column {
                name: "id",
                type_: "int4",
                typeoid: Some(INTEGER_OID),
                value: json!(1),
            }]),
            identity: None,
            timestamp: Utc::now(),
        };

        let res = crate::process_record(
            &rec,
            &vec![],
            "supabase_multiplayer",
            1024 * 1024,
            &mut conn,
        )
        .unwrap();

        assert_eq!(res, vec![]);
    }

    #[test]
    fn test_simple_insert() {
        let mut conn = establish_connection();

        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");
        create_auth_schema(&mut conn);
        truncate("realtime", "subscription", &mut conn);
        create_publication_for_all_tables("supabase_multiplayer", &mut conn);

        drop_table("public", "notes2", &mut conn);
        diesel::sql_query("create table if not exists public.notes2(id int primary key);")
            .execute(&mut conn)
            .unwrap();

        let notes_oid =
            crate::filters::table::table_oid::get_table_oid("public", "notes2", &mut conn).unwrap();

        let claim_sub = uuid::Uuid::new_v4();
        let sub_id = uuid::uuid!("37c7e506-9eca-4671-8c48-526d404660ce");

        insert_into(subscription)
            .values((
                subscription_id.eq(sub_id),
                entity.eq(notes_oid),
                claims.eq(json!({
                    "role": "postgres",
                    "email": "example@example.com",
                    "sub": claim_sub
                })),
            ))
            .execute(&mut conn)
            .unwrap();

        let subscriptions = subscription.load::<Subscription>(&mut conn).unwrap();

        let note_id: i32 = 1;

        let wal2json_json = r#"{
            "action":"I",
            "timestamp":"2022-07-07 14:52:58.092695+00",
            "schema":"public",
            "table":"notes2",
            "columns":[
                {"name":"id","type":"integer","typeoid":23,"value":1}
            ],
            "pk":[
                {"name":"id","type":"integer","typeoid":23}
            ]
        }"#;

        let rec: wal2json::Record = serde_json::from_str(wal2json_json).unwrap();

        let res = crate::process_record(
            &rec,
            &subscriptions,
            "supabase_multiplayer",
            1024 * 1024,
            &mut conn,
        )
        .unwrap();

        let expected = vec![realtime::WALRLS {
            wal: realtime::Data {
                schema: "public",
                table: "notes2",
                r#type: realtime::Action::INSERT,
                commit_timestamp: &rec.timestamp,
                columns: vec![realtime::Column {
                    name: "id".to_string(),
                    type_: "int4".to_string(),
                }],
                record: HashMap::from([("id", json!(note_id))]),
                old_record: None,
            },
            is_rls_enabled: false,
            subscription_ids: vec![sub_id], // A SUBSCRIBER EXISTS
            errors: vec![],
        }];

        assert_eq!(res, expected);
    }

    #[test]
    fn test_simple_update() {
        let mut conn = establish_connection();
        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");
        create_auth_schema(&mut conn);
        truncate("realtime", "subscription", &mut conn);
        create_publication_for_all_tables("supabase_multiplayer", &mut conn);

        drop_table("public", "notes4", &mut conn);
        diesel::sql_query("create table if not exists public.notes4(id int primary key);")
            .execute(&mut conn)
            .unwrap();

        let notes_oid =
            crate::filters::table::table_oid::get_table_oid("public", "notes4", &mut conn).unwrap();

        let claim_sub = uuid::Uuid::new_v4();
        let sub_id = uuid::uuid!("37c7e506-9eca-4671-8c48-526d404660ce");

        insert_into(subscription)
            .values((
                subscription_id.eq(sub_id),
                entity.eq(notes_oid),
                claims.eq(json!({
                    "role": "postgres",
                    "email": "example@example.com",
                    "sub": claim_sub
                })),
            ))
            .execute(&mut conn)
            .unwrap();

        let subscriptions = subscription.load::<Subscription>(&mut conn).unwrap();

        let note_id: i32 = 1;
        let old_note_id: i32 = 0;

        let wal2json_json = r#"{
            "action":"U",
            "timestamp":"2022-07-07 14:52:58.092695+00",
            "schema":"public",
            "table":"notes4",
            "columns":[
                {"name":"id","type":"integer","typeoid":23,"value":1}
            ],
            "identity":[
                {"name":"id","type":"integer","typeoid":23,"value":0}
            ],
            "pk":[
                {"name":"id","type":"integer","typeoid":23}
            ]
        }"#;

        let rec: wal2json::Record = serde_json::from_str(wal2json_json).unwrap();

        let walrus_output = crate::process_record(
            &rec,
            &subscriptions,
            "supabase_multiplayer",
            1024 * 1024,
            &mut conn,
        )
        .unwrap();

        let expected = vec![realtime::WALRLS {
            wal: realtime::Data {
                schema: "public",
                table: "notes4",
                r#type: realtime::Action::UPDATE,
                commit_timestamp: &rec.timestamp,
                columns: vec![realtime::Column {
                    name: "id".to_string(),
                    type_: "int4".to_string(),
                }],
                record: HashMap::from([("id", json!(note_id))]),
                old_record: Some(HashMap::from([("id", json!(old_note_id))])),
            },
            is_rls_enabled: false,
            subscription_ids: vec![sub_id],
            errors: vec![],
        }];

        assert_eq!(walrus_output, expected);
    }

    #[test]
    fn test_simple_delete() {
        let mut conn = establish_connection();
        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");
        create_auth_schema(&mut conn);
        truncate("realtime", "subscription", &mut conn);
        create_publication_for_all_tables("supabase_multiplayer", &mut conn);

        drop_table("public", "notes5", &mut conn);
        diesel::sql_query("create table if not exists public.notes5(id int primary key);")
            .execute(&mut conn)
            .unwrap();

        let notes_oid: u32 =
            crate::filters::table::table_oid::get_table_oid("public", "notes5", &mut conn).unwrap();

        let claim_sub = uuid::Uuid::new_v4();
        let sub_id = uuid::uuid!("37c7e506-9eca-4671-8c48-526d404660ce");

        insert_into(subscription)
            .values((
                subscription_id.eq(sub_id),
                entity.eq(notes_oid),
                claims.eq(json!({
                    "role": "postgres",
                    "email": "example@example.com",
                    "sub": claim_sub
                })),
            ))
            .execute(&mut conn)
            .unwrap();

        let subscriptions = subscription.load::<Subscription>(&mut conn).unwrap();

        let old_note_id: i32 = 0;

        let wal2json_json = r#"{
            "action":"D",
            "timestamp":"2022-07-07 14:52:58.092695+00",
            "schema":"public",
            "table":"notes5",
            "identity":[
                {"name":"id","type":"integer","typeoid":23,"value":0}
            ],
            "pk":[
                {"name":"id","type":"integer","typeoid":23}
            ]
        }"#;

        let rec: wal2json::Record = serde_json::from_str(wal2json_json).unwrap();

        let walrus_output = crate::process_record(
            &rec,
            &subscriptions,
            "supabase_multiplayer",
            1024 * 1024,
            &mut conn,
        )
        .unwrap();

        let expected = vec![realtime::WALRLS {
            wal: realtime::Data {
                schema: "public",
                table: "notes5",
                r#type: realtime::Action::DELETE,
                commit_timestamp: &rec.timestamp,
                columns: vec![realtime::Column {
                    name: "id".to_string(),
                    type_: "int4".to_string(),
                }],
                record: HashMap::new(),
                old_record: Some(HashMap::from([("id", json!(old_note_id))])),
            },
            is_rls_enabled: false,
            subscription_ids: vec![sub_id],
            errors: vec![],
        }];

        assert_eq!(walrus_output, expected);
    }

    #[test]
    fn test_error_unauthorized() {
        let mut conn = establish_connection();
        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");
        create_auth_schema(&mut conn);
        truncate("realtime", "subscription", &mut conn);
        create_publication_for_all_tables("supabase_multiplayer", &mut conn);

        drop_table("public", "notes6", &mut conn);
        diesel::sql_query("create table if not exists public.notes6(id int primary key);")
            .execute(&mut conn)
            .unwrap();

        create_role("authenticated", &mut conn);

        diesel::sql_query("create table if not exists public.notes6(id int primary key);")
            .execute(&mut conn)
            .unwrap();

        diesel::sql_query("revoke select on public.notes6 from authenticated;")
            .execute(&mut conn)
            .unwrap();

        let notes_oid: u32 =
            crate::filters::table::table_oid::get_table_oid("public", "notes6", &mut conn).unwrap();

        let claim_sub = uuid::Uuid::new_v4();
        let sub_id = uuid::uuid!("37c7e506-9eca-4671-8c48-526d404660ce");

        insert_into(subscription)
            .values((
                subscription_id.eq(sub_id),
                entity.eq(notes_oid),
                claims.eq(json!({
                    "role": "authenticated",
                    "email": "example@example.com",
                    "sub": claim_sub
                })),
            ))
            .execute(&mut conn)
            .unwrap();

        let subscriptions = subscription.load::<Subscription>(&mut conn).unwrap();

        let wal2json_json = r#"{
            "action":"D",
            "timestamp":"2022-07-07 14:52:58.092695+00",
            "schema":"public",
            "table":"notes6",
            "identity":[
                {"name":"id","type":"integer","typeoid":23,"value":0}
            ],
            "pk":[
                {"name":"id","type":"integer","typeoid":23}
            ]
        }"#;

        let rec: wal2json::Record = serde_json::from_str(wal2json_json).unwrap();

        let walrus_output = crate::process_record(
            &rec,
            &subscriptions,
            "supabase_multiplayer",
            1024 * 1024,
            &mut conn,
        )
        .unwrap();

        let expected = vec![realtime::WALRLS {
            wal: realtime::Data {
                schema: "public",
                table: "notes6",
                r#type: realtime::Action::DELETE,
                commit_timestamp: &rec.timestamp,
                columns: vec![],
                record: HashMap::new(),
                old_record: None,
            },
            is_rls_enabled: false,
            subscription_ids: vec![sub_id],
            errors: vec!["Error 401: Unauthorized"],
        }];

        assert_eq!(walrus_output, expected);
    }

    #[test]
    fn test_quoted_type_schema_and_table() {
        // TODO: Add user defined filter to make sure delegate to sql works

        let mut conn = establish_connection();
        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");
        create_auth_schema(&mut conn);
        truncate("realtime", "subscription", &mut conn);
        create_publication_for_all_tables("supabase_multiplayer", &mut conn);

        diesel::sql_query("create schema if not exists \"dEv\";")
            .execute(&mut conn)
            .unwrap();

        diesel::sql_query("drop type if exists \"dEv\".\"Color\" cascade;")
            .execute(&mut conn)
            .unwrap();

        diesel::sql_query("create type \"dEv\".\"Color\" as enum ('RED', 'YELLOW', 'GREEN');")
            .execute(&mut conn)
            .unwrap();

        drop_table("dEv", "Notes7", &mut conn);
        diesel::sql_query(
            "create table if not exists \"dEv\".\"Notes7\"(id \"dEv\".\"Color\" primary key);",
        )
        .execute(&mut conn)
        .unwrap();

        let type_oid = diesel::dsl::sql::<sql_types::Oid>(
            "select oid from pg_type where typname = 'Color' limit 1",
        )
        .get_result::<u32>(&mut conn)
        .unwrap();

        create_role("authenticated", &mut conn);
        grant_all_on_schema("dEv", "authenticated", &mut conn);

        diesel::sql_query(
            "create policy rls_note_select
            on \"dEv\".\"Notes7\"
            to authenticated
            using (true);",
        )
        .execute(&mut conn)
        .unwrap();

        diesel::sql_query("alter table \"dEv\".\"Notes7\" enable row level security;")
            .execute(&mut conn)
            .unwrap();

        let notes_oid: u32 =
            crate::filters::table::table_oid::get_table_oid("dEv", "Notes7", &mut conn).unwrap();

        let claim_sub = uuid::Uuid::new_v4();
        let sub_id = uuid::uuid!("37c7e506-9eca-4671-8c48-526d404660ce");

        insert_into(subscription)
            .values((
                subscription_id.eq(sub_id),
                entity.eq(notes_oid),
                claims.eq(json!({
                    "role": "authenticated",
                    "email": "example@example.com",
                    "sub": claim_sub
                })),
                filters.eq(vec![UserDefinedFilter {
                    column_name: "id".to_string(),
                    op: realtime::Op::Equal,
                    value: "YELLOW".to_string(),
                }]),
            ))
            .execute(&mut conn)
            .unwrap();

        let subscriptions = subscription.load::<Subscription>(&mut conn).unwrap();

        diesel::sql_query("insert into \"dEv\".\"Notes7\"(id) values ('YELLOW');")
            .execute(&mut conn)
            .unwrap();

        let wal2json_json = format!(
            r#"{{
                "action":"I",
                "timestamp":"2022-07-07 14:52:58.092695+00",
                "schema":"dEv",
                "table":"Notes7",
                "columns":[
                    {{"name":"id","type":"Color","typeoid":{type_oid},"value": "YELLOW"}}
                ],
                "pk":[
                    {{"name":"id","type":"Color","typeoid":{type_oid}}}
                ]
            }}"#
        );

        let rec: wal2json::Record = serde_json::from_str(wal2json_json.as_ref()).unwrap();

        let walrus_output = crate::process_record(
            &rec,
            &subscriptions,
            "supabase_multiplayer",
            1024 * 1024,
            &mut conn,
        )
        .unwrap();

        let expected = vec![realtime::WALRLS {
            wal: realtime::Data {
                schema: "dEv",
                table: "Notes7",
                r#type: realtime::Action::INSERT,
                commit_timestamp: &rec.timestamp,
                columns: vec![realtime::Column {
                    name: "id".to_string(),
                    type_: "Color".to_string(),
                }],
                record: HashMap::from([("id", json!("YELLOW"))]),
                old_record: None,
            },
            is_rls_enabled: true,
            subscription_ids: vec![sub_id],
            errors: vec![],
        }];

        assert_eq!(walrus_output, expected);
    }
}
