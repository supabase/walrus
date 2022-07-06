#[macro_use]
extern crate diesel;
use clap::Parser;
use diesel::prelude::*;
use diesel::*;
use env_logger;
use itertools::Itertools;
use log::{debug, error, info, warn};
use serde_json;
use sql::schema::realtime::subscription::dsl::*;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time;

mod filters;
mod realtime_fmt;
mod sql;
mod sql_functions;
mod timestamp_fmt;
mod wal2json;
mod walrus_fmt;

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
        info!("Stream interrupted. Restarting pg_recvlogical in 5 seconds");
        sleep(time::Duration::from_secs(5));
    }
}

fn run(args: &Args) -> Result<(), String> {
    // Connect to Postgres
    let conn_result = &mut PgConnection::establish(&args.connection);
    let publication = &args.publication;

    let conn = match conn_result {
        Ok(c) => c,
        Err(_) => {
            return Err("failed to make postgres connection".to_string());
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
        Err(err) => Err(format!("{}", err)),
        Ok(mut cmd) => {
            info!("pg_recvlogical started");
            // Reading from stdin
            let stdin = cmd.stdout.as_mut().unwrap();
            let stdin_reader = io::BufReader::new(stdin);
            let stdin_lines = stdin_reader.lines();

            // Load initial snapshot of subscriptions
            info!("Snapshot of subscriptions loading");
            let mut subscriptions = match subscription.load::<realtime_fmt::Subscription>(conn) {
                Ok(subscriptions) => subscriptions,
                Err(err) => {
                    cmd.kill().unwrap();
                    error!("Error loading subscriptions: {}", err);
                    return Err("Error loading subscriptions".to_string());
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
                                //println!("rec {:?}", wal2json_record);
                                // Update subscriptions if needed
                                realtime_fmt::update_subscriptions(
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
                                        return Err("walrus error".to_string());
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
                Err(err) => Err(format!("{}", err)),
            }
        }
    }
}

fn pkey_cols<'a>(rec: &'a wal2json::Record) -> Vec<&'a str> {
    match &rec.pk {
        Some(pkey_refs) => pkey_refs.iter().map(|x| x.name).collect(),
        None => vec![],
    }
}

fn has_primary_key(rec: &wal2json::Record) -> bool {
    pkey_cols(rec).len() != 0
}

fn process_record<'a>(
    rec: &'a wal2json::Record,
    subscriptions: &Vec<realtime_fmt::Subscription>,
    publication: &str,
    max_record_bytes: usize,
    conn: &mut PgConnection,
) -> Result<Vec<realtime_fmt::WALRLS<'a>>, String> {
    let is_in_publication =
        sql_functions::is_in_publication(&rec.schema, &rec.table, publication, conn)?;

    // Subscriptions to the current entity
    let entity_subscriptions: Vec<&realtime_fmt::Subscription> = subscriptions
        .iter()
        .filter(|x| &x.schema_name == &rec.schema)
        .filter(|x| &x.table_name == &rec.table)
        .map(|x| x)
        .collect();

    if subscriptions.len() > 0 {
        debug!(
            "Rec {} {} table {} {}",
            &rec.schema,
            &rec.table,
            &subscriptions.first().unwrap().schema_name,
            &subscriptions.first().unwrap().table_name,
        );
    }

    let is_subscribed_to = entity_subscriptions.len() > 0;
    let is_rls_enabled = sql_functions::is_rls_enabled(&rec.schema, &rec.table, conn)?;

    debug!(
        "Processing record: {}.{} inpub: {}, entity_subs {}, rls_on {}",
        &rec.schema, &rec.table, is_in_publication, is_subscribed_to, is_rls_enabled
    );

    let exceeds_max_size = serde_json::json!(rec).to_string().len() > max_record_bytes;

    let action = match rec.action {
        wal2json::Action::I => realtime_fmt::Action::INSERT,
        wal2json::Action::U => realtime_fmt::Action::UPDATE,
        wal2json::Action::D => realtime_fmt::Action::DELETE,
        wal2json::Action::T => realtime_fmt::Action::TRUNCATE,
    };

    // If the table isn't in the publication or no one is subscribed, do no work
    if !(is_in_publication && is_subscribed_to && action != realtime_fmt::Action::TRUNCATE) {
        debug!("Early exit. Not in pub or no one listening");
        return Ok(vec![]);
    }

    let subscribed_roles: Vec<&String> = entity_subscriptions
        .iter()
        .filter(|x| &x.schema_name == &rec.schema)
        .filter(|x| &x.table_name == &rec.table)
        .map(|x| &x.claims_role_name)
        .unique()
        .collect();

    let mut result: Vec<realtime_fmt::WALRLS> = vec![];

    // If the table has no primary key, return
    if action != realtime_fmt::Action::DELETE && !has_primary_key(rec) {
        let r = realtime_fmt::WALRLS {
            wal: realtime_fmt::Data {
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
        // Subscriptions to current entity + role
        let entity_role_subscriptions: Vec<&realtime_fmt::Subscription> = entity_subscriptions
            .iter()
            .filter(|x| &x.claims_role_name == role)
            .map(|x| *x)
            .collect();

        let selectable_columns =
            sql_functions::selectable_columns(&rec.schema, &rec.table, role, conn)?;

        let columns = rec
            .columns
            .as_ref()
            .unwrap_or(&vec![])
            .iter()
            .filter(|col| selectable_columns.contains(&col.name.to_string()))
            .map(|w2j_col| realtime_fmt::Column {
                name: w2j_col.name,
                type_: w2j_col.type_,
            })
            .collect();

        let mut record_elem = HashMap::new();
        let mut old_record_elem = None;
        let mut old_record_elem_content = HashMap::new();

        // If the role can not select any columns in the table, return
        if action != realtime_fmt::Action::DELETE && selectable_columns.len() == 0 {
            let r = realtime_fmt::WALRLS {
                wal: realtime_fmt::Data {
                    schema: rec.schema,
                    table: rec.table,
                    r#type: action.clone(),
                    commit_timestamp: &rec.timestamp,
                    columns,
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
            if vec![realtime_fmt::Action::INSERT, realtime_fmt::Action::UPDATE].contains(&action) {
                for col_name in &selectable_columns {
                    'record: for col in rec.columns.as_ref().unwrap_or(&vec![]) {
                        if col_name == col.name {
                            if !exceeds_max_size || col.value.to_string().len() < 64 {
                                record_elem.insert(col.name, col.value.clone());
                                break 'record;
                            }
                        }
                    }
                }
            }

            if vec![realtime_fmt::Action::UPDATE, realtime_fmt::Action::DELETE].contains(&action) {
                for col_name in &selectable_columns {
                    match &rec.identity {
                        Some(identity) => {
                            'old_record: for col in identity {
                                if col_name == col.name {
                                    if !exceeds_max_size || col.value.to_string().len() < 64 {
                                        old_record_elem_content.insert(col.name, col.value.clone());
                                        break 'old_record;
                                    }
                                }
                            }
                        }
                        None => (),
                    }
                }
                old_record_elem = Some(old_record_elem_content);
            }

            let walcols: Vec<walrus_fmt::WALColumn> = rec
                .columns
                .as_ref()
                .unwrap_or(&vec![])
                .iter()
                .map(|col| {
                    walrus_fmt::WALColumn {
                        name: col.name,
                        type_name: col.type_,
                        type_oid: col.typeoid,
                        value: col.value.clone(),
                        is_pkey: pkey_cols(rec).contains(&&col.name),
                        is_selectable: false, // stub: unused,
                    }
                })
                .collect();

            // User Defined Filters
            let mut visible_through_filters = vec![];
            let mut delegate_to_sql_filters = vec![];

            for sub in entity_role_subscriptions {
                match filters::visible_through_filters(
                    &sub.filters,
                    rec.columns.as_ref().unwrap_or(&vec![]),
                ) {
                    Ok(true) => {
                        //debug!("Filters handled in rust: {:?}", &sub.filters);
                        visible_through_filters.push(sub);
                    }
                    Ok(false) => (),
                    // delegate to SQL when we can't handle the comparison in rust
                    Err(_) => {
                        //debug!(
                        //    "Filters delegated to SQL: {:?}. Error: {}",
                        //    &sub.filters, err
                        //);
                        delegate_to_sql_filters.push(sub);
                    }
                }
            }

            if delegate_to_sql_filters.len() > 0 {
                match sql_functions::is_visible_through_filters(
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
            let subscriptions_to_notify: Vec<&realtime_fmt::Subscription> = match is_rls_enabled
                && visible_through_filters.len() > 0
                && !vec![realtime_fmt::Action::DELETE, realtime_fmt::Action::TRUNCATE]
                    .contains(&action)
            {
                false => visible_through_filters,
                true => {
                    match sql_functions::is_visible_through_rls(
                        &rec.schema,
                        &rec.table,
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

            let r = realtime_fmt::WALRLS {
                wal: realtime_fmt::Data {
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
    use crate::realtime_fmt::Subscription;
    use crate::sql::schema::realtime::subscription::dsl::*;
    use crate::wal2json;
    use chrono::Utc;
    use diesel::prelude::*;
    use diesel::*;
    use serde_json::json;
    use uuid;

    const BOOLOID: i32 = 16;
    const INT4OID: u32 = 23;
    const INT8OID: i32 = 20;
    const TEXTOID: i32 = 25;

    fn establish_connection() -> PgConnection {
        let database_url = "postgresql://postgres:password@localhost:5501/postgres";
        PgConnection::establish(&database_url).unwrap()
    }

    fn clean(conn: &mut PgConnection) {
        delete(subscription).execute(conn).unwrap();
    }

    #[test]
    fn test_basic() {
        let mut conn = establish_connection();

        let claim_sub = uuid::Uuid::new_v4();

        insert_into(subscription)
            .values((
                subscription_id.eq(uuid::Uuid::new_v4()),
                entity.eq(16487),
                claims.eq(json!({
                    "role": "postgres",
                    "email": "example@example.com",
                    "sub": claim_sub
                })),
            ))
            .execute(&mut conn)
            .unwrap();

        let subscriptions = subscription.load::<Subscription>(&mut conn).unwrap();
        clean(&mut conn);

        assert_eq!(subscriptions.len(), 1);
    }

    #[test]
    fn test_no_one_listening() {
        let mut conn = establish_connection();

        let rec = wal2json::Record {
            action: wal2json::Action::I,
            schema: "public",
            table: "notes",
            pk: Some(vec![wal2json::PrimaryKeyRef {
                name: "id",
                type_: "int4",
                typeoid: INT4OID,
            }]),
            columns: Some(vec![wal2json::Column {
                name: "id",
                type_: "int4",
                typeoid: Some(INT4OID),
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
}
