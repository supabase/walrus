#[macro_use]
extern crate diesel;
use clap::Parser;
use diesel::prelude::*;
use diesel::*;
use env_logger;
use itertools::Itertools;
use log::{debug, error, info, warn};
use serde_json;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time;

mod filters;
mod migrations;
mod realtime_fmt;
mod schema;
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
    migrations::run_migrations(conn).expect("Pending migrations failed to execute");
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
            use schema::realtime::subscription::dsl::*;
            let mut subscriptions = match subscription.load::<realtime_fmt::Subscription>(conn) {
                Ok(subscriptions) => subscriptions,
                Err(err) => {
                    cmd.kill().unwrap();
                    error!("Error loading subscriptions: {}", err);
                    return Err("Error loading subscriptions".to_string());
                }
            };
            info!("Snapshot of subscriptions loaded");

            //println!("subs {:?}", subscriptions);

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

fn pkey_cols(rec: &wal2json::Record) -> Vec<&String> {
    match &rec.pk {
        Some(pkey_refs) => pkey_refs.iter().map(|x| &x.name).collect(),
        None => vec![],
    }
}

fn has_primary_key(rec: &wal2json::Record) -> bool {
    pkey_cols(rec).len() != 0
}

fn process_record(
    rec: &wal2json::Record,
    subscriptions: &Vec<realtime_fmt::Subscription>,
    publication: &str,
    max_record_bytes: usize,
    conn: &mut PgConnection,
) -> Result<Vec<realtime_fmt::WALRLS>, String> {
    let is_in_publication =
        sql_functions::is_in_publication(&rec.schema, &rec.table, publication, conn)?;

    let load_subscriptions: Vec<&realtime_fmt::Subscription> = subscriptions
        .iter()
        .filter(|x| &x.schema_name == "load_messages")
        .map(|x| x)
        .collect();

    debug!("N load subs {}", &load_subscriptions.len(),);

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
                schema: rec.schema.to_string(),
                table: rec.table.to_string(),
                r#type: action.clone(),
                commit_timestamp: rec.timestamp,
                columns: vec![],
                record: HashMap::new(),
                old_record: None,
            },
            is_rls_enabled,
            subscription_ids: subscriptions
                .iter()
                .map(|x| x.subscription_id.clone())
                .collect(),
            errors: vec!["Error 400: Bad Request, no primary key".to_string()],
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
            .filter(|col| selectable_columns.contains(&col.name))
            .map(|w2j_col| realtime_fmt::Column {
                name: w2j_col.name.to_string(),
                type_: w2j_col.type_.to_string(),
            })
            .collect();

        let mut record_elem = HashMap::new();
        let mut old_record_elem = None;
        let mut old_record_elem_content = HashMap::new();

        // If the role can not select any columns in the table, return
        if action != realtime_fmt::Action::DELETE && selectable_columns.len() == 0 {
            let r = realtime_fmt::WALRLS {
                wal: realtime_fmt::Data {
                    schema: rec.schema.to_string(),
                    table: rec.table.to_string(),
                    r#type: action.clone(),
                    commit_timestamp: rec.timestamp,
                    columns,
                    record: HashMap::new(),
                    old_record: None,
                },
                is_rls_enabled,
                subscription_ids: entity_role_subscriptions
                    .iter()
                    .map(|x| x.subscription_id.clone())
                    .collect(),
                errors: vec!["Error 401: Unauthorized".to_string()],
            };
            result.push(r);
        } else {
            if vec![realtime_fmt::Action::INSERT, realtime_fmt::Action::UPDATE].contains(&action) {
                for col_name in &selectable_columns {
                    'record: for col in rec.columns.as_ref().unwrap_or(&vec![]) {
                        if col_name == &col.name {
                            if !exceeds_max_size || col.value.to_string().len() < 64 {
                                record_elem.insert(col_name.to_string(), col.value.clone());
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
                                if col_name == &col.name {
                                    if !exceeds_max_size || col.value.to_string().len() < 64 {
                                        old_record_elem_content
                                            .insert(col_name.to_string(), col.value.clone());
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
                        name: col.name.to_string(),
                        type_name: col.type_.to_string(),
                        type_oid: col.typeoid.clone(),
                        value: col.value.clone(),
                        is_pkey: pkey_cols(rec).contains(&&col.name),
                        is_selectable: false, // stub: unused,
                    }
                })
                .collect();

            // User Defined Filters
            let mut subscription_id_is_visible_through_filters = vec![];
            let mut subscription_id_delegate_to_sql = vec![];

            for sub in entity_role_subscriptions {
                match filters::visible_through_filters(
                    &sub.filters,
                    rec.columns.as_ref().unwrap_or(&vec![]),
                ) {
                    Ok(true) => {
                        //debug!("Filters handled in rust: {:?}", &sub.filters);
                        subscription_id_is_visible_through_filters.push(sub.subscription_id);
                    }
                    Ok(false) => (),
                    // delegate to SQL when we can't handle the comparison in rust
                    Err(_) => {
                        //debug!(
                        //    "Filters delegated to SQL: {:?}. Error: {}",
                        //    &sub.filters, err
                        //);
                        subscription_id_delegate_to_sql.push(sub.subscription_id);
                    }
                }
            }

            if subscription_id_delegate_to_sql.len() > 0 {
                match sql_functions::is_visible_through_filters(
                    &walcols,
                    &subscription_id_delegate_to_sql,
                    conn,
                ) {
                    Ok(sub_ids) => subscription_id_is_visible_through_filters.extend(&sub_ids),
                    Err(err) => {
                        error!("Failed to deletegate some filters to SQL: {}", err)
                    }
                }
            }

            // Row Level Security
            let subscription_ids_to_notify = match is_rls_enabled
                && subscription_id_is_visible_through_filters.len() > 0
                && !vec![realtime_fmt::Action::DELETE, realtime_fmt::Action::TRUNCATE]
                    .contains(&action)
            {
                false => subscription_id_is_visible_through_filters,
                true => {
                    match sql_functions::is_visible_through_rls(
                        &rec.schema,
                        &rec.table,
                        &walcols,
                        &subscription_id_is_visible_through_filters,
                        conn,
                    ) {
                        Ok(sub_ids) => sub_ids,
                        Err(err) => {
                            error!("Failed to delegate RLS to SQL: {}", err);
                            vec![]
                        }
                    }
                }
            };

            let r = realtime_fmt::WALRLS {
                wal: realtime_fmt::Data {
                    schema: rec.schema.to_string(),
                    table: rec.table.to_string(),
                    r#type: action.clone(),
                    commit_timestamp: rec.timestamp,
                    columns,
                    record: record_elem,
                    old_record: old_record_elem,
                },
                is_rls_enabled,
                subscription_ids: subscription_ids_to_notify,
                errors: match exceeds_max_size {
                    true => vec!["Error 413: Payload Too Large".to_string()],
                    false => vec![],
                },
            };
            result.push(r);
        }
    }

    Ok(result)
}
