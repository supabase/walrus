use clap::Parser;
use diesel::*;
use env_logger;
use itertools::Itertools;
use log::{error, info, warn};
use serde_json;
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time;

mod filters;
mod migrations;
mod realtime_fmt;
mod sql_functions;
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

    let conn = match conn_result {
        Ok(c) => c,
        Err(_) => {
            return Err("failed to make postgres connection".to_string());
        }
    };

    // Run pending migrations
    migrations::run_migrations(conn).expect("Pending migrations failed to execute");
    info!("Postgres connection established");

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
            "--option=actions=insert,update,delete",
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
            let mut subscriptions = sql_functions::get_subscriptions(conn)?;

            // Iterate input data
            for input_line in stdin_lines {
                match input_line {
                    Ok(line) => {
                        let result_record = serde_json::from_str::<wal2json::Record>(&line);
                        match result_record {
                            Ok(wal2json_record) => {
                                // Update subscriptions if needed
                                update_subscriptions(&wal2json_record, &mut subscriptions);

                                // New
                                let walrus = process_record(
                                    &wal2json_record,
                                    &subscriptions,
                                    1024 * 1024,
                                    conn,
                                );

                                match walrus {
                                    Ok(rows) => {
                                        for row in rows {
                                            match serde_json::to_string(&row) {
                                                Ok(walrus_json) => println!("{}", walrus_json),
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

/// Checks to see if the new record is a change to realtime.subscriptions
/// and updates the subscriptions variable if a change is detected
fn update_subscriptions(
    rec: &wal2json::Record,
    mut subscriptions: &mut Vec<realtime_fmt::Subscription>,
) -> () {
    // If the record is a new subscription. Handle it and return
    if rec.schema == "realtime" && rec.table == "subscription" {
        //TODO manage the subscriptions vector from the WAL stream
        match rec.action {
            wal2json::Action::I => {
                /*

                realtime_fmt::Subscription{
                    schema_name: rec.schema.to_string(),
                    table_name: rec.table_name.to_string(),
                    subscription_id:
                    filters:
                    claims_role:
                }
                */
            }
            wal2json::Action::U => {
                panic!("subscriptions should not be updated");
            }
            wal2json::Action::D => {}
            wal2json::Action::T => {
                subscriptions.clear();
            }
        }
    }
}

fn pkey_cols(rec: &wal2json::Record) -> Vec<&String> {
    rec.pk.iter().map(|x| &x.name).collect()
}

fn has_primary_key(rec: &wal2json::Record) -> bool {
    pkey_cols(rec).len() != 0
}

fn process_record(
    rec: &wal2json::Record,
    subscriptions: &Vec<realtime_fmt::Subscription>,
    max_record_bytes: usize,
    conn: &mut PgConnection,
) -> Result<Vec<realtime_fmt::WALRLS>, String> {
    let is_in_publication =
        sql_functions::is_in_publication(&rec.schema, &rec.table, "supabase_realtime", conn)?;
    let is_subscribed_to = subscriptions.len() > 0;
    let is_rls_enabled = sql_functions::is_rls_enabled(&rec.schema, &rec.table, conn)?;
    let exceeds_max_size = serde_json::json!(rec).to_string().len() > max_record_bytes;

    // Subscriptions to the current entity
    let entity_subscriptions: Vec<&realtime_fmt::Subscription> = subscriptions
        .iter()
        .filter(|x| &x.schema_name == &rec.schema)
        .filter(|x| &x.table_name == &rec.table)
        .map(|x| x)
        .collect();

    let subscribed_roles: Vec<&String> = entity_subscriptions
        .iter()
        .filter(|x| &x.schema_name == &rec.schema)
        .filter(|x| &x.table_name == &rec.table)
        .map(|x| &x.claims_role)
        .unique()
        .collect();

    let mut result: Vec<realtime_fmt::WALRLS> = vec![];

    // If the table isn't in the publication or no one is subscribed, do no work
    if !(is_in_publication & is_subscribed_to) {
        return Ok(vec![]);
    }

    let action = match rec.action {
        wal2json::Action::I => realtime_fmt::Action::INSERT,
        wal2json::Action::U => realtime_fmt::Action::UPDATE,
        wal2json::Action::D => realtime_fmt::Action::DELETE,
        wal2json::Action::T => realtime_fmt::Action::TRUNCATE,
    };

    // If the table has no primary key, return
    if action != realtime_fmt::Action::DELETE && !has_primary_key(rec) {
        let r = realtime_fmt::WALRLS {
            wal: realtime_fmt::Data {
                schema: rec.schema.to_string(),
                table: rec.table.to_string(),
                r#type: action.clone(),
                commit_timestamp: rec.timestamp.to_string(),
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
            .filter(|x| &x.claims_role == role)
            .map(|x| *x)
            .collect();

        let selectable_columns =
            sql_functions::selectable_columns(&rec.schema, &rec.table, role, conn)?;

        let columns = rec
            .columns
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
                    commit_timestamp: rec.timestamp.to_string(),
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
                    'record: for col in &rec.columns {
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

            for sub in entity_role_subscriptions {
                match filters::visible_through_filters(&sub.filters, &rec.columns) {
                    Ok(true) => {
                        subscription_id_is_visible_through_filters.push(sub.subscription_id)
                    }
                    Ok(false) => (),
                    // delegate to SQL when we can't handle the comparison in rust
                    Err(_) => {
                        match sql_functions::is_visible_through_filters(
                            &walcols,
                            &sub.filters,
                            conn,
                        ) {
                            Ok(true) => {
                                subscription_id_is_visible_through_filters.push(sub.subscription_id)
                            }
                            Ok(false) => (),
                            Err(_) => {
                                panic!("error from sql during filter");
                            }
                        };
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
                            println!("error from sql during RLS {}", err);
                            panic!("rls");
                        }
                    }
                }
            };

            let r = realtime_fmt::WALRLS {
                wal: realtime_fmt::Data {
                    schema: rec.schema.to_string(),
                    table: rec.table.to_string(),
                    r#type: action.clone(),
                    commit_timestamp: rec.timestamp.to_string(),
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
