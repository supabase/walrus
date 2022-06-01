use cached::proc_macro::cached;
use cached::TimedSizedCache;
use clap::Parser;
use diesel::dsl::sql;
use diesel::sql_types::*;
use diesel::*;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use env_logger;
use log::{error, info, warn};
use serde::Serialize;
use serde_json;
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::io::BufRead;
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time;

mod realtime_fmt;
mod wal2json;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

fn run_migrations(
    connection: &mut PgConnection,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    sql_query("create schema if not exists realtime")
        .execute(connection)
        .expect("failed to create 'realtime' schema");

    sql_query("set search_path='realtime'")
        .execute(connection)
        .expect("failed to set search path");

    connection.run_pending_migrations(MIGRATIONS)?;
    Ok(())
}

#[derive(Serialize)]
pub struct WalrusRecord {
    wal: serde_json::Value,
    is_rls_enabled: bool,
    subscription_ids: Vec<uuid::Uuid>,
    errors: Vec<String>,
}

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
    run_migrations(conn).expect("Pending migrations failed to execute");
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

            // Iterate input data
            for input_line in stdin_lines {
                match input_line {
                    Ok(line) => {
                        println!("{}", line);
                        let result_record = serde_json::from_str::<wal2json::Record>(&line);
                        match result_record {
                            Ok(wal2json_record) => {
                                // New
                                let walrus = process_record(&wal2json_record, conn);

                                // Old
                                let json_line = serde_json::json!(wal2json_record);

                                let result_wal_rls_rows =
                                    sql::<
                                        Record<(
                                            sql_types::Jsonb,
                                            sql_types::Bool,
                                            sql_types::Array<sql_types::Uuid>,
                                            sql_types::Array<sql_types::Text>,
                                        )>,
                                    >(
                                        "SELECT x from realtime.apply_rls("
                                    )
                                    .bind::<Jsonb, _>(json_line)
                                    .sql(") x")
                                    .get_results::<(
                                        serde_json::Value,
                                        bool,
                                        Vec<uuid::Uuid>,
                                        Vec<String>,
                                    )>(
                                        conn
                                    );
                                match result_wal_rls_rows {
                                    Ok(rows) => {
                                        for row in rows {
                                            let walrus_rec = WalrusRecord {
                                                wal: row.0,
                                                is_rls_enabled: row.1,
                                                subscription_ids: row.2,
                                                errors: row.3,
                                            };
                                            match serde_json::to_string(&walrus_rec) {
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

fn process_record(
    rec: &wal2json::Record,
    conn: &mut PgConnection,
) -> Result<realtime_fmt::WALRLS, String> {
    let is_in_publication = is_in_publication(&rec.schema, &rec.table, "supabase_realtime", conn)?;
    let is_subscribed_to = is_subscribed_to(&rec.schema, &rec.table, conn)?;
    let is_rls_enabled = is_rls_enabled(&rec.schema, &rec.table, conn)?;
    let subscribed_roles = subscribed_roles(&rec.schema, &rec.table, conn)?;
    let pkey_cols: Vec<&String> = (&rec).pk.iter().map(|x| &x.name).collect();

    println!("Published {}", is_in_publication);
    println!("Subscribed {}", is_subscribed_to);
    println!("Secured {}", is_rls_enabled);
    println!("Subscribed Roles {}", subscribed_roles.join(", "));

    let action = match rec.action {
        wal2json::Action::I => realtime_fmt::Action::INSERT,
        wal2json::Action::U => realtime_fmt::Action::UPDATE,
        wal2json::Action::D => realtime_fmt::Action::DELETE,
        wal2json::Action::T => realtime_fmt::Action::TRUNCATE,
    };

    if action != realtime_fmt::Action::DELETE && pkey_cols.len() == 0 {
        return Ok(realtime_fmt::WALRLS {
            wal: realtime_fmt::Data {
                schema: rec.schema.to_string(),
                table: rec.table.to_string(),
                r#type: action,
                commit_timestamp: rec.timestamp.to_string(),
                columns: vec![],
                record: HashMap::new(),
                old_record: None,
            },
            is_rls_enabled,
            subscription_ids: get_subscription_ids(&rec.schema, &rec.table, conn)?,
            errors: vec!["Error 400: Bad Request, no primary key".to_string()],
        });
    }

    for role in &subscribed_roles {
        let selectable_columns = selectable_columns(&rec.schema, &rec.table, role, conn)?;
        println!("Selectable Columns {}", selectable_columns.join(", "));

        //let column_data = rec.columns.into_iter().filter(|x| selectable_columns.contains(&x.name)).collect()

        // For user in subscribed users
        //      // check column permissions
    }

    Ok(realtime_fmt::WALRLS {
        wal: realtime_fmt::Data {
            schema: rec.schema.to_string(),
            table: rec.table.to_string(),
            r#type: action,
            commit_timestamp: rec.timestamp.to_string(),
            columns: vec![],
            record: HashMap::new(),
            old_record: None,
        },
        is_rls_enabled,
        subscription_ids: vec![],
        errors: vec![],
    })
}

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
        fn is_subscribed_to(schema_name: Text, table_name: Text) -> Bool;
    }

    sql_function! {
        fn subscribed_roles(schema_name: Text, table_name: Text) -> Array<Text>;
    }

    sql_function! {
        fn selectable_columns(schema_name: Text, table_name: Text, role_name: Text) -> Array<Text>;
    }

    sql_function! {
        fn get_subscription_ids(schema_name: Text, table_name: Text) -> Array<Uuid>;
    }
}

#[cached(
    type = "TimedSizedCache<String, Result<bool, String>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(250, 1)}",
    convert = r#"{ format!("{}.{}", schema_name, table_name) }"#,
    sync_writes = true
)]
fn is_rls_enabled(
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
fn is_in_publication(
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
    type = "TimedSizedCache<String, Result<bool, String>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(250, 1)}",
    convert = r#"{ format!("{}.{}", schema_name, table_name) }"#,
    sync_writes = true
)]
fn is_subscribed_to(
    schema_name: &str,
    table_name: &str,
    conn: &mut PgConnection,
) -> Result<bool, String> {
    select(sql_functions::is_subscribed_to(schema_name, table_name))
        .first(conn)
        .map_err(|x| format!("{}", x))
}

#[cached(
    type = "TimedSizedCache<String, Result<Vec<String>, String>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(250, 1)}",
    convert = r#"{ format!("{}.{}", schema_name, table_name) }"#,
    sync_writes = true
)]
fn subscribed_roles(
    schema_name: &str,
    table_name: &str,
    conn: &mut PgConnection,
) -> Result<Vec<String>, String> {
    select(sql_functions::subscribed_roles(schema_name, table_name))
        .first(conn)
        .map_err(|x| format!("{}", x))
}

#[cached(
    type = "TimedSizedCache<String, Result<Vec<String>, String>>",
    create = "{ TimedSizedCache::with_size_and_lifespan(250, 1)}",
    convert = r#"{ format!("{}.{}-{}", schema_name, table_name, role_name) }"#,
    sync_writes = true
)]
fn selectable_columns(
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

fn get_subscription_ids(
    schema_name: &str,
    table_name: &str,
    conn: &mut PgConnection,
) -> Result<Vec<uuid::Uuid>, String> {
    select(sql_functions::get_subscription_ids(schema_name, table_name))
        .first(conn)
        .map_err(|x| format!("{}", x))
}
