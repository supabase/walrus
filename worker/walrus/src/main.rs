use clap::Parser;
use diesel::dsl::sql;
use diesel::sql_types::*;
use diesel::*;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use serde::Serialize;
use serde_json;
use std::error::Error;
use std::io;
use std::io::BufRead;
use std::process::{Command, Stdio};
use uuid;

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

    let mut cmd = Command::new("pg_recvlogical")
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
        ])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    {
        // Connect to Postgres
        // "postgresql://oliverrice:@localhost:28814/walrus";
        let conn = &mut PgConnection::establish(&args.connection).unwrap();

        // Run any pending migrations
        run_migrations(conn).expect("Pending migrations failed to execute");

        // Reading from stdin
        let stdin = cmd.stdout.as_mut().unwrap();
        let stdin_reader = io::BufReader::new(stdin);
        let stdin_lines = stdin_reader.lines();

        // Iterate input data
        for input_line in stdin_lines {
            match input_line {
                Ok(line) => {
                    let result_json_line = serde_json::from_str::<serde_json::Value>(&line);
                    match result_json_line {
                        Ok(json_line) => {
                            let result_wal_rls_rows = sql::<
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
                            )>(conn);
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
                                                println!(
                                                    "Failed to serialize walrus result: {}",
                                                    err
                                                )
                                            }
                                        }
                                    }
                                }
                                Err(err) => println!("WALRUS Error: {}", err),
                            }
                        }
                        Err(err) => println!("Failed to parse: {}", err),
                    }
                }
                Err(err) => println!("Error: {}", err),
            }
        }
    }

    cmd.wait().unwrap();
}
