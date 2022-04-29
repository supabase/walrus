use clap::Parser;
use diesel::dsl::sql;
use diesel::sql_types::*;
use diesel::*;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use serde::Serialize;
use serde_json;
use std::io;
use std::io::BufRead;
use uuid;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./migrations");

#[derive(Serialize)]
pub struct WalrusRecord {
    wal: serde_json::Value,
    is_rls_enabled: bool,
    subscription_ids: Vec<uuid::Uuid>,
    errors: Vec<String>,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Number of times to greet
    #[clap(short, long, default_value_t = 1)]
    count: u8,
}

fn main() {
    // Parse command line arguments
    let _args = Args::parse();

    // Connect to Postgres
    let database_url = "postgresql://oliverrice:@localhost:28814/walrus";
    let conn = &mut PgConnection::establish(&database_url).unwrap();

    // Run any pending migrations
    conn.run_pending_migrations(MIGRATIONS)
        .expect("Pending migrations failed to execute");

    // Reading from stdin
    let stdin = io::stdin();
    let stdin_reader = io::BufReader::new(stdin);
    let stdin_lines = stdin_reader.lines();

    // Iterate input data
    for input_line in stdin_lines {
        match input_line {
            Ok(line) => {
                let result_json_line = serde_json::from_str::<serde_json::Value>(&line);
                match result_json_line {
                    Ok(json_line) => {
                        let result_wal_rls_rows =
                            sql::<
                                Record<(
                                    sql_types::Jsonb,
                                    sql_types::Bool,
                                    sql_types::Array<sql_types::Uuid>,
                                    sql_types::Array<sql_types::Text>,
                                )>,
                            >("SELECT x from realtime.apply_rls(")
                            .bind::<Jsonb, _>(json_line)
                            .sql(") x")
                            .get_results::<(serde_json::Value, bool, Vec<uuid::Uuid>, Vec<String>)>(
                                conn,
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
                                            println!("Failed to serialize walrus result: {}", err)
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
