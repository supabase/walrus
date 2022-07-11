use futures::StreamExt;
use postgres_protocol::message::backend::LogicalReplicationMessage::*;
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::NoTls;
use tokio_postgres::SimpleQueryMessage::Row;

/// Describes a table in a PostgreSQL database.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresTableDesc {
    /// The OID of the table.
    pub oid: u32,
    /// The name of the schema that the table belongs to.
    pub namespace: String,
    /// The name of the table.
    pub name: String,
    /// The description of each column, in order.
    pub columns: Vec<PostgresColumnDesc>,
}

/// Describes a column in a [`PostgresTableDesc`].
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PostgresColumnDesc {
    /// The name of the column.
    pub name: String,
    /// The OID of the column's type.
    pub type_oid: u32,
    /// The modifier for the column's type.
    pub type_mod: i32,
    /// True if the column lacks a `NOT NULL` constraint.
    pub nullable: bool,
    /// Whether the column is part of the table's primary key.
    /// TODO The order of the columns in the primary key matters too.
    pub primary_key: bool,
}

struct PostgresTaskInfo {
    /// Our cursor into the WAL
    lsn: PgLsn,
    source_tables: HashMap<u32, PostgresTableDesc>,
}

#[tokio::main]
async fn main() {
    let conninfo = "host=127.0.0.1 port=5501 user=postgres password=password replication=database";
    let (client, connection) = tokio_postgres::connect(conninfo, NoTls).await.unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    client
        .simple_query("DROP TABLE IF EXISTS test_logical_replication")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE test_logical_replication(i int)")
        .await
        .unwrap();
    let res = client
        .simple_query("SELECT 'test_logical_replication'::regclass::oid")
        .await
        .unwrap();

    let rel_id: u32 = if let Row(row) = &res[0] {
        row.get("oid").unwrap().parse().unwrap()
    } else {
        panic!("unexpeced query message");
    };

    client
        .simple_query("DROP PUBLICATION IF EXISTS test_pub")
        .await
        .unwrap();
    client
        .simple_query("CREATE PUBLICATION test_pub FOR ALL TABLES")
        .await
        .unwrap();

    let slot = "test_logical_slot";

    let query = format!(
        r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL "pgoutput""#,
        slot
    );
    let slot_query = client.simple_query(&query).await.unwrap();
    let lsn = if let Row(row) = &slot_query[0] {
        row.get("consistent_point").unwrap()
    } else {
        panic!("unexpeced query message");
    };

    println!("lsn {}", lsn);

    // issue a query that will appear in the slot's stream since it happened after its creation
    client
        .simple_query("INSERT INTO test_logical_replication VALUES (42)")
        .await
        .unwrap();

    let options = r#"("proto_version" '1', "publication_names" 'test_pub')"#;
    let query = format!(
        r#"START_REPLICATION SLOT {:?} LOGICAL {} {}"#,
        slot, lsn, options
    );
    let copy_stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();

    let stream = LogicalReplicationStream::new(copy_stream);
    tokio::pin!(stream);

    let task = PostgresTaskInfo {
        lsn: 0.into(),
        source_tables: HashMap::new(),
    };
    let mut last_keepalive = Instant::now();
    let mut inserts: Vec<u32> = vec![];
    let mut deletes: Vec<u32> = vec![];
    let mut lsn: u64;

    // TODO(olirice) track table defs mapping
    // TODO(olirice) send keepalive every 20 seconds

    loop {
        let msg: Option<
            Result<ReplicationMessage<LogicalReplicationMessage>, tokio_postgres::Error>,
        > = stream.next().await;

        let msg_res = match msg {
            Some(Ok(XLogData(xlog_data))) => match xlog_data.data() {
                Begin(begin) => {
                    println!("{:?}", begin)
                }
                Insert(insert) => {
                    println!("{:?}", insert)
                }
                Update(update) => {
                    println!("{:?}", update)
                }
                Delete(delete) => {
                    println!("{:?}", delete)
                }
                Commit(commit) => {
                    println!("{:?}", commit);
                    lsn = commit.commit_lsn();
                }
                Relation(relation) => {
                    println!("{:?}", relation)
                }
                Origin(_) | Type(_) => {}
                Truncate(truncate) => {
                    println!("{:?}", truncate)
                }
                _ => println!("unknown logical replication message type"),
            },
            Some(Err(_)) => panic!("unexpected replication stream error"),
            None => panic!("unexpected replication stream end"),
            Some(Ok(PrimaryKeepAlive(_))) => {
                println!("keep alive")
            }
            Some(Ok(_)) => (),
            _ => println!("Unexpected replication message"),
        };
    }
}
