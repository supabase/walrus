use futures::StreamExt;
use postgres_protocol::message::backend::LogicalReplicationMessage::*;
use postgres_protocol::message::backend::ReplicationMessage::*;
use postgres_protocol::message::backend::{LogicalReplicationMessage, ReplicationMessage};
use std::collections::HashMap;
use std::time::Instant;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::NoTls;
use tokio_postgres::SimpleQueryMessage::Row;

mod postgres_state;

use postgres_state::PostgresState;

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

    let mut state = PostgresState {
        lsn: 0.into(),
        tables: HashMap::new(),
    };

    let mut last_keepalive = Instant::now();
    // TODO(olirice) track table defs mapping
    // TODO(olirice) send keepalive every 20 seconds

    loop {
        let msg: Option<
            Result<ReplicationMessage<LogicalReplicationMessage>, tokio_postgres::Error>,
        > = stream.next().await;

        match msg {
            Some(Ok(XLogData(xlog_data))) => {
                let state_message = state.transform(&xlog_data.data());
                if let Some(message) = state_message {
                    println!("got some state!, {:?}", message);
                }
            }
            Some(Err(_)) => panic!("unexpected replication stream error"),
            Some(Ok(PrimaryKeepAlive(_))) => {
                println!("keep alive")
            }
            Some(Ok(_)) => (),
            None => panic!("unexpected replication stream end"),
        };
    }
}
