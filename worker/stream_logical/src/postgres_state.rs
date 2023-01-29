use postgres_protocol::message::backend;
use postgres_protocol::message::backend::LogicalReplicationMessage;
use std::collections::HashMap;
use tokio_postgres::types::PgLsn;

use postgres_types::Type;

/// Describes the REPLICA IDENTITY setting of a table
#[derive(Debug)]
pub enum ReplicaIdentity {
    /// default selection for replica identity (primary key or nothing)
    Default,
    /// no replica identity is logged for this relation
    Nothing,
    /// all columns are logged as replica identity
    Full,
    /// An explicitly chosen candidate key's columns are used as replica identity.
    /// Note this will still be set if the index has been dropped; in that case it
    /// has the same meaning as 'd'.
    Index,
}

impl From<&backend::ReplicaIdentity> for ReplicaIdentity {
    fn from(value: &backend::ReplicaIdentity) -> Self {
        match value {
            Full => Self::Full,
            _ => Self::Index,
            // TODO
        }
    }
}

/// Describes a table in a PostgreSQL database.
#[derive(Debug)]
pub struct PostgresTableDesc {
    /// The OID of the table.
    oid: u32,
    /// The name of the schema that the table belongs to.
    namespace: String,
    /// The name of the table.
    name: String,
    /// replica identity identifier
    replica_identity: ReplicaIdentity,
    /// The description of each column, in order.
    columns: HashMap<u32, Vec<PostgresColumnDesc>>,
}

/// Describes a column in a [`PostgresTableDesc`].
#[derive(Debug)]
pub struct PostgresColumnDesc {
    /// The name of the column.
    name: String,
    /// The OID of the column's type.
    type_oid: u32,
    /// The modifier for the column's type.
    type_mod: i32,
    /// True if the column lacks a `NOT NULL` constraint.
    nullable: bool,
    /// Whether the column is part of the table's primary key.
    /// TODO The order of the columns in the primary key matters too.
    primary_key: bool,
}

#[derive(Debug)]
pub struct PostgresState {
    /// Our cursor into the WAL
    pub lsn: PgLsn,
    pub tables: HashMap<u32, PostgresTableDesc>,
}

#[derive(Debug)]
pub enum DataChange {
    Insert,
    Update,
    Delete,
    Truncate,
}

enum PostgresStateError {
    UnknownType(u32),
    UnknownTable(u32),
}

impl PostgresState {
    /// Processes a logical replication message, update the internal state as needed
    /// and outputs an "enriched" change data capture message with some schema data
    pub fn transform(
        &mut self,
        message: &LogicalReplicationMessage,
    ) -> Result<Option<DataChange>, PostgresStateError> {
        match message {
            LogicalReplicationMessage::Relation(relation) => {
                // NOT SAFE
                let namespace: String = relation.namespace().unwrap().into();
                let name: String = relation.name().unwrap().into();
                let replica_identity = ReplicaIdentity::from(relation.replica_identity());

                let mut columns = HashMap::new();

                for col in relation.columns() {
                    let type_oid = col.type_id();
                    let type_modifier = col.type_modifier();
                }

                self.tables.insert(
                    relation.rel_id(),
                    PostgresTableDesc {
                        oid: relation.rel_id(),
                        namespace,
                        name,
                        replica_identity,
                        columns: HashMap::new(),
                    },
                );

                println!("{:?}", self);
                Ok(None)
            }
            LogicalReplicationMessage::Type(type_) => {
                println!("{:?}", type_);
                Ok(None)
            }
            LogicalReplicationMessage::Insert(insert) => {
                println!("{:?}", insert);
                Ok(Some(DataChange::Insert))
            }
            LogicalReplicationMessage::Update(update) => {
                println!("{:?}", update);
                Ok(Some(DataChange::Update))
            }
            LogicalReplicationMessage::Delete(delete) => {
                println!("{:?}", delete);
                Ok(Some(DataChange::Delete))
            }
            LogicalReplicationMessage::Truncate(truncate) => {
                println!("{:?}", truncate);
                Ok(Some(DataChange::Truncate))
            }
            LogicalReplicationMessage::Commit(commit) => {
                println!("{:?}", commit);
                self.lsn = commit.commit_lsn().into();
                Ok(None)
            }
            LogicalReplicationMessage::Begin(begin) => {
                println!("{:?}", begin);
                Ok(None)
            }
            LogicalReplicationMessage::Origin(origin) => {
                println!("{:?}", origin);
                Ok(None)
            }
            _ => {
                println!("unknown logical replication message type");
                Ok(None)
            }
        }
    }
}
