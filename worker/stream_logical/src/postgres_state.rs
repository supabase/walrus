use postgres_protocol::message::backend;
use postgres_protocol::message::backend::{LogicalReplicationMessage, TupleData};
use serde::Serialize;
use std::collections::HashMap;
use std::str;
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

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
pub enum Action {
    INSERT,
    UPDATE,
    DELETE,
    TRUNCATE,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
pub struct PostgresRecord {
    pub schema: String,
    pub table: String,
    pub r#type: Action,
    //#[serde(with = "crate::timestamp_fmt")]
    //pub commit_timestamp: &'a DateTime<Utc>,
    //pub columns: Vec<Column>,
    pub record: HashMap<String, serde_json::Value>,
    pub old_record: Option<HashMap<String, serde_json::Value>>,
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
    columns: HashMap<String, PostgresColumnDesc>,
}

/// Describes a column in a [`PostgresTableDesc`].
#[derive(Debug)]
pub struct PostgresColumnDesc {
    /// The name of the column.
    name: String,
    /// The OID of the column's type.
    type_oid: i32,
    /// The modifier for the column's type -> pg_attribute.atttypmoda (e.g. max len of varchar)
    type_mod: i32,
    /// Whether the column is part of the table's primary key.
    is_identity: bool,
}

#[derive(Debug)]
pub struct PostgresTypeDesc {
    oid: i32,
    namespace: String,
    name: String,
}

#[derive(Debug)]
pub struct PostgresState {
    /// Our cursor into the WAL
    pub lsn: PgLsn,
    pub tables: HashMap<u32, PostgresTableDesc>,
    pub types: HashMap<u32, PostgresTypeDesc>,
}

#[derive(Debug)]
pub enum DataChange {
    Insert(PostgresRecord),
    Update,
    Delete,
    Truncate,
}

pub enum PostgresStateError {
    UnknownType(u32),
    UnknownTable(u32),
    InvalidColumnName,
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
                    let type_mod = col.type_modifier();
                    let name: String = col.name().unwrap().into();
                    let is_identity = col.flags() == 1i8;

                    columns.insert(
                        name.clone(),
                        PostgresColumnDesc {
                            name,
                            type_oid,
                            type_mod,
                            is_identity,
                        },
                    );
                }

                self.tables.insert(
                    relation.rel_id(),
                    PostgresTableDesc {
                        oid: relation.rel_id(),
                        namespace,
                        name,
                        replica_identity,
                        columns,
                    },
                );

                println!("{:?}", self);
                Ok(None)
            }
            LogicalReplicationMessage::Type(type_) => {
                self.types.insert(
                    type_.id(),
                    PostgresTypeDesc {
                        oid: type_.id() as i32,
                        namespace: type_.namespace().unwrap().into(),
                        name: type_.name().unwrap().into(),
                    },
                );
                println!("{:?}", type_);
                Ok(None)
            }
            LogicalReplicationMessage::Insert(insert) => {
                let table = match self.tables.get(&insert.rel_id()) {
                    Some(table) => table,
                    None => return Err(PostgresStateError::UnknownTable(insert.rel_id())),
                };
                let new_tuple: &[TupleData] = insert.tuple().tuple_data();

                let record = table
                    .columns
                    .values()
                    .zip(new_tuple)
                    .map(|(col, val)| {
                        (
                            col.name.clone(),
                            match val {
                                TupleData::Null => serde_json::Value::Null,
                                TupleData::UnchangedToast => {
                                    serde_json::Value::String("UNCHANGED_TOAST".to_string())
                                }
                                TupleData::Text(bytes) => {
                                    let x = str::from_utf8(bytes).unwrap();
                                    serde_json::json!(x)
                                }
                            },
                        )
                    })
                    .collect::<HashMap<String, serde_json::Value>>();

                let row = PostgresRecord {
                    schema: table.namespace.clone(),
                    table: table.name.clone(),
                    r#type: Action::INSERT,
                    record,
                    old_record: None,
                };

                let content = DataChange::Insert(row);

                println!("{:?}", content);
                Ok(Some(content))
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
