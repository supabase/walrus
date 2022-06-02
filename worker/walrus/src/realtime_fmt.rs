use chrono;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::*;
use uuid;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Action {
    INSERT,
    UPDATE,
    DELETE,
    TRUNCATE,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    pub name: String,
    #[serde(alias = "type")]
    pub type_: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Data {
    pub schema: String,
    pub table: String,
    pub r#type: Action,
    // TODO
    pub commit_timestamp: String, //chrono::DateTime<chrono::Utc>,
    pub columns: Vec<Column>,
    pub record: HashMap<String, serde_json::Value>,
    pub old_record: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WALRLS {
    pub wal: Data,
    pub is_rls_enabled: bool,
    pub subscription_ids: Vec<uuid::Uuid>,
    pub errors: Vec<String>,
}

// Subscriptions
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Op {
    #[serde(alias = "eq")]
    Equal,
    #[serde(alias = "neq")]
    NotEqual,
    #[serde(alias = "lt")]
    LessThan,
    #[serde(alias = "lte")]
    LessThanOrEqual,
    #[serde(alias = "gt")]
    GreaterThan,
    #[serde(alias = "gte")]
    GreaterThanOrEqual,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UserDefinedFiltern {
    pub column_name: String,
    pub op: Op,
    pub value: serde_json::Value,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Subscription {
    pub subscription_id: uuid::Uuid,
    pub filters: Vec<UserDefinedFiltern>,
    pub claims_role: String,
}
