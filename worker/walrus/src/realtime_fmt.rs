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
    pub r#type: String,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_record: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct WALRLS {
    pub wal: Data,
    pub is_rls_enabled: bool,
    pub subscription_ids: Vec<uuid::Uuid>,
    pub errors: Vec<String>,
}
