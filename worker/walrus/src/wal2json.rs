use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column {
    pub name: String,
    #[serde(alias = "type")]
    pub type_: String,
    pub typeoid: Option<u32>,
    pub value: serde_json::Value,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PrimaryKeyRef {
    pub name: String,
    #[serde(alias = "type")]
    pub type_: String,
    pub typeoid: u32,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Action {
    I,
    U,
    D,
    T,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Record {
    pub action: Action,
    pub schema: String, //&str,
    pub table: String,
    pub pk: Option<Vec<PrimaryKeyRef>>,
    pub columns: Option<Vec<Column>>, // option is for truncate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<Vec<Column>>, // option is for insert/update
    // Example: 2022-06-22 15:38:19.695275+00
    #[serde(with = "crate::timestamp_fmt")]
    pub timestamp: DateTime<Utc>,
}
