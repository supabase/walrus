use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Column<'a> {
    pub name: &'a str,
    #[serde(alias = "type")]
    pub type_: &'a str,
    pub typeoid: Option<u32>,
    pub value: serde_json::Value,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PrimaryKeyRef<'a> {
    pub name: &'a str,
    #[serde(alias = "type")]
    pub type_: &'a str,
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
pub struct Record<'a> {
    pub action: Action,
    pub schema: &'a str,
    pub table: &'a str,
    pub pk: Option<Vec<PrimaryKeyRef<'a>>>,
    pub columns: Option<Vec<Column<'a>>>, // option is for truncate
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<Vec<Column<'a>>>, // option is for insert/update
    // Example: 2022-06-22 15:38:19.695275+00
    #[serde(with = "crate::timestamp_fmt")]
    pub timestamp: DateTime<Utc>,
}

impl<'a> Record<'a> {
    pub fn pkey_cols(&self) -> Vec<&'a str> {
        match &self.pk {
            Some(pkey_refs) => pkey_refs.iter().map(|x| x.name).collect(),
            None => vec![],
        }
    }

    pub fn has_primary_key(&self) -> bool {
        self.pkey_cols().len() != 0
    }
}
