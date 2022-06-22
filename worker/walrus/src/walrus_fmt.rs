use serde::Serialize;

#[derive(Serialize)]
pub struct WalrusRecord {
    wal: serde_json::Value,
    is_rls_enabled: bool,
    subscription_ids: Vec<uuid::Uuid>,
    errors: Vec<String>,
}

#[derive(Serialize, Debug)]
pub struct WALColumn {
    pub name: String,
    pub type_name: String,
    pub type_oid: Option<u32>,
    pub value: serde_json::Value,
    pub is_pkey: bool,
    pub is_selectable: bool,
}
