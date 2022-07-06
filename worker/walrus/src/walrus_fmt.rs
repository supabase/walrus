use serde::Serialize;

#[derive(Serialize)]
pub struct WalrusRecord<'a> {
    wal: serde_json::Value,
    is_rls_enabled: bool,
    subscription_ids: Vec<&'a uuid::Uuid>,
    errors: Vec<&'a str>,
}

#[derive(Serialize, Debug)]
pub struct WALColumn<'a> {
    pub name: &'a str,
    pub type_name: &'a str,
    pub type_oid: Option<u32>,
    pub value: serde_json::Value,
    pub is_pkey: bool,
    pub is_selectable: bool,
}
