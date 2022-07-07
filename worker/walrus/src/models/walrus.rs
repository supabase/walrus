use serde::Serialize;

/// An internal representation of columns used when passing column data to SQL is required
/// (user defined filters)
#[derive(Serialize, Debug)]
pub struct Column<'a> {
    pub name: &'a str,
    pub type_name: &'a str,
    pub type_oid: Option<u32>,
    pub value: serde_json::Value,
    pub is_pkey: bool,
    pub is_selectable: bool,
}
