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

#[cfg(test)]
mod tests {
    extern crate diesel;
    use crate::models::realtime;
    use chrono::{DateTime, Utc};
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_serialize_insert() {
        let id: i32 = 2;
        let sub_id = uuid::uuid!("37c7e506-9eca-4671-8c48-526d404660ce");
        let record = realtime::WALRLS {
            wal: realtime::Data {
                schema: "public",
                table: "notes",
                r#type: realtime::Action::INSERT,

                commit_timestamp: &DateTime::parse_from_rfc3339("2020-04-12T22:10:57.002456+02:00")
                    .unwrap()
                    .with_timezone(&Utc),
                columns: vec![realtime::Column {
                    name: "id".to_string(),
                    type_: "int4".to_string(),
                }],
                record: HashMap::from([("id", json!(id))]),
                old_record: None,
            },
            is_rls_enabled: true,
            subscription_ids: vec![sub_id, sub_id],
            errors: vec!["sample error"],
        };

        let ser = serde_json::to_string_pretty(&record).unwrap();

        assert_eq!(
            ser,
            r#"{
  "wal": {
    "schema": "public",
    "table": "notes",
    "type": "INSERT",
    "commit_timestamp": "2020-04-12T20:10:57.002456Z",
    "columns": [
      {
        "name": "id",
        "type": "int4"
      }
    ],
    "record": {
      "id": 2
    },
    "old_record": null
  },
  "is_rls_enabled": true,
  "subscription_ids": [
    "37c7e506-9eca-4671-8c48-526d404660ce",
    "37c7e506-9eca-4671-8c48-526d404660ce"
  ],
  "errors": [
    "sample error"
  ]
}"#
        )
    }

    #[test]
    fn test_serialize_delete() {
        let id: i32 = 2;
        let record = realtime::WALRLS {
            wal: realtime::Data {
                schema: "public",
                table: "notes6",
                r#type: realtime::Action::DELETE,

                commit_timestamp: &DateTime::parse_from_rfc3339("2020-04-12T22:10:57.002456+02:00")
                    .unwrap()
                    .with_timezone(&Utc),
                columns: vec![realtime::Column {
                    name: "id".to_string(),
                    type_: "int4".to_string(),
                }],
                record: HashMap::new(),
                old_record: Some(HashMap::from([("id", json!(id))])),
            },
            is_rls_enabled: false,
            subscription_ids: vec![],
            errors: vec!["sample error"],
        };

        let ser = serde_json::to_string_pretty(&record).unwrap();

        assert_eq!(
            ser,
            r#"{
  "wal": {
    "schema": "public",
    "table": "notes6",
    "type": "DELETE",
    "commit_timestamp": "2020-04-12T20:10:57.002456Z",
    "columns": [
      {
        "name": "id",
        "type": "int4"
      }
    ],
    "record": {},
    "old_record": {
      "id": 2
    }
  },
  "is_rls_enabled": false,
  "subscription_ids": [],
  "errors": [
    "sample error"
  ]
}"#
        )
    }
}
