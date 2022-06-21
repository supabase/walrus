use crate::wal2json;
use chrono;
use diesel::*;
use log::error;
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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
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
pub struct UserDefinedFilter {
    pub column_name: String,
    pub op: Op,
    pub value: String, // Why did I make this a text field?,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Subscription {
    pub id: i64,
    pub schema_name: String,
    pub table_name: String,
    pub subscription_id: uuid::Uuid,
    pub filters: Vec<UserDefinedFilter>,
    pub claims_role: String,
}

/// Checks to see if the new record is a change to realtime.subscriptions
/// and updates the subscriptions variable if a change is detected
pub fn update_subscriptions(
    rec: &wal2json::Record,
    subscriptions: &mut Vec<Subscription>,
    conn: &mut PgConnection,
) -> () {
    // If the record is not a subscription, return
    if rec.schema != "realtime" || rec.table != "subscription" {
        return ();
    }

    if rec.action == wal2json::Action::T {
        subscriptions.clear();
        return ();
    }

    let id: i64 = match rec
        .columns
        .as_ref()
        // Deletes have the id value in the identity field
        .unwrap_or(rec.identity.as_ref().unwrap_or(&vec![]))
        .iter()
        .filter(|x| x.name == "id")
        .map(|x| x.value.clone())
        .next()
    {
        Some(id_json) => match id_json {
            serde_json::Value::Number(id_num) => match id_num.as_i64() {
                Some(id) => id,
                None => {
                    error!(
                        "Invalid id in realtime.subscription. Expected i64, got: {}",
                        id_num
                    );
                    return ();
                }
            },
            _ => {
                error!(
                    "Invalid id in realtime.subscription. Expected number, got: {}",
                    id_json
                );
                return ();
            }
        },
        None => {
            error!("No id column found on realtime.subscription");
            return ();
        }
    };

    match rec.action {
        wal2json::Action::I => {
            match crate::sql_functions::get_subscription_by_id(id, conn) {
                Ok(new_sub) => subscriptions.push(new_sub),
                Err(err) => error!(
                    "Failed to parse wal2json record to realtime.subscription: {} ",
                    err
                ),
            };
        }
        wal2json::Action::U => {
            // Delete existing sub
            subscriptions.retain_mut(|x| x.id != id);

            // Add updated sub
            match crate::sql_functions::get_subscription_by_id(id, conn) {
                Ok(new_sub) => subscriptions.push(new_sub),
                Err(err) => error!(
                    "Failed to parse wal2json record to realtime.subscription: {} ",
                    err
                ),
            };
        }
        wal2json::Action::D => {
            subscriptions.retain(|x| x.id != id);
        }
        wal2json::Action::T => {
            // Handled above
        }
    };
}
