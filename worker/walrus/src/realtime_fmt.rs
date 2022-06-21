use crate::wal2json;
use chrono;
use log::error;
use log::info;
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
    pub id: u64,
    pub schema_name: String,
    pub table_name: String,
    pub subscription_id: uuid::Uuid,
    pub filters: Vec<UserDefinedFilter>,
    pub claims_role: String,
}

/// Checks to see if the new record is a change to realtime.subscriptions
/// and updates the subscriptions variable if a change is detected
pub fn update_subscriptions(rec: &wal2json::Record, subscriptions: &mut Vec<Subscription>) -> () {
    fn get_column_value_by_name(
        columns: &Vec<wal2json::Column>,
        column_name: &str,
    ) -> Option<serde_json::Value> {
        columns
            .iter()
            .filter(|x| x.name == column_name)
            .map(|x| x.value.clone())
            .next()
    }

    enum NewOrOld {
        New,
        Old,
    }

    fn wal2json_to_subscription(
        rec: &wal2json::Record,
        new_or_old: NewOrOld,
    ) -> Result<Subscription, String> {
        let columns = match new_or_old {
            NewOrOld::New => match &rec.columns {
                Some(cols) => cols.clone(),
                None => {
                    return Err("Failed to parse wal2json record. No columns contents".to_string());
                }
            },
            NewOrOld::Old => match &rec.identity {
                Some(ident) => ident.clone(),
                None => {
                    return Err("Failed to parse wal2json record. No identity contents".to_string());
                }
            },
        };

        let id: u64 = match get_column_value_by_name(&columns, "id") {
            Some(id_json) => match id_json {
                serde_json::Value::Number(id_num) => match id_num.as_u64() {
                    Some(id) => id,
                    None => {
                        return Err(format!(
                            "Invalid id in realtime.subscription. Expected u64: {}",
                            id_num,
                        ));
                    }
                },
                _ => {
                    return Err(format!(
                        "Invalid id in realtime.subscription. Expected number, got: {}",
                        id_json
                    ));
                }
            },
            None => {
                return Err("No id column found on realtime.subscription".to_string());
            }
        };

        let subscription_id = match get_column_value_by_name(&columns, "subscription_id") {
            Some(uuid_json) => match uuid::Uuid::parse_str(&uuid_json.as_str().unwrap()) {
                Ok(uuid) => uuid,
                Err(err) => {
                    return Err(format!(
                        "Invalid subscription_id realtime.subscription: {}",
                        err
                    ));
                }
            },
            None => {
                return Err("No subscription_id found on realtime.subscription".to_string());
            }
        };

        let filters: Vec<UserDefinedFilter> = match get_column_value_by_name(&columns, "filters") {
            Some(fs_json) => match serde_json::from_value(fs_json.clone()) {
                Ok(udfs) => udfs,
                Err(err) => {
                    return Err(format!("Invalid filters in realtime.subscription: {}", err));
                }
            },
            None => {
                return Err("No filters column found on realtime.subscription".to_string());
            }
        };

        let claims_role = match get_column_value_by_name(&columns, "claims_role") {
            Some(role_json) => match role_json {
                serde_json::Value::String(role_name) => role_name,
                _ => {
                    return Err("Invalid claims_role in realtime.subscription".to_string());
                }
            },
            None => {
                return Err("No claims_role column found on realtime.subscription".to_string());
            }
        }
        .to_string();

        Ok(Subscription {
            id,
            schema_name: rec.schema.to_string(),
            table_name: rec.table.to_string(),
            subscription_id,
            filters,
            claims_role,
        })
    }

    // If the record is not a subscription, return
    if rec.schema != "realtime" || rec.table != "subscription" {
        return ();
    }
    //TODO manage the subscriptions vector from the WAL stream
    match rec.action {
        wal2json::Action::I => {
            info!("SUBSCRIPTION: GOT INSERT");
            match wal2json_to_subscription(rec, NewOrOld::New) {
                Ok(new_sub) => subscriptions.push(new_sub),
                Err(err) => error!(
                    "Failed to parse wal2json record to realtime.subscription: {} ",
                    err
                ),
            };
        }
        wal2json::Action::U => {
            info!("SUBSCRIPTION: GOT UPDATE");
            // Delete old sub
            let old_sub = match wal2json_to_subscription(rec, NewOrOld::New) {
                Ok(old_sub) => old_sub,
                Err(err) => {
                    error!(
                        "Failed to parse wal2json record to realtime.subscription update new: {} ",
                        err
                    );
                    return ();
                }
            };
            subscriptions.retain_mut(|x| x.id != old_sub.id);

            // Add new sub
            match wal2json_to_subscription(rec, NewOrOld::New) {
                Ok(new_sub) => subscriptions.push(new_sub),
                Err(err) => {
                    error!(
                        "Failed to parse wal2json record to realtime.subscription update new: {} ",
                        err
                    );
                    return ();
                }
            };
        }
        wal2json::Action::D => {
            info!("SUBSCRIPTION: GOT DELETE");
            let old_sub = match wal2json_to_subscription(rec, NewOrOld::New) {
                Ok(old_sub) => old_sub,
                Err(err) => {
                    error!(
                        "Failed to parse wal2json record to realtime.subscription update new: {} ",
                        err
                    );
                    return ();
                }
            };
            subscriptions.retain(|x| x.id != old_sub.id);
        }
        wal2json::Action::T => {
            info!("SUBSCRIPTION: GOT TRUNCATE");
            subscriptions.clear();
        }
    }
}
