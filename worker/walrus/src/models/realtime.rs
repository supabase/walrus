use crate::models::wal2json;
use crate::sql::schema::realtime::subscription::dsl::*;
use chrono::{DateTime, NaiveDateTime, Utc};
use diesel::deserialize::{self, FromSql};
use diesel::pg::{Pg, PgValue};
use diesel::serialize::{self, IsNull, Output, ToSql, WriteTuple};
use diesel::sql_types::{Record, Text};
use diesel::*;
use log::{debug, error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::*;
use uuid;

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
pub enum Action {
    INSERT,
    UPDATE,
    DELETE,
    TRUNCATE,
}

impl Action {
    pub fn from_wal2json(action: &wal2json::Action) -> Self {
        match action {
            wal2json::Action::I => Self::INSERT,
            wal2json::Action::U => Self::UPDATE,
            wal2json::Action::D => Self::DELETE,
            wal2json::Action::T => Self::TRUNCATE,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct Column {
    pub name: String,
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub type_: String,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
pub struct Data<'a> {
    pub schema: &'a str,
    pub table: &'a str,
    pub r#type: Action,
    #[serde(with = "crate::timestamp_fmt")]
    pub commit_timestamp: &'a DateTime<Utc>,
    pub columns: Vec<Column>,
    pub record: HashMap<&'a str, serde_json::Value>,
    pub old_record: Option<HashMap<&'a str, serde_json::Value>>,
}

#[derive(Serialize, Clone, Debug, Eq, PartialEq)]
pub struct WALRLS<'a> {
    pub wal: Data<'a>,
    pub is_rls_enabled: bool,
    pub subscription_ids: Vec<uuid::Uuid>,
    pub errors: Vec<&'a str>,
}

// Subscriptions
#[derive(Serialize, Deserialize, Clone, Debug, Queryable, Eq, PartialEq)]
pub struct Subscription {
    pub id: i64,
    pub subscription_id: uuid::Uuid,
    pub entity: u32,
    // This also works for anonymous deser of filters (schema.rs also must change)
    //pub filters: Vec<(String, EqualityOp, String)>,
    pub filters: Vec<UserDefinedFilter>,
    pub claims: serde_json::Value,
    pub claims_role: i32,
    pub created_at: NaiveDateTime,
    pub schema_name: String,
    pub table_name: String,
    pub claims_role_name: String,
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

    debug!("Subscription record detected");

    if rec.action == wal2json::Action::T {
        subscriptions.clear();
        debug!("Subscription truncate. Total {}", subscriptions.len());
        return ();
    }

    let id_val: i64 = match rec
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
                Some(id_val) => id_val,
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
            match subscription
                .filter(id.eq(id_val))
                .first::<Subscription>(conn)
            {
                Ok(new_sub) => {
                    subscriptions.push(new_sub);
                    debug!("Subscription inserted. Total {}", subscriptions.len());
                }
                Err(err) => {
                    error!("No subscription found: id={}, Error: {} ", id_val, err);
                }
            };
        }
        wal2json::Action::U => {
            // Delete existing sub
            let before_update_count = subscriptions.len();

            subscriptions.retain_mut(|x| x.id != id_val);

            // Add updated sub
            match subscription
                .filter(id.eq(id_val))
                .first::<Subscription>(conn)
            {
                Ok(new_sub) => subscriptions.push(new_sub),
                Err(err) => error!("No subscription found: {} ", err),
            };

            debug!(
                "Subscription update. Total before {}, after {}. id_val {}",
                before_update_count,
                subscriptions.len(),
                id_val,
            );
        }
        wal2json::Action::D => {
            let before_delete_count = subscriptions.len();
            subscriptions.retain(|x| x.id != id_val);

            debug!(
                "Subscription delete. Total before {}, after {}",
                before_delete_count,
                subscriptions.len()
            );
        }
        wal2json::Action::T => {
            // Handled above
        }
    };
}

#[derive(SqlType, PartialEq)]
#[diesel(postgres_type(schema = "realtime", name = "equality_op"))]
pub struct OpType;

#[derive(
    Debug, PartialEq, FromSqlRow, AsExpression, Clone, Deserialize, Serialize, Eq, Ord, PartialOrd,
)]
#[diesel(sql_type = OpType)]
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

impl ToSql<OpType, Pg> for Op {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        match *self {
            Op::Equal => out.write_all(b"eq")?,
            Op::NotEqual => out.write_all(b"neq")?,
            Op::LessThan => out.write_all(b"lt")?,
            Op::LessThanOrEqual => out.write_all(b"lte")?,
            Op::GreaterThan => out.write_all(b"gt")?,
            Op::GreaterThanOrEqual => out.write_all(b"gt")?,
        }
        Ok(IsNull::No)
    }
}

impl FromSql<OpType, Pg> for Op {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        match bytes.as_bytes() {
            b"eq" => Ok(Op::Equal),
            b"neq" => Ok(Op::NotEqual),
            b"lt" => Ok(Op::LessThan),
            b"lte" => Ok(Op::LessThanOrEqual),
            b"gt" => Ok(Op::GreaterThan),
            b"gte" => Ok(Op::GreaterThanOrEqual),
            _ => Err("Unrecognized enum variant".into()),
        }
    }
}

#[derive(SqlType, PartialEq, QueryId)]
#[diesel(postgres_type(schema = "realtime", name = "user_defined_filter"))]
pub struct UserDefinedFilterType;

#[derive(Debug, PartialEq, FromSqlRow, AsExpression, Clone, Deserialize, Serialize, Eq)]
#[diesel(sql_type = UserDefinedFilterType)]
pub struct UserDefinedFilter {
    pub column_name: String,
    pub op: Op,
    pub value: String, // Why did I make this a text field?,
}

impl ToSql<UserDefinedFilterType, Pg> for UserDefinedFilter {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        WriteTuple::<(Text, OpType, Text)>::write_tuple(
            &(self.column_name.as_str(), &self.op, self.value.as_str()),
            out,
        )
    }
}

impl FromSql<UserDefinedFilterType, Pg> for UserDefinedFilter {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let (column_name, op, value) =
            FromSql::<Record<(Text, OpType, Text)>, Pg>::from_sql(bytes)?;
        Ok(UserDefinedFilter {
            column_name,
            op,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    extern crate diesel;
    use crate::models::realtime::*;
    use crate::models::{realtime, wal2json};
    use crate::tests::*;
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use uuid;

    #[test]
    fn test_update_subscriptions_insert() {
        let mut conn = establish_connection();

        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");

        crate::tests::truncate("realtime", "subscription", &mut conn);

        let subscriptions_table_oid =
            crate::filters::table::table_oid::get_table_oid("realtime", "subscription", &mut conn)
                .unwrap();

        let mut subscriptions = vec![];

        let sub_id = uuid::uuid!("54249b4a-98ca-4941-8af7-0154123df504");

        insert_into(subscription)
            .values((
                subscription_id.eq(sub_id),
                entity.eq(subscriptions_table_oid),
                claims.eq(json!({
                    "role": "postgres",
                    "email": "example@example.com",
                    "sub": sub_id,
                })),
            ))
            .execute(&mut conn)
            .unwrap();

        let subscription_row = subscription
            .first::<realtime::Subscription>(&mut conn)
            .unwrap();
        let subscription_row_id = subscription_row.id;

        let wal2json = format!(
            r#"{{
            "action":"I",
            "timestamp":"2022-07-13 17:04:40.784361+00",
            "schema":"realtime",
            "table":"subscription",
            "columns":[
                {{
                    "name":"id",
                    "type":"bigint",
                    "typeoid":20,
                    "value":{subscription_row_id}
                }}
            ],
            "pk":[{{"name":"id","type":"bigint","typeoid":20}}]
        }}"#
        );

        let rec: wal2json::Record = serde_json::from_str(&wal2json).unwrap();
        update_subscriptions(&rec, &mut subscriptions, &mut conn);

        // Subscription was added
        assert_eq!(subscriptions, vec![subscription_row]);
    }

    #[test]
    fn test_update_subscriptions_update() {
        let mut conn = establish_connection();

        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");

        crate::tests::truncate("realtime", "subscription", &mut conn);

        let subscriptions_table_oid =
            crate::filters::table::table_oid::get_table_oid("realtime", "subscription", &mut conn)
                .unwrap();

        let sub_id = uuid::uuid!("54249b4a-98ca-4941-8af7-0154123df504");

        insert_into(subscription)
            .values((
                subscription_id.eq(sub_id),
                entity.eq(subscriptions_table_oid),
                claims.eq(json!({
                    "role": "postgres",
                    "email": "example@example.com",
                    "sub": sub_id,
                })),
            ))
            .execute(&mut conn)
            .unwrap();

        let subscription_row = subscription
            .first::<realtime::Subscription>(&mut conn)
            .unwrap();
        let subscription_row_id = subscription_row.id;

        let mut subscriptions = vec![subscription_row.clone()];

        // Update the subscription id
        let updated_sub_id = uuid::uuid!("54249b4a-98ca-4941-8af7-0154123df504");
        diesel::sql_query(format!(
            "update realtime.subscription set subscription_id = '{updated_sub_id}'"
        ))
        .execute(&mut conn)
        .unwrap();

        let wal2json = format!(
            r#"{{
            "action":"U",
            "timestamp":"2022-07-13 17:04:40.784361+00",
            "schema":"realtime",
            "table":"subscription",
            "columns":[
                {{
                    "name":"id",
                    "type":"bigint",
                    "typeoid":20,
                    "value":{subscription_row_id}
                }}
            ],
            "pk":[{{"name":"id","type":"bigint","typeoid":20}}]
        }}"#
        );

        let rec: wal2json::Record = serde_json::from_str(&wal2json).unwrap();
        update_subscriptions(&rec, &mut subscriptions, &mut conn);

        assert_eq!(subscriptions[0].subscription_id, updated_sub_id);
    }

    #[test]
    fn test_update_subscriptions_delete() {
        let mut conn = establish_connection();

        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");

        crate::tests::truncate("realtime", "subscription", &mut conn);

        let subscription_row_id = 1;

        let mut subscriptions = vec![realtime::Subscription {
            id: subscription_row_id,
            subscription_id: uuid::uuid!("54249b4a-98ca-4941-8af7-0154123df504"),
            entity: 999,
            filters: vec![],
            claims: json!({}),
            claims_role: 999,
            schema_name: "abc".to_string(),
            table_name: "abc".to_string(),
            claims_role_name: "abc".to_string(),
            created_at: Utc::now().naive_utc(),
        }];

        let wal2json = format!(
            r#"{{
            "action":"D",
            "timestamp":"2022-07-13 17:04:40.784361+00",
            "schema":"realtime",
            "table":"subscription",
            "identity":[
                {{
                    "name":"id",
                    "type":"bigint",
                    "typeoid":20,
                    "value":{subscription_row_id}
                }}
            ],
            "pk":[{{"name":"id","type":"bigint","typeoid":20}}]
        }}"#
        );

        let rec: wal2json::Record = serde_json::from_str(&wal2json).unwrap();
        update_subscriptions(&rec, &mut subscriptions, &mut conn);

        assert_eq!(subscriptions, vec![]);
    }

    #[test]
    fn test_update_subscriptions_truncate() {
        let mut conn = establish_connection();

        crate::sql::migrations::run_migrations(&mut conn)
            .expect("Pending migrations failed to execute");

        crate::tests::truncate("realtime", "subscription", &mut conn);

        let subscription_row_id = 1;

        let mut subscriptions = vec![realtime::Subscription {
            id: subscription_row_id,
            subscription_id: uuid::uuid!("54249b4a-98ca-4941-8af7-0154123df504"),
            entity: 999,
            filters: vec![],
            claims: json!({}),
            claims_role: 999,
            schema_name: "abc".to_string(),
            table_name: "abc".to_string(),
            claims_role_name: "abc".to_string(),
            created_at: Utc::now().naive_utc(),
        }];

        let wal2json = format!(
            r#"{{
            "action":"T",
            "timestamp":"2022-07-13 17:04:40.784361+00",
            "schema":"realtime",
            "table":"subscription"
        }}"#
        );

        let rec: wal2json::Record = serde_json::from_str(&wal2json).unwrap();
        update_subscriptions(&rec, &mut subscriptions, &mut conn);

        assert_eq!(subscriptions, vec![]);
    }
}
