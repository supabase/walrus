use crate::errors;
use crate::models::walrus;
use crate::models::{realtime, wal2json};
use diesel::*;
use log::warn;

pub mod sql_functions {
    use diesel::sql_types::*;
    use diesel::*;

    sql_function! {
        #[sql_name = "realtime.is_visible_through_filters"]
        fn is_visible_through_filters(columns: Jsonb, ids: Array<Int8> ) -> Array<Int8>
    }
}

pub fn is_visible_through_filters_sql(
    columns: &Vec<walrus::Column>,
    ids: &Vec<i64>,
    // TODO: convert this to use subscription_ids to reduce n calls
    conn: &mut PgConnection,
) -> Result<Vec<i64>, String> {
    select(sql_functions::is_visible_through_filters(
        serde_json::json!(columns),
        ids,
    ))
    .first(conn)
    .map_err(|x| format!("{}", x))
}

fn is_null(v: &serde_json::Value) -> bool {
    v == &serde_json::Value::Null
}

pub fn visible_through_filters(
    filters: &Vec<realtime::UserDefinedFilter>,
    columns: &Vec<wal2json::Column>,
) -> Result<bool, errors::FilterError> {
    use realtime::Op;

    for filter in filters {
        let filter_value: serde_json::Value = match serde_json::from_str(&filter.value) {
            Ok(v) => v,
            // Composite types are not parsable as json
            Err(err) => return Err(errors::FilterError::DelegateToSQL(format!("{}", err))),
        };

        let column = match columns
            .iter()
            .filter(|x| x.name == filter.column_name)
            .next()
        {
            Some(col) => col,
            // The filter references a column that does not exist
            None => {
                warn!(
                    "Attempted to filter non-existing column {}",
                    filter.column_name
                );
                return Ok(false);
            }
        };

        // Null column or filter values always result in a false because return is alway null
        // until `IS` op is implemented
        if is_null(&filter_value) || is_null(&column.value) {
            return Ok(false);
        };

        match column.type_.as_ref() {
            // All operations supported
            // regtype names (provided by wal2json)
            "boolean" | "smallint" | "integer" | "bigint" | "serial" | "bigserial" | "numeric"
            | "double precision" | "character" | "character varying" | "text"
            // Type 2 (from pg_type.typname)
            | "bool" | "char" | "int2" | "int4" | "int8" | "float4" | "float8" | "varchar"
            => match &filter.op {
                // List is currently exhastive but that may be possible if types/ops expand
                Op::Equal
                | Op::NotEqual
                | Op::LessThan
                | Op::LessThanOrEqual
                | Op::GreaterThan
                | Op::GreaterThanOrEqual => {
                    let valid_ops = get_valid_ops(&column.value, &filter_value)?;
                    if !valid_ops.contains(&filter.op) {
                        return Ok(false);
                    }
                }
            },
            // Only Equality ops supported
            "uuid" => match &filter.op {
                Op::Equal | Op::NotEqual => {
                    let valid_ops = get_valid_ops(&column.value, &filter_value)?;
                    if !valid_ops.contains(&filter.op) {
                        return Ok(false);
                    };
                }
                _ => {
                    return Err(errors::FilterError::DelegateToSQL(
                        "could not handle filter op for allowed types".to_string(),
                    ))
                }
            },
            _ => {
                return Err(errors::FilterError::DelegateToSQL(
                    "Could not handle type. Delegate comparison to SQL".to_string(),
                ))
            }
        };
    }
    Ok(true)
}

/// Returns a vector of realtime::Op that match for a OP b
fn get_valid_ops(
    a: &serde_json::Value,
    b: &serde_json::Value,
) -> Result<Vec<crate::realtime::Op>, errors::FilterError> {
    use serde_json::Value;

    match (a, b) {
        (Value::Null, Value::Null) => Ok(vec![]),
        (Value::Bool(a_), Value::Bool(b_)) => Ok(get_matching_ops(a_, b_)),
        (Value::Number(a_), Value::Number(b_)) => Ok(get_matching_ops(&a_.as_f64(), &b_.as_f64())),
        (Value::String(a_), Value::String(b_)) => Ok(get_matching_ops(a_, b_)),
        // Array possible
        // Object possible
        _ => {
            return Err(errors::FilterError::DelegateToSQL(
                "non-scalar or mismatched json value types".to_string(),
            ));
        }
    }
}

fn get_matching_ops<T>(a: &T, b: &T) -> Vec<crate::realtime::Op>
where
    T: PartialEq + PartialOrd,
{
    use realtime::Op;
    let mut ops = vec![];

    if a == b {
        ops.push(Op::Equal);
    };
    if a != b {
        ops.push(Op::NotEqual);
    };
    if a < b {
        ops.push(Op::LessThan);
    };
    if a <= b {
        ops.push(Op::LessThanOrEqual);
    };
    if a > b {
        ops.push(Op::GreaterThan);
    };
    if a >= b {
        ops.push(Op::GreaterThanOrEqual);
    };
    ops.sort();
    ops
}

#[cfg(test)]
mod tests {

    use crate::filters::record::user_defined::get_valid_ops;
    use crate::models::realtime::Op;
    use serde_json::json;

    #[test]
    fn test_get_valid_ops_eq() {
        let a: i32 = 1;
        let a = json!(a);

        let eq_ops = get_valid_ops(&a, &a).unwrap();
        assert_eq!(
            eq_ops,
            vec![Op::Equal, Op::LessThanOrEqual, Op::GreaterThanOrEqual]
        );
    }

    #[test]
    fn test_get_valid_ops_lt() {
        let a: i32 = 1;
        let a = json!(a);

        let b: i32 = 2;
        let b = json!(b);

        let eq_ops = get_valid_ops(&a, &b).unwrap();
        assert_eq!(
            eq_ops,
            vec![Op::NotEqual, Op::LessThan, Op::LessThanOrEqual]
        );
    }

    #[test]
    fn test_get_valid_ops_gt() {
        let a: i32 = 2;
        let a = json!(a);

        let b: i32 = 1;
        let b = json!(b);

        let eq_ops = get_valid_ops(&a, &b).unwrap();
        assert_eq!(
            eq_ops,
            vec![Op::NotEqual, Op::GreaterThan, Op::GreaterThanOrEqual]
        );
    }
}
