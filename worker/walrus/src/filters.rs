use crate::models::{realtime, wal2json};
use log::warn;

fn is_null(v: &serde_json::Value) -> bool {
    v == &serde_json::Value::Null
}

pub fn visible_through_filters(
    filters: &Vec<realtime::UserDefinedFilter>,
    columns: &Vec<wal2json::Column>,
) -> Result<bool, String> {
    use realtime::Op;

    for filter in filters {
        let filter_value: serde_json::Value = match serde_json::from_str(&filter.value) {
            Ok(v) => v,
            Err(err) => return Err(format!("{}", err)),
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
            "boolean" | "smallint" | "integer" | "bigint" | "serial" | "bigserial" | "numeric"
            | "double precision" | "character" | "character varying" | "text" => match &filter.op {
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
                _ => return Err("could not handle filter op for allowed types".to_string()),
            },
            _ => return Err("Could not handle type. Delegate comparison to SQL".to_string()),
        };
    }
    Ok(true)
}

/// Returns a vector of realtime::Op that match for a OP b
fn get_valid_ops(
    a: &serde_json::Value,
    b: &serde_json::Value,
) -> Result<Vec<crate::realtime::Op>, String> {
    use serde_json::Value;

    match (a, b) {
        (Value::Null, Value::Null) => Ok(vec![]),
        (Value::Bool(a_), Value::Bool(b_)) => Ok(get_matching_ops(a_, b_)),
        (Value::Number(a_), Value::Number(b_)) => Ok(get_matching_ops(&a_.as_f64(), &b_.as_f64())),
        (Value::String(a_), Value::String(b_)) => Ok(get_matching_ops(a_, b_)),
        // Array possible
        // Object possible
        _ => return Err("non-scalar or mismatched json value types".to_string()),
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
    ops
}
