use chrono::{DateTime, TimeZone, Utc};
use serde::{self, Deserialize, Deserializer, Serializer};

// Example: 2022-06-22 15:38:19.695275+00
// https://docs.rs/chrono/latest/chrono/format/strftime/index.html
const DESER_FORMAT: &'static str = "%Y-%m-%d %H:%M:%S%.f%#z";

// Example: 2000-01-01T00:01:01Z
const SER_FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S%.fZ";

// The signature of a serialize_with function must follow the pattern:
//
//    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
//    where
//        S: Serializer
//
// although it may also be generic over the input types T.
pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = format!("{}", date.format(SER_FORMAT));
    serializer.serialize_str(&s)
}

// The signature of a deserialize_with function must follow the pattern:
//
//    fn deserialize<'de, D>(D) -> Result<T, D::Error>
//    where
//        D: Deserializer<'de>
//
// although it may also be generic over the output types T.
pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Utc.datetime_from_str(&s, DESER_FORMAT)
        .map_err(serde::de::Error::custom)
}
