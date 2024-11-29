pub mod realtime {
    table! {
        realtime.subscription (id) {
            id -> Int8,
            subscription_id -> Uuid,
            entity -> Oid,
            //filters -> Array<Record<(Text, crate::realtime_fmt::EqualityOpType, Text)>>,
            filters -> Array<crate::models::realtime::UserDefinedFilterType>,
            claims -> Jsonb,
            claims_role -> Int4,
            created_at -> Timestamp,
            schema_name -> Text,
            table_name -> Text,
            claims_role_name -> Text,
        }
    }
}
