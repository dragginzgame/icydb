mod order_by;
mod remap;
mod terminals;

pub(in crate::db::sql::lowering::aggregate) use order_by::strip_inert_global_aggregate_output_order_terms;
pub(in crate::db::sql::lowering::aggregate) use terminals::LoweredSqlGlobalAggregateTerminals;
