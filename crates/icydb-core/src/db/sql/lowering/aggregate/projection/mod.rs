mod order_by;
mod remap;
mod terminals;

pub(in crate::db::sql::lowering::aggregate) use order_by::strip_inert_global_aggregate_output_order_terms;
pub(in crate::db::sql::lowering) use remap::expr_references_global_direct_fields;
pub(in crate::db::sql::lowering::aggregate) use remap::resolve_having_global_aggregate_terminal_index;
pub(in crate::db::sql::lowering::aggregate) use terminals::LoweredSqlGlobalAggregateTerminals;
