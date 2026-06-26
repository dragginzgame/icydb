mod validation;

pub(in crate::db::sql::lowering::aggregate) use validation::validate_grouped_aggregate_scalar_subexpressions;
pub(in crate::db::sql::lowering) use validation::{
    SqlAggregateCallInterner, resolve_having_aggregate_expr_index,
};
