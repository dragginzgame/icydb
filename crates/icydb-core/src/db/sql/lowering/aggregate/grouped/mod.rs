mod validation;

pub(in crate::db::sql::lowering::aggregate) use validation::validate_grouped_aggregate_scalar_subexpressions;
pub(in crate::db::sql::lowering) use validation::{
    extend_unique_sql_expr_aggregate_calls, extend_unique_sql_select_item_aggregate_calls,
    resolve_having_aggregate_expr_index,
};
