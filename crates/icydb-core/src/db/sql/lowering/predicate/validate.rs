use crate::db::{
    query::plan::expr::{Expr, scalar_where_truth_condition_is_admitted},
    sql::lowering::SqlLoweringError,
};

// Validate one planner-owned boolean WHERE expression after shared SQL
// lowering. This owns clause admission only; it does not reshape semantics.
pub(super) fn validate_where_bool_expr(expr: &Expr) -> Result<(), SqlLoweringError> {
    if scalar_where_truth_condition_is_admitted(expr) {
        Ok(())
    } else {
        Err(SqlLoweringError::unsupported_where_expression())
    }
}
