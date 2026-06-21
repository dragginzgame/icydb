use crate::db::sql::parser::{SqlExpr, SqlOrderTerm};

// Drop singleton-result ORDER BY terms that target the global aggregate output
// row itself, while preserving base-row ordering used to shape the aggregate input window.
pub(in crate::db::sql::lowering::aggregate) fn strip_inert_global_aggregate_output_order_terms(
    order_by: Vec<SqlOrderTerm>,
    inert_targets: &[SqlExpr],
) -> Vec<SqlOrderTerm> {
    order_by
        .into_iter()
        .filter(|term| !inert_targets.iter().any(|target| target == &term.field))
        .collect()
}
