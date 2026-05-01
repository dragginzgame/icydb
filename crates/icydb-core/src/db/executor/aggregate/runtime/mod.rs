//! Module: executor::aggregate::runtime
//! Responsibility: aggregate-owned grouped runtime mechanics for read execution.
//! Does not own: grouped route derivation or shared executor contracts.
//! Boundary: grouped fold/distinct/having/output execution for grouped read paths.

mod grouped_distinct;
mod grouped_fold;
mod grouped_output;
mod grouped_row;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        executor::projection::{GroupedRowView, ProjectionEvalError, evaluate_grouped_having_expr},
        query::plan::expr::CompiledExpr,
    },
    error::InternalError,
};

#[cfg(feature = "diagnostics")]
pub(in crate::db::executor) use grouped_fold::{
    GroupedCountFoldMetrics, with_grouped_count_fold_metrics,
};
pub(in crate::db::executor) use grouped_fold::{
    build_grouped_stream_with_runtime, execute_group_fold_stage,
};
pub(in crate::db::executor) use grouped_output::{
    GroupedOutputRuntimeObserverBindings, finalize_grouped_output_with_observer,
    finalize_path_outcome_for_path,
};
pub(in crate::db) use grouped_row::RuntimeGroupedRow;

// Evaluate one compiled grouped HAVING expression on one finalized grouped output row.
pub(in crate::db::executor) fn group_matches_having_expr(
    expr: &CompiledExpr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<bool, InternalError> {
    evaluate_grouped_having_expr(expr, grouped_row)
        .map_err(ProjectionEvalError::into_grouped_projection_internal_error)
}
