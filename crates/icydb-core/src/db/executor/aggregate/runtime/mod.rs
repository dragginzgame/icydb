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
        data::DecodedDataStoreKey,
        executor::{
            aggregate::CompiledExpr,
            pipeline::runtime::{GroupedStreamStage, RowView},
            projection::{GroupedRowView, ProjectionEvalError, evaluate_grouped_having_expr},
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
};

pub(in crate::db::executor) use grouped_fold::{
    build_grouped_stream_with_runtime, execute_group_fold_stage, try_execute_grouped_count_metadata,
};
pub(in crate::db) use grouped_row::RuntimeGroupedRow;

// Evaluate one compiled grouped HAVING expression on one finalized grouped output row.
pub(in crate::db::executor) fn group_matches_having_expr(
    expr: &CompiledExpr,
    grouped_row: &GroupedRowView<'_>,
) -> Result<bool, InternalError> {
    evaluate_grouped_having_expr(expr, grouped_row)
        .map_err(ProjectionEvalError::into_internal_error)
}

// Non-stopping terminals reuse the same source reader as ordered count folds.
fn fold_filtered_source_rows(
    consistency: MissingRowPolicy,
    stream: &mut GroupedStreamStage,
    mut ingest: impl FnMut(&DecodedDataStoreKey, &RowView) -> Result<(), InternalError>,
) -> Result<usize, InternalError> {
    fold_filtered_source_rows_until(consistency, stream, |key, row| {
        ingest(key, row)?;
        Ok(false)
    })
    .map(|(filtered_rows, _stopped)| filtered_rows)
}

// A true callback result means the ordered page stopped before consuming its
// boundary row. Count only successfully ingested, non-stopping rows; absence,
// filter rejection and failed ingest never advance the completed-row count.
fn fold_filtered_source_rows_until(
    consistency: MissingRowPolicy,
    stream: &mut GroupedStreamStage,
    mut ingest: impl FnMut(&DecodedDataStoreKey, &RowView) -> Result<bool, InternalError>,
) -> Result<(usize, bool), InternalError> {
    let (row_runtime, execution_preparation, resolved) = stream.fold_inputs_mut();
    let effective_runtime_filter_program = execution_preparation.effective_runtime_filter_program();
    let mut filtered_rows = 0usize;
    while let Some(data_key) = resolved.key_stream_mut().next_key()? {
        let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
            continue;
        };
        if let Some(filter) = effective_runtime_filter_program
            && !row_view.eval_filter_program(filter)?
        {
            continue;
        }
        if ingest(&data_key, &row_view)? {
            return Ok((filtered_rows, true));
        }
        filtered_rows = filtered_rows.saturating_add(1);
    }
    Ok((filtered_rows, false))
}
