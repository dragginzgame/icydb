//! Module: executor::aggregate::runtime::grouped_fold::ingest
//! Responsibility: generic grouped fold ingest over one shared grouped bundle.
//! Does not own: grouped planner policy or grouped page/projection finalization.
//! Boundary: folds grouped source rows into the shared grouped bundle state.

use crate::{
    db::{
        data::DataKey,
        executor::{
            KeyStreamLoopControl,
            aggregate::{
                ExecutionContext, GroupError, runtime::grouped_fold::bundle::GroupedAggregateBundle,
            },
            pipeline::contracts::{GroupedRouteStage, GroupedStreamStage},
        },
    },
    error::InternalError,
};

// Ingest grouped source rows into the shared grouped bundle while preserving
// grouped budget contracts and borrowed grouped-key fast paths.
pub(super) fn fold_group_rows_into_bundle(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage<'_>,
    grouped_execution_context: &mut ExecutionContext,
    grouped_bundle: &mut GroupedAggregateBundle,
) -> Result<(usize, usize), InternalError> {
    let (row_runtime, execution_preparation, resolved) = stream.parts_mut();
    let compiled_predicate = execution_preparation.compiled_predicate();
    let mut scanned_rows = 0usize;
    let mut filtered_rows = 0usize;
    let consistency = route.consistency();
    let mut on_key = |data_key: DataKey| -> Result<KeyStreamLoopControl, InternalError> {
        let Some(row_view) = row_runtime.read_row_view(consistency, &data_key)? else {
            return Ok(KeyStreamLoopControl::Emit);
        };
        scanned_rows = scanned_rows.saturating_add(1);
        if let Some(compiled_predicate) = compiled_predicate
            && !row_view.eval_predicate(compiled_predicate)
        {
            return Ok(KeyStreamLoopControl::Emit);
        }
        filtered_rows = filtered_rows.saturating_add(1);

        // Phase 1: preserve the borrowed grouped-key fast path so existing
        // groups stay allocation-free on the hot ingest loop.
        let borrowed_group_hash =
            if super::supports_borrowed_group_probe(&row_view, route.group_fields())? {
                Some(super::stable_hash_group_values_from_row_view(
                    &row_view,
                    route.group_fields(),
                )?)
            } else {
                None
            };
        let mut owned_group_key = None;

        // Phase 2: update the shared per-group aggregate-state row instead of
        // routing the row through one engine-owned group map per aggregate.
        grouped_bundle
            .ingest_row(
                grouped_execution_context,
                &data_key,
                &row_view,
                route.group_fields(),
                borrowed_group_hash,
                &mut owned_group_key,
            )
            .map_err(GroupError::into_internal_error)?;

        Ok(KeyStreamLoopControl::Emit)
    };
    crate::db::executor::drive_key_stream_with_control_flow(
        resolved.key_stream_mut(),
        &mut || KeyStreamLoopControl::Emit,
        &mut on_key,
    )?;

    Ok((scanned_rows, filtered_rows))
}
