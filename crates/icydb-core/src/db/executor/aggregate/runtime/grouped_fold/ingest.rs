//! Module: db::executor::aggregate::runtime::grouped_fold::ingest
//! Responsibility: module-local ownership and contracts for db::executor::aggregate::runtime::grouped_fold::ingest.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use std::cmp::Ordering;

use crate::{
    db::{
        contracts::canonical_value_compare,
        data::DataKey,
        executor::{
            KeyStreamLoopControl,
            aggregate::{ExecutionContext, FoldControl, GroupError, GroupedAggregateEngine},
            group::{GroupKey, KeyCanonicalError},
            pipeline::contracts::{GroupedRouteStage, GroupedStreamStage, RowView},
        },
    },
    error::InternalError,
    value::Value,
};

///
/// GroupedFoldRowInput
///
/// GroupedFoldRowInput carries the one decoded grouped row payload that every
/// grouped reducer needs during one hot ingest step.
/// Keeping these row-scoped values together avoids argument-list churn while
/// preserving the existing grouped fold ownership boundary.
///

struct GroupedFoldRowInput<'a> {
    canonical_group_value: &'a Value,
    data_key: &'a DataKey,
    group_key: &'a GroupKey,
    row_view: &'a RowView,
}

// Ingest grouped source rows into aggregate reducers while preserving budget contracts.
pub(super) fn fold_group_rows_into_engines(
    route: &GroupedRouteStage,
    stream: &mut GroupedStreamStage<'_>,
    grouped_execution_context: &mut ExecutionContext,
    grouped_engines: &mut [Box<dyn GroupedAggregateEngine>],
    short_circuit_keys: &mut [Vec<Value>],
    max_groups_bound: usize,
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
        let group_values = row_view.group_values(route.group_fields())?;
        let group_key = GroupKey::from_group_values(group_values)
            .map_err(KeyCanonicalError::into_internal_error)?;
        let row_input = GroupedFoldRowInput {
            canonical_group_value: group_key.canonical_value(),
            data_key: &data_key,
            group_key: &group_key,
            row_view: &row_view,
        };
        fold_group_input_with_engines(
            short_circuit_keys,
            max_groups_bound,
            grouped_execution_context,
            grouped_engines,
            row_input,
        )?;

        Ok(KeyStreamLoopControl::Emit)
    };
    crate::db::executor::drive_key_stream_with_control_flow(
        resolved.key_stream_mut(),
        &mut || KeyStreamLoopControl::Emit,
        &mut on_key,
    )?;

    Ok((scanned_rows, filtered_rows))
}

// Shared per-row grouped-engine ingest control flow.
// Typed wrappers inject aggregate-engine ingestion while this helper owns
// short-circuit key rejection and bounded tracking invariants.
fn fold_group_input_with_engines(
    short_circuit_keys: &mut [Vec<Value>],
    max_groups_bound: usize,
    grouped_execution_context: &mut ExecutionContext,
    grouped_engines: &mut [Box<dyn GroupedAggregateEngine>],
    row_input: GroupedFoldRowInput<'_>,
) -> Result<(), InternalError> {
    // Phase 1: specialize the common single-aggregate grouped shape so the
    // hot row-ingest loop avoids sibling-engine iteration and repeated bounds
    // checks when only one reducer exists.
    if grouped_engines.len() == 1 && short_circuit_keys.len() == 1 {
        return fold_group_input_single_engine(
            &mut short_circuit_keys[0],
            max_groups_bound,
            grouped_execution_context,
            &mut grouped_engines[0],
            row_input,
        );
    }

    // Phase 2: retain the generic multi-engine ingest path for grouped shapes
    // that need sibling reducer coordination.
    for (index, done_group_keys) in short_circuit_keys.iter_mut().enumerate() {
        if done_group_keys.iter().any(|done| {
            canonical_value_compare(done, row_input.canonical_group_value) == Ordering::Equal
        }) {
            continue;
        }

        let Some(engine) = grouped_engines.get_mut(index) else {
            return Err(
                GroupedRouteStage::engine_index_out_of_bounds_during_fold_ingest(
                    index,
                    grouped_engines.len(),
                ),
            );
        };
        let fold_control = engine
            .ingest(
                grouped_execution_context,
                row_input.data_key,
                row_input.group_key,
                row_input.row_view,
            )
            .map_err(GroupError::into_internal_error)?;
        if matches!(fold_control, FoldControl::Break) {
            done_group_keys.push(row_input.canonical_group_value.clone());
            debug_assert!(
                done_group_keys.len() <= max_groups_bound,
                "grouped short-circuit key tracking must stay bounded by max_groups",
            );
        }
    }

    Ok(())
}

// Ingest one grouped row into the common single-reducer grouped shape without
// paying the generic sibling-engine coordination loop.
fn fold_group_input_single_engine(
    done_group_keys: &mut Vec<Value>,
    max_groups_bound: usize,
    grouped_execution_context: &mut ExecutionContext,
    grouped_engine: &mut Box<dyn GroupedAggregateEngine>,
    row_input: GroupedFoldRowInput<'_>,
) -> Result<(), InternalError> {
    if done_group_keys.iter().any(|done| {
        canonical_value_compare(done, row_input.canonical_group_value) == Ordering::Equal
    }) {
        return Ok(());
    }

    let fold_control = grouped_engine
        .ingest(
            grouped_execution_context,
            row_input.data_key,
            row_input.group_key,
            row_input.row_view,
        )
        .map_err(GroupError::into_internal_error)?;
    if matches!(fold_control, FoldControl::Break) {
        done_group_keys.push(row_input.canonical_group_value.clone());
        debug_assert!(
            done_group_keys.len() <= max_groups_bound,
            "grouped short-circuit key tracking must stay bounded by max_groups",
        );
    }

    Ok(())
}
