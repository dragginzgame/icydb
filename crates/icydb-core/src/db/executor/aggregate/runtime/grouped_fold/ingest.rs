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
            group::{CanonicalKey, GroupKey, KeyCanonicalError},
            pipeline::contracts::{GroupedRouteStage, GroupedStreamStage},
        },
    },
    error::InternalError,
    value::Value,
};

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
        let group_key = Value::List(group_values)
            .canonical_key()
            .map_err(KeyCanonicalError::into_internal_error)?;
        fold_group_input_with_engines(
            short_circuit_keys,
            group_key.canonical_value(),
            max_groups_bound,
            grouped_execution_context,
            grouped_engines,
            &data_key,
            &group_key,
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
    canonical_group_value: &Value,
    max_groups_bound: usize,
    grouped_execution_context: &mut ExecutionContext,
    grouped_engines: &mut [Box<dyn GroupedAggregateEngine>],
    data_key: &DataKey,
    group_key: &GroupKey,
) -> Result<(), InternalError> {
    for (index, done_group_keys) in short_circuit_keys.iter_mut().enumerate() {
        if done_group_keys
            .iter()
            .any(|done| canonical_value_compare(done, canonical_group_value) == Ordering::Equal)
        {
            continue;
        }

        let Some(engine) = grouped_engines.get_mut(index) else {
            return Err(crate::db::error::query_executor_invariant(format!(
                "grouped engine index out of bounds during fold ingest: index={index}, engine_count={}",
                grouped_engines.len()
            )));
        };
        let fold_control = engine
            .ingest(grouped_execution_context, data_key, group_key)
            .map_err(GroupError::into_internal_error)?;
        if matches!(fold_control, FoldControl::Break) {
            done_group_keys.push(canonical_group_value.clone());
            debug_assert!(
                done_group_keys.len() <= max_groups_bound,
                "grouped short-circuit key tracking must stay bounded by max_groups",
            );
        }
    }

    Ok(())
}
