//! Module: executor::projection::covering
//! Responsibility: executor-owned covering projection execution.
//! Does not own: response envelopes or projected-row DISTINCT finalization.
//! Boundary: consumes prepared access plans and emits structural projection rows.

mod contracts;
mod hybrid;
mod pure;
mod shared;

#[cfg(feature = "diagnostics")]
use std::cell::Cell;
use std::rc::Rc;

use self::contracts::{
    AccessPlannedQuery, CoveringHybridReadExecutionPlan, CoveringReadExecutionPlan,
};
use crate::{
    db::{
        Db,
        cursor::IndexScanContinuationInput,
        executor::{
            EntityAuthority, IndexScan, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            decode_single_covering_projection_pairs, projection::MaterializedProjectionRows,
        },
        index::{
            IndexKey, key_within_envelope, predicate::IndexPredicateExecution,
            raw_keys_for_component_prefix_with_kind,
        },
        predicate::MissingRowPolicy,
    },
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};
use std::ops::Bound;

///
/// PreparedCoveringProjectionRuntime
///
/// Runtime-only covering projection inputs that travel together from the
/// shared prepared plan into pure or hybrid covering execution.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct PreparedCoveringProjectionRuntime<'a> {
    plan: &'a AccessPlannedQuery,
    index_prefix_specs: &'a [LoweredIndexPrefixSpec],
    index_range_specs: &'a [LoweredIndexRangeSpec],
    index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    metrics: CoveringProjectionMetricsRecorder,
}

impl<'a> PreparedCoveringProjectionRuntime<'a> {
    #[must_use]
    pub(in crate::db::executor) const fn new(
        plan: &'a AccessPlannedQuery,
        index_prefix_specs: &'a [LoweredIndexPrefixSpec],
        index_range_specs: &'a [LoweredIndexRangeSpec],
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
        metrics: CoveringProjectionMetricsRecorder,
    ) -> Self {
        Self {
            plan,
            index_prefix_specs,
            index_range_specs,
            index_predicate_execution,
            metrics,
        }
    }
}

///
/// CoveringProjectionMetricsRecorder
///
/// Executor callback bundle for covering projection materialization counters.
/// The executor owns covering projection execution, while the SQL diagnostics
/// adapter owns its counter storage.
///

#[cfg(all(feature = "sql", feature = "diagnostics"))]
#[derive(Clone, Copy)]
pub(in crate::db) struct CoveringProjectionMetricsRecorder {
    path_hit: fn(),
    index_field_access: fn(),
    row_field_access: fn(),
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
const fn ignore_covering_projection_event() {}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
impl CoveringProjectionMetricsRecorder {
    /// Construct one observer from projection materialization counter
    /// callbacks supplied by the response-shaping layer.
    pub(in crate::db) const fn new(
        hybrid_path_hit: fn(),
        hybrid_index_field_access: fn(),
        hybrid_row_field_access: fn(),
    ) -> Self {
        Self {
            path_hit: hybrid_path_hit,
            index_field_access: hybrid_index_field_access,
            row_field_access: hybrid_row_field_access,
        }
    }

    /// Construct one observer that intentionally records no adapter metrics.
    pub(in crate::db) const fn none() -> Self {
        Self::new(
            ignore_covering_projection_event,
            ignore_covering_projection_event,
            ignore_covering_projection_event,
        )
    }

    pub(super) fn record_hybrid_path_hit(self) {
        (self.path_hit)();
    }

    pub(super) fn record_hybrid_index_field_access(self) {
        (self.index_field_access)();
    }

    pub(super) fn record_hybrid_row_field_access(self) {
        (self.row_field_access)();
    }
}

pub(in crate::db::executor) fn try_execute_prepared_covering_projection_rows_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    runtime: PreparedCoveringProjectionRuntime<'_>,
    covering: Option<Rc<CoveringReadExecutionPlan>>,
    hybrid: impl FnOnce() -> Option<Rc<CoveringHybridReadExecutionPlan>>,
) -> Result<Option<MaterializedProjectionRows>, InternalError>
where
    C: CanisterKind,
{
    if let Some(covering) = covering
        && let Some(projected) = pure::try_execute_covering_projection_rows_with_plan_for_canister(
            db,
            authority.clone(),
            runtime.plan,
            runtime.index_prefix_specs,
            runtime.index_range_specs,
            &covering,
            runtime.index_predicate_execution,
        )?
    {
        return Ok(Some(MaterializedProjectionRows::from_value_rows(projected)));
    }

    let Some(hybrid) = hybrid() else {
        return Ok(None);
    };

    hybrid::try_execute_hybrid_covering_projection_rows_with_plan_for_canister(
        db, authority, runtime, &hybrid,
    )
    .map(|projected| projected.map(MaterializedProjectionRows::from_value_rows))
}

/// Visit one representative per component-zero group under a planner proof.
pub(in crate::db::executor) fn try_execute_ordered_distinct_group_seek_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    index_range_specs: &[LoweredIndexRangeSpec],
    covering: &CoveringReadExecutionPlan,
) -> Result<Option<Vec<Value>>, InternalError>
where
    C: CanisterKind,
{
    let Some(contract) = covering.ordered_distinct_group_seek_contract() else {
        return Ok(None);
    };
    let direction = contract.direction();
    let [range] = index_range_specs else {
        return Err(InternalError::query_executor_invariant());
    };
    let scan_contract = range.scan_contract();
    let store = db.recovered_store(scan_contract.store_path())?;
    let mut lower = range.lower().clone();
    let mut upper = range.upper().clone();
    let mut representatives = Vec::with_capacity(contract.representative_budget());

    while representatives.len() < contract.representative_budget() {
        let chunk = IndexScan::components_chunk_structural(
            store,
            authority.entity_tag(),
            &scan_contract,
            &lower,
            &upper,
            IndexScanContinuationInput::new(None, direction),
            1,
            Some(1),
            &[0],
            None,
        )?;
        let (mut rows, raw_anchor) = chunk.into_component_rows_and_resume_anchor();
        let Some(row) = rows.pop() else {
            break;
        };
        let raw_anchor = raw_anchor.ok_or_else(InternalError::query_executor_invariant)?;
        let decoded_anchor = IndexKey::try_from_raw(&raw_anchor).map_err(|error| {
            InternalError::index_scan_key_corrupted_during("DISTINCT group seek", error)
        })?;
        let [projected_component] = row.2.as_ref() else {
            return Err(InternalError::query_executor_invariant());
        };
        if decoded_anchor.component(0) != Some(projected_component.as_slice()) {
            return Err(InternalError::index_entry_decode_failed());
        }
        let (group_low, group_high) = raw_keys_for_component_prefix_with_kind(
            decoded_anchor.index_id(),
            decoded_anchor.key_kind(),
            decoded_anchor.component_count(),
            row.2.as_ref(),
        )
        .map_err(|_| InternalError::query_executor_invariant())?;
        let value = decode_single_covering_projection_pairs(
            vec![row],
            store,
            MissingRowPolicy::Error,
            covering.existing_row_mode,
            Ok::<Value, InternalError>,
        )?
        .and_then(|mut decoded| decoded.pop())
        .map(|(_data_key, value)| value)
        .ok_or_else(InternalError::query_executor_invariant)?;
        representatives.push(value);
        let boundary = match direction {
            crate::db::direction::Direction::Asc => group_high,
            crate::db::direction::Direction::Desc => group_low,
        };
        if !key_within_envelope(&boundary, &lower, &upper) {
            break;
        }
        match direction {
            crate::db::direction::Direction::Asc => lower = Bound::Excluded(boundary),
            crate::db::direction::Direction::Desc => upper = Bound::Excluded(boundary),
        }
    }

    Ok(Some(representatives))
}

///
/// CoveringProjectionMetricsRecorder
///
/// Zero-sized no-op recorder used when SQL materialization diagnostics are not
/// compiled. Keeping the type available avoids cfg-heavy executor signatures.
///

#[cfg(not(all(feature = "sql", feature = "diagnostics")))]
#[derive(Clone, Copy)]
pub(in crate::db) struct CoveringProjectionMetricsRecorder;

#[cfg(not(all(feature = "sql", feature = "diagnostics")))]
impl CoveringProjectionMetricsRecorder {
    pub(in crate::db) const fn new() -> Self {
        Self
    }

    pub(in crate::db) const fn none() -> Self {
        Self::new()
    }

    pub(super) const fn record_hybrid_path_hit(self) {
        let _ = self;
    }

    pub(super) const fn record_hybrid_index_field_access(self) {
        let _ = self;
    }

    pub(super) const fn record_hybrid_row_field_access(self) {
        let _ = self;
    }
}

#[cfg(feature = "diagnostics")]
std::thread_local! {
    static PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS: Cell<u64> = const { Cell::new(0) };
    static PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_pure_covering_decode_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS.with(|counter| {
        counter.set(counter.get().saturating_add(delta));
    });
}

#[cfg(feature = "diagnostics")]
pub(super) fn record_pure_covering_row_assembly_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS.with(|counter| {
        counter.set(counter.get().saturating_add(delta));
    });
}

#[cfg(feature = "diagnostics")]
pub(in crate::db) fn current_pure_covering_decode_local_instructions() -> u64 {
    PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS.with(Cell::get)
}

#[cfg(feature = "diagnostics")]
pub(in crate::db) fn current_pure_covering_row_assembly_local_instructions() -> u64 {
    PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS.with(Cell::get)
}
