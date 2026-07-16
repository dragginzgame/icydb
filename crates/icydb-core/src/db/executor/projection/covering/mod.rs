//! Module: executor::projection::covering
//! Responsibility: executor-owned covering projection execution.
//! Does not own: response envelopes or projected-row DISTINCT finalization.
//! Boundary: consumes prepared access plans and emits structural projection rows.

mod contracts;
mod hybrid;
mod pure;
mod shared;

#[cfg(all(feature = "sql", feature = "diagnostics"))]
use std::cell::Cell;
use std::rc::Rc;

use self::contracts::{
    AccessPlannedQuery, CoveringHybridReadExecutionPlan, CoveringReadExecutionPlan,
};
use crate::{
    db::{
        Db,
        executor::{
            EntityAuthority, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            projection::MaterializedProjectionRows,
        },
        index::predicate::IndexPredicateExecution,
    },
    error::InternalError,
    traits::CanisterKind,
};

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
/// The executor owns covering projection execution, while the adapter layer
/// still owns its test/diagnostic counter storage.
///

#[cfg(any(test, feature = "diagnostics"))]
#[derive(Clone, Copy)]
pub(in crate::db) struct CoveringProjectionMetricsRecorder {
    path_hit: fn(),
    index_field_access: fn(),
    row_field_access: fn(),
}

#[cfg(any(test, feature = "diagnostics"))]
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

///
/// CoveringProjectionMetricsRecorder
///
/// Zero-sized no-op recorder used when materialization diagnostics are not
/// compiled. Keeping the type available avoids cfg-heavy executor signatures.
///

#[cfg(not(any(test, feature = "diagnostics")))]
#[derive(Clone, Copy)]
pub(in crate::db) struct CoveringProjectionMetricsRecorder;

#[cfg(not(any(test, feature = "diagnostics")))]
impl CoveringProjectionMetricsRecorder {
    pub(in crate::db) const fn new() -> Self {
        Self
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

#[cfg(all(feature = "sql", feature = "diagnostics"))]
std::thread_local! {
    static PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS: Cell<u64> = const { Cell::new(0) };
    static PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(super) fn record_pure_covering_decode_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS.with(|counter| {
        counter.set(counter.get().saturating_add(delta));
    });
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(super) fn record_pure_covering_row_assembly_local_instructions(delta: u64) {
    if delta == 0 {
        return;
    }

    PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS.with(|counter| {
        counter.set(counter.get().saturating_add(delta));
    });
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) fn current_pure_covering_decode_local_instructions() -> u64 {
    PURE_COVERING_DECODE_LOCAL_INSTRUCTIONS.with(Cell::get)
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(in crate::db) fn current_pure_covering_row_assembly_local_instructions() -> u64 {
    PURE_COVERING_ROW_ASSEMBLY_LOCAL_INSTRUCTIONS.with(Cell::get)
}
