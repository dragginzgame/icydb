//! Module: executor::projection::covering
//! Responsibility: executor-owned covering projection execution.
//! Does not own: response envelopes or projected-row DISTINCT finalization.
//! Boundary: consumes prepared access plans and emits structural projection rows.

mod hybrid;
mod pure;
mod shared;

#[cfg(all(feature = "sql", feature = "diagnostics"))]
use std::cell::Cell;

use crate::{
    db::{Db, executor::EntityAuthority, query::plan::AccessPlannedQuery},
    error::InternalError,
    traits::CanisterKind,
    value::Value,
};

///
/// CoveringProjectionRows
///
/// Opaque transport wrapper for executor-owned covering projection results.
/// This exists to keep executor APIs structural while letting adapter layers
/// consume the projected value matrix for their own response shaping.
///

#[cfg(feature = "sql")]
#[derive(Debug)]
pub(in crate::db) struct CoveringProjectionRows(Vec<Vec<Value>>);

#[cfg(feature = "sql")]
impl CoveringProjectionRows {
    /// Construct one covering projection row payload from executor-owned rows.
    pub(super) const fn new(rows: Vec<Vec<Value>>) -> Self {
        Self(rows)
    }

    /// Consume this structural wrapper into the row values used by adapters.
    #[must_use]
    pub(in crate::db) fn into_value_rows(self) -> Vec<Vec<Value>> {
        self.0
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

#[cfg(feature = "sql")]
pub(in crate::db) fn try_execute_covering_projection_rows_for_canister<C>(
    db: &Db<C>,
    authority: EntityAuthority,
    plan: &AccessPlannedQuery,
    metrics: CoveringProjectionMetricsRecorder,
) -> Result<Option<CoveringProjectionRows>, InternalError>
where
    C: CanisterKind,
{
    if let Some(projected) =
        pure::try_execute_covering_projection_rows_for_canister(db, authority, plan)?
    {
        return Ok(Some(CoveringProjectionRows::new(projected)));
    }

    hybrid::try_execute_hybrid_covering_projection_rows_for_canister(db, authority, plan, metrics)
        .map(|projected| projected.map(CoveringProjectionRows::new))
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

#[cfg(all(feature = "sql", feature = "diagnostics", target_arch = "wasm32"))]
fn read_local_instruction_counter() -> u64 {
    canic_cdk::api::performance_counter(1)
}

#[cfg(all(feature = "sql", feature = "diagnostics", not(target_arch = "wasm32")))]
const fn read_local_instruction_counter() -> u64 {
    0
}

#[cfg(all(feature = "sql", feature = "diagnostics"))]
pub(super) fn measure_structural_result<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> (u64, Result<T, E>) {
    let start = read_local_instruction_counter();
    let result = run();
    let delta = read_local_instruction_counter().saturating_sub(start);

    (delta, result)
}

#[cfg(feature = "sql")]
pub(super) fn apply_projection_page_window<T>(rows: &mut Vec<T>, offset: u32, limit: Option<u32>) {
    let offset = usize::min(rows.len(), usize::try_from(offset).unwrap_or(usize::MAX));
    if offset > 0 {
        rows.drain(..offset);
    }

    if let Some(limit) = limit {
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        if rows.len() > limit {
            rows.truncate(limit);
        }
    }
}
