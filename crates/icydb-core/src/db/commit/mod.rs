//! IcyDB commit protocol and atomicity guardrails.
//!
//! Contract:
//! - `begin_commit` persists a marker that fully describes durable mutations.
//! - Durable correctness is owned by marker replay in recovery (row ops).
//! - In-process apply guards are best-effort cleanup only and are not authoritative.
//!
//! ## Commit Boundary and Authority of CommitMarker
//!
//! The `CommitMarker` fully specifies every row mutation. After
//! the marker is persisted, executors must not re-derive semantics or branch
//! on entity/index contents; apply logic deterministically replays row ops.
//! Recovery replays row ops as recorded, not planner logic.

mod apply;
mod decode;
mod guard;
mod marker;
mod memory;
mod prepare;
mod recovery;
mod rollback;
mod store;
#[cfg(test)]
mod tests;
mod validate;

use crate::error::InternalError;
#[cfg(test)]
use crate::test_support::{TEST_MEMORY_RANGE_END, TEST_MEMORY_RANGE_START, test_commit_memory_id};
#[cfg(test)]
use canic_memory::{registry::MemoryRegistryError, runtime::registry::MemoryRegistryRuntime};
use std::fmt::Display;

///
/// Re-exports
///
pub(in crate::db) use apply::{PreparedIndexMutation, PreparedRowCommitOp};
pub(in crate::db) use guard::{CommitApplyGuard, CommitGuard, begin_commit, finish_commit};
pub(in crate::db) use marker::CommitRowOp;
pub(in crate::db) use marker::{CommitIndexOp, CommitMarker, MAX_COMMIT_BYTES};
pub(in crate::db) use prepare::prepare_row_commit_for_entity;
pub(in crate::db) use recovery::{ensure_recovered, ensure_recovered_for_write};
pub(in crate::db) use rollback::{rollback_prepared_row_ops_reverse, snapshot_row_rollback};
pub(in crate::db) use validate::validate_commit_marker_shape;

/// Build a standard commit-marker corruption message.
pub(in crate::db) fn commit_corruption_message(detail: impl Display) -> String {
    format!("commit marker corrupted: {detail}")
}

/// Build a standard commit-marker component corruption message.
pub(in crate::db) fn commit_component_corruption_message(
    component: &str,
    detail: impl Display,
) -> String {
    format!("commit marker {component} corrupted: {detail}")
}

/// Construct a store-corruption `InternalError` for commit-marker failures.
pub(in crate::db) fn commit_corruption(detail: impl Display) -> InternalError {
    InternalError::store_corruption(commit_corruption_message(detail))
}

/// Construct a store-corruption `InternalError` for commit-marker component failures.
pub(in crate::db) fn commit_component_corruption(
    component: &str,
    detail: impl Display,
) -> InternalError {
    InternalError::store_corruption(commit_component_corruption_message(component, detail))
}

/// Return true if a commit marker is currently persisted.
#[cfg(test)]
pub(in crate::db) fn commit_marker_present() -> Result<bool, InternalError> {
    store::commit_marker_present()
}

/// Initialize commit marker storage for tests.
///
/// Tests reserve a dedicated range and pin the commit marker slot to one
/// canonical id managed by `test_support`.
#[cfg(test)]
pub(in crate::db) fn init_commit_store_for_tests() -> Result<(), InternalError> {
    // Phase 1: ensure the memory registry has at least one reserved range.
    let init_result = MemoryRegistryRuntime::init(Some((
        "icydb_test",
        TEST_MEMORY_RANGE_START,
        TEST_MEMORY_RANGE_END,
    )));
    match init_result {
        Ok(_) => {}
        Err(MemoryRegistryError::Overlap { .. }) => {
            MemoryRegistryRuntime::init(None).map_err(|err| {
                InternalError::store_internal(format!("memory registry init failed: {err}"))
            })?;
        }
        Err(err) => {
            return Err(InternalError::store_internal(format!(
                "memory registry init failed: {err}"
            )));
        }
    }

    // Phase 2: pin and register the explicit commit marker slot.
    memory::configure_commit_memory_id(test_commit_memory_id())?;

    // Phase 3: initialize the commit store in the configured slot.
    store::with_commit_store(|_| Ok(()))
}
