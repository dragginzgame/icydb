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

#[cfg(test)]
use crate::error::{ErrorClass, ErrorOrigin, InternalError};
#[cfg(test)]
use canic_memory::{
    registry::{MemoryRegistry, MemoryRegistryError},
    runtime::registry::MemoryRegistryRuntime,
};
#[cfg(test)]
use std::collections::BTreeSet;
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

#[cfg(test)]
/// Return true if a commit marker is currently persisted.
pub(in crate::db) fn commit_marker_present() -> Result<bool, InternalError> {
    store::commit_marker_present()
}

#[cfg(test)]
/// Initialize commit marker storage for tests.
///
/// This registers a placeholder data-store entry if none exists so the commit
/// memory allocator can select the correct reserved range.
pub(in crate::db) fn init_commit_store_for_tests() -> Result<(), InternalError> {
    // Phase 1: ensure the memory registry has at least one reserved range.
    let init_result = MemoryRegistryRuntime::init(Some(("icydb_test", 1, 200)));
    match init_result {
        Ok(_) => {}
        Err(MemoryRegistryError::Overlap { .. }) => {
            MemoryRegistryRuntime::init(None).map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("memory registry init failed: {err}"),
                )
            })?;
        }
        Err(err) => {
            return Err(InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                format!("memory registry init failed: {err}"),
            ));
        }
    }

    // Phase 2: ensure a DB-store entry exists so commit memory can be allocated.
    let snapshots = MemoryRegistryRuntime::snapshot_ids_by_range();
    if snapshots.is_empty() {
        return Err(InternalError::new(
            ErrorClass::Internal,
            ErrorOrigin::Store,
            "no memory ranges available for commit marker tests",
        ));
    }
    let has_store_entry = snapshots.iter().any(|snapshot| {
        snapshot.entries.iter().any(|(_, entry)| {
            entry.label.ends_with("DataStore") || entry.label.ends_with("IndexStore")
        })
    });

    if !has_store_entry {
        let snapshot = snapshots.first().ok_or_else(|| {
            InternalError::new(
                ErrorClass::Internal,
                ErrorOrigin::Store,
                "no memory ranges available for commit marker tests",
            )
        })?;
        let used_ids = snapshot
            .entries
            .iter()
            .map(|(id, _)| *id)
            .collect::<BTreeSet<_>>();
        let dummy_id = (snapshot.range.start..=snapshot.range.end)
            .find(|id| !used_ids.contains(id))
            .ok_or_else(|| {
                InternalError::new(
                    ErrorClass::Unsupported,
                    ErrorOrigin::Store,
                    format!(
                        "no free memory ids available for commit marker tests in range {}-{}",
                        snapshot.range.start, snapshot.range.end
                    ),
                )
            })?;

        MemoryRegistry::register(dummy_id, &snapshot.owner, "commit_test::DataStore").map_err(
            |err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit test memory registration failed: {err}"),
                )
            },
        )?;
    }

    // Phase 3: initialize the commit store in the production slot.
    store::with_commit_store(|_| Ok(()))
}
