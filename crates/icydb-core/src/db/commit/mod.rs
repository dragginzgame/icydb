//! Module: commit
//! Responsibility: durable commit-marker protocol and recovery authority boundaries.
//! Does not own: query planning, index encoding semantics, or predicate semantics.
//! Boundary: executor::mutation -> commit (one-way).
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
mod guard;
mod hooks;
mod marker;
mod memory;
mod prepare;
mod prepared_op;
mod rebuild;
mod recovery;
mod replay;
mod rollback;
mod store;
#[cfg(test)]
mod tests;

use crate::error::InternalError;
#[cfg(test)]
use crate::testing::{TEST_MEMORY_RANGE_END, TEST_MEMORY_RANGE_START, test_commit_memory_id};
#[cfg(test)]
use canic_memory::{registry::MemoryRegistryError, runtime::registry::MemoryRegistryRuntime};

///
/// Re-exports
///
pub(in crate::db) use guard::{
    CommitApplyGuard, CommitGuard, begin_commit, begin_commit_with_migration_state,
    begin_single_row_commit, finish_commit,
};
pub use hooks::EntityRuntimeHooks;
#[cfg(debug_assertions)]
pub(in crate::db) use hooks::debug_assert_unique_runtime_hook_tags;
pub(in crate::db) use hooks::{
    has_runtime_hooks, resolve_runtime_hook_by_path, resolve_runtime_hook_by_tag,
};
pub(in crate::db) use marker::CommitRowOp;
pub(in crate::db) use marker::{
    COMMIT_MARKER_FORMAT_VERSION_CURRENT, CommitIndexOp, CommitMarker, CommitSchemaFingerprint,
    MAX_COMMIT_BYTES, decode_commit_marker_payload, decode_data_key, encode_commit_marker_payload,
    validate_commit_marker_shape,
};
pub(in crate::db) use prepare::{
    prepare_row_commit_for_entity, prepare_row_commit_for_entity_with_readers,
    prepare_row_commit_for_entity_with_structural_readers,
};
pub(in crate::db) use prepared_op::{
    PreparedIndexDeltaKind, PreparedIndexMutation, PreparedRowCommitOp,
};
pub(in crate::db) use rebuild::rebuild_secondary_indexes_from_rows;
pub(in crate::db) use recovery::ensure_recovered;
pub(in crate::db) use replay::replay_commit_marker_row_ops;
pub(in crate::db) use rollback::{
    rollback_prepared_row_ops_reverse, snapshot_row_only_rollback, snapshot_row_rollback,
};

/// Return true if a commit marker is currently persisted.
#[cfg(test)]
pub(in crate::db) fn commit_marker_present() -> Result<bool, InternalError> {
    store::commit_marker_present()
}

/// Clear the persisted commit marker in tests.
#[cfg(test)]
pub(in crate::db) fn clear_commit_marker_for_tests() -> Result<(), InternalError> {
    store::with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
}

/// Load persisted migration-state bytes from the shared commit control slot.
pub(in crate::db) fn load_migration_state_bytes() -> Result<Option<Vec<u8>>, InternalError> {
    store::with_commit_store(|store| store.load_migration_state_bytes())
}

/// Clear persisted migration-state bytes from the shared commit control slot.
pub(in crate::db) fn clear_migration_state_bytes() -> Result<(), InternalError> {
    store::with_commit_store(store::CommitStore::clear_migration_state_bytes)
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
            MemoryRegistryRuntime::init(None)
                .map_err(InternalError::commit_memory_registry_init_failed)?;
        }
        Err(err) => {
            return Err(InternalError::commit_memory_registry_init_failed(err));
        }
    }

    // Phase 2: pin and register the explicit commit marker slot.
    memory::configure_commit_memory_id(test_commit_memory_id())?;

    // Phase 3: initialize the commit store in the configured slot.
    store::with_commit_store(|_| Ok(()))
}
