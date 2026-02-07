//! IcyDB commit protocol and atomicity guardrails.
//!
//! Contract:
//! - `begin_commit` persists a marker that fully describes durable mutations.
//! - Durable correctness is owned by marker replay in recovery (index ops, then data ops).
//! - In-process apply guards are best-effort cleanup only and are not authoritative.
//!
//! ## Commit Boundary and Authority of CommitMarker
//!
//! The `CommitMarker` fully specifies every index and data mutation. After
//! the marker is persisted, executors must not re-derive semantics or branch
//! on entity/index contents; apply logic deterministically replays the marker
//! ops. Recovery replays commit ops as recorded, not planner logic.

mod decode;
mod guard;
mod memory;
mod recovery;
mod store;
#[cfg(test)]
mod tests;

use crate::{
    db::commit::store::{CommitStore, with_commit_store, with_commit_store_infallible},
    error::{ErrorClass, ErrorOrigin, InternalError},
    types::Ulid,
};
#[cfg(test)]
use canic_memory::{
    registry::{MemoryRegistry, MemoryRegistryError},
    runtime::registry::MemoryRegistryRuntime,
};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::BTreeSet;

pub use guard::CommitApplyGuard;
pub use recovery::{ensure_recovered, ensure_recovered_for_write};

#[cfg(test)]
/// Return true if a commit marker is currently persisted.
pub fn commit_marker_present() -> Result<bool, InternalError> {
    store::commit_marker_present()
}

#[cfg(test)]
/// Initialize commit marker storage for tests.
///
/// This registers a placeholder data-store entry if none exists so the commit
/// memory allocator can select the correct reserved range.
pub fn init_commit_store_for_tests() -> Result<(), InternalError> {
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

    with_commit_store(|_| Ok(()))
}

// Stage-2 invariant:
// - We persist a commit marker before any stable mutation.
// - After marker creation, executor apply phases are infallible or trap.
// - Recovery replays the stored mutation plan (index ops, then data ops).
// This makes partial mutations deterministic without a WAL.

const COMMIT_LABEL: &str = "CommitMarker";
const COMMIT_ID_BYTES: usize = 16;

// Conservative upper bound to avoid rejecting valid commits when index entries
// are large; still small enough to fit typical canister constraints.
pub const MAX_COMMIT_BYTES: u32 = 16 * 1024 * 1024;

///
/// CommitKind
///

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum CommitKind {
    Save,
    Delete,
}

///
/// CommitIndexOp
///
/// Raw index mutation recorded in a commit marker.
/// Carries store identity plus raw key/value bytes.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitIndexOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

///
/// CommitDataOp
///
/// Raw data-store mutation recorded in a commit marker.
/// Carries store identity plus raw key/value bytes.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitDataOp {
    pub store: String,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

///
/// CommitMarker
///
/// Persisted mutation plan covering all index and data operations.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption; commit markers are not forward-compatible.
/// This is internal commit-protocol metadata, not a user-schema type.

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitMarker {
    pub id: [u8; COMMIT_ID_BYTES],
    pub kind: CommitKind,
    pub index_ops: Vec<CommitIndexOp>,
    pub data_ops: Vec<CommitDataOp>,
}

impl CommitMarker {
    /// Construct a new commit marker with a fresh commit id.
    pub fn new(
        kind: CommitKind,
        index_ops: Vec<CommitIndexOp>,
        data_ops: Vec<CommitDataOp>,
    ) -> Result<Self, InternalError> {
        let id = Ulid::try_generate()
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit id generation failed: {err}"),
                )
            })?
            .to_bytes();

        Ok(Self {
            id,
            kind,
            index_ops,
            data_ops,
        })
    }
}

///
/// CommitGuard
///
/// In-flight commit handle that clears the marker on completion.
/// Must not be leaked across mutation boundaries.
///

#[derive(Clone, Debug)]
pub struct CommitGuard {
    pub marker: CommitMarker,
}

impl CommitGuard {
    // Clear the commit marker without surfacing errors.
    fn clear(self) {
        let _ = self;
        with_commit_store_infallible(CommitStore::clear_infallible);
    }
}

/// Persist a commit marker and open the commit window.
pub fn begin_commit(marker: CommitMarker) -> Result<CommitGuard, InternalError> {
    with_commit_store(|store| {
        if store.load()?.is_some() {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Store,
                "commit marker already present before begin",
            ));
        }
        store.set(&marker)?;

        Ok(CommitGuard { marker })
    })
}

/// Apply commit ops and clear the marker regardless of outcome.
///
/// The apply closure performs mechanical marker application only.
/// Any in-process rollback guard used by the closure is non-authoritative
/// transitional cleanup; durable authority remains the commit marker protocol.
pub fn finish_commit(
    mut guard: CommitGuard,
    apply: impl FnOnce(&mut CommitGuard) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    // COMMIT WINDOW:
    // Apply mutates stores from a prevalidated marker payload.
    // Marker durability + recovery replay remain the atomicity authority.
    // We clear the marker on any outcome so recovery does not reapply an
    // already-attempted marker in this process.
    let result = apply(&mut guard);
    let commit_id = guard.marker.id;
    guard.clear();
    // Internal invariant: commit markers must not persist after a finished mutation.
    assert!(
        with_commit_store_infallible(|store| store.is_empty()),
        "commit marker must be cleared after finish_commit (commit_id={commit_id:?})"
    );
    result
}
