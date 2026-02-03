//! IcyDB commit protocol and atomicity guardrails.
//!
//! Contract: once `begin_commit` succeeds, mutations must either complete
//! successfully or roll back before `finish_commit` returns. The commit marker
//! must cover all mutations, and recovery replays index ops before data ops.
//!
//! ## Commit Boundary and Authority of CommitMarker
//!
//! The `CommitMarker` fully specifies every index and data mutation. After
//! the marker is persisted, executors must not re-derive semantics or branch
//! on entity/index contents; apply logic deterministically replays the marker
//! ops. Recovery replays commit ops as recorded, not planner logic.

mod decode;
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
use serde::{Deserialize, Serialize};

pub use recovery::ensure_recovered;

#[cfg(test)]
#[expect(dead_code)]
/// Return true if a commit marker is currently persisted.
pub fn commit_marker_present() -> Result<bool, InternalError> {
    store::commit_marker_present()
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
pub fn finish_commit(
    mut guard: CommitGuard,
    apply: impl FnOnce(&mut CommitGuard) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    // COMMIT WINDOW:
    // Apply must either complete successfully or roll back all mutations before
    // returning an error. We clear the marker on any outcome so recovery does
    // not replay an already-rolled-back write.
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
