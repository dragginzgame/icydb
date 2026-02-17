use crate::{
    error::{ErrorClass, ErrorOrigin, InternalError},
    types::Ulid,
};
use serde::{Deserialize, Serialize};

// Stage-2 invariant:
// - We persist a commit marker before any stable mutation.
// - After marker creation, executor apply phases are infallible or trap.
// - Recovery replays the stored row mutation plan.
// This makes partial mutations deterministic without a WAL.

pub(crate) const COMMIT_LABEL: &str = "CommitMarker";
const COMMIT_ID_BYTES: usize = 16;

// Conservative upper bound to avoid rejecting valid commits when index entries
// are large; still small enough to fit typical canister constraints.
pub(crate) const MAX_COMMIT_BYTES: u32 = 16 * 1024 * 1024;

///
/// CommitRowOp
///
/// Row-level mutation recorded in a commit marker.
/// Store identity is derived from `entity_path` at apply/recovery time.
///

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(in crate::db) struct CommitRowOp {
    pub(crate) entity_path: String,
    pub(crate) key: Vec<u8>,
    pub(crate) before: Option<Vec<u8>>,
    pub(crate) after: Option<Vec<u8>>,
}

impl CommitRowOp {
    /// Construct a row-level commit operation.
    #[must_use]
    pub(crate) fn new(
        entity_path: impl Into<String>,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
    ) -> Self {
        Self {
            entity_path: entity_path.into(),
            key,
            before,
            after,
        }
    }
}

///
/// CommitIndexOp
///
/// Internal index mutation used during row-op preparation/apply.
/// Not persisted in commit markers.
///

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitIndexOp {
    pub(crate) store: String,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Option<Vec<u8>>,
}

///
/// CommitMarker
///
/// Persisted mutation plan covering row-level operations.
/// Recovery replays the marker exactly as stored.
/// Unknown fields are rejected as corruption; commit markers are not forward-compatible.
/// This is internal commit-protocol metadata, not a user-schema type.
///

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CommitMarker {
    pub(crate) id: [u8; COMMIT_ID_BYTES],
    pub(crate) row_ops: Vec<CommitRowOp>,
}

impl CommitMarker {
    /// Construct a new commit marker with a fresh commit id.
    pub(crate) fn new(row_ops: Vec<CommitRowOp>) -> Result<Self, InternalError> {
        let id = Ulid::try_generate()
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Internal,
                    ErrorOrigin::Store,
                    format!("commit id generation failed: {err}"),
                )
            })?
            .to_bytes();

        Ok(Self { id, row_ops })
    }
}
