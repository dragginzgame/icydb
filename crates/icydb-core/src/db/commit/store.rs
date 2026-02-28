//! Commit marker storage and access.

use crate::{
    db::{
        codec::deserialize_persisted_payload,
        commit::{
            CommitMarker, MAX_COMMIT_BYTES, commit_corruption, memory::commit_memory_id,
            validate_commit_marker_shape,
        },
    },
    error::InternalError,
    serialize::serialize,
};
use canic_cdk::structures::{
    Cell as StableCell, DefaultMemoryImpl, Storable,
    memory::{MemoryId, VirtualMemory},
    storable::Bound,
};
use canic_memory::MEMORY_MANAGER;
use std::{borrow::Cow, cell::RefCell};

///
/// RawCommitMarker
/// Raw, bounded commit marker bytes stored in stable memory.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawCommitMarker(Vec<u8>);

impl RawCommitMarker {
    const fn empty() -> Self {
        Self(Vec::new())
    }

    const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    // Serialize and bound-check a commit marker payload.
    fn try_from_marker(marker: &CommitMarker) -> Result<Self, InternalError> {
        let bytes = serialize(marker)?;
        if bytes.len() > MAX_COMMIT_BYTES as usize {
            return Err(InternalError::store_unsupported(format!(
                "commit marker exceeds max size: {} bytes (limit {MAX_COMMIT_BYTES})",
                bytes.len()
            )));
        }
        Ok(Self(bytes))
    }

    // Deserialize the stored payload, treating failures as corruption.
    fn try_decode(&self) -> Result<Option<CommitMarker>, InternalError> {
        if self.is_empty() {
            return Ok(None);
        }
        if self.0.len() > MAX_COMMIT_BYTES as usize {
            return Err(commit_corruption(format!(
                "commit marker exceeds max size: {} bytes (limit {MAX_COMMIT_BYTES})",
                self.0.len()
            )));
        }

        let marker = deserialize_persisted_payload::<CommitMarker>(
            &self.0,
            MAX_COMMIT_BYTES as usize,
            "commit marker",
        )
        .map_err(commit_corruption)?;
        validate_commit_marker_shape(&marker)?;

        Ok(Some(marker))
    }
}

impl Storable for RawCommitMarker {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_COMMIT_BYTES,
        is_fixed_size: false,
    };
}

///
/// CommitStore
/// Stable-cell wrapper for commit marker storage.
///

pub(super) struct CommitStore {
    cell: StableCell<RawCommitMarker, VirtualMemory<DefaultMemoryImpl>>,
}

impl CommitStore {
    fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        let cell = StableCell::init(memory, RawCommitMarker::empty());
        Self { cell }
    }

    pub(super) fn load(&self) -> Result<Option<CommitMarker>, InternalError> {
        self.cell.get().try_decode()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.cell.get().is_empty()
    }

    pub(super) fn set(&mut self, marker: &CommitMarker) -> Result<(), InternalError> {
        let raw = RawCommitMarker::try_from_marker(marker)?;
        self.cell.set(raw);
        Ok(())
    }

    pub(super) fn clear_infallible(&mut self) {
        self.cell.set(RawCommitMarker::empty());
    }
}

thread_local! {
    static COMMIT_STORE: RefCell<Option<CommitStore>> = const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn commit_marker_present() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(store.load()?.is_some()))
}

// Lazily initialize and access the commit marker store.
pub(super) fn with_commit_store<R>(
    f: impl FnOnce(&mut CommitStore) -> Result<R, InternalError>,
) -> Result<R, InternalError> {
    COMMIT_STORE.with(|cell| {
        if cell.borrow().is_none() {
            // StableCell::init performs a benign stable write for the empty marker.
            let store = CommitStore::init(commit_memory()?);
            *cell.borrow_mut() = Some(store);
        }
        let mut guard = cell.borrow_mut();
        let store = guard.as_mut().expect("commit store missing after init");
        f(store)
    })
}

// Fast, observational check for marker presence without decoding.
pub(super) fn commit_marker_present_fast() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(!store.is_empty()))
}

/// Clear the persisted commit marker after successful replay and index rebuild.
///
/// This semantic boundary keeps marker-clear intent explicit in recovery flow.
pub(super) fn clear_commit_marker_after_successful_rebuild() -> Result<(), InternalError> {
    with_commit_store(|store| {
        store.clear_infallible();
        Ok(())
    })
}

// Access the commit store without fallible initialization.
pub(super) fn with_commit_store_infallible<R>(f: impl FnOnce(&mut CommitStore) -> R) -> R {
    COMMIT_STORE.with(|cell| {
        let mut guard = cell.borrow_mut();
        let store = guard.as_mut().expect("commit store not initialized");
        f(store)
    })
}

// Resolve the virtual memory backing the commit marker store.
fn commit_memory() -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    let id = commit_memory_id()?;
    Ok(MEMORY_MANAGER.with_borrow_mut(|mgr| mgr.get(MemoryId::new(id))))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::RawCommitMarker;
    use crate::{
        db::{
            codec::MAX_ROW_BYTES,
            commit::{CommitMarker, CommitRowOp, MAX_COMMIT_BYTES},
            data::DataKey,
        },
        error::{ErrorClass, ErrorOrigin},
        serialize::{SerializeError, deserialize_bounded, serialize},
    };
    use serde::Serialize;

    // Test helper: commit marker with an extra field to exercise strict decode.
    #[derive(Serialize)]
    struct CommitMarkerWithExtra {
        id: [u8; 16],
        row_ops: Vec<CommitRowOp>,
        extra: u8,
    }

    #[test]
    fn commit_marker_rejects_unknown_fields() {
        let marker = CommitMarkerWithExtra {
            id: [0u8; 16],
            row_ops: Vec::new(),
            extra: 1,
        };

        let bytes = serialize(&marker).expect("serialize marker with extra field");
        let err = RawCommitMarker(bytes)
            .try_decode()
            .expect_err("unknown field should be rejected");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }

    #[test]
    fn commit_marker_bounded_decode_allows_over_row_limit() {
        let len = (MAX_ROW_BYTES as usize).saturating_add(1);
        let bytes = vec![0xFF; len];
        let err = deserialize_bounded::<CommitMarker>(&bytes, MAX_COMMIT_BYTES as usize)
            .expect_err("invalid CBOR should fail decode");

        match err {
            SerializeError::DeserializeSizeLimitExceeded { .. } => {
                panic!("size gate should allow commit marker payloads under MAX_COMMIT_BYTES")
            }
            SerializeError::Deserialize(_) => {}
            SerializeError::Serialize(_) => panic!("unexpected serialize error"),
        }
    }

    #[test]
    fn commit_marker_rejects_oversized_stored_payload_as_corruption() {
        let len = (MAX_COMMIT_BYTES as usize).saturating_add(1);
        let err = RawCommitMarker(vec![0; len])
            .try_decode()
            .expect_err("oversized persisted marker should be rejected");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message.contains("commit marker exceeds max size"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn commit_marker_rejects_oversized_payload_before_persist() {
        let oversized_after = vec![0u8; MAX_COMMIT_BYTES as usize + 1];
        let marker = CommitMarker {
            id: [2u8; 16],
            row_ops: vec![CommitRowOp::new(
                "test::Entity",
                vec![1u8],
                None,
                Some(oversized_after),
            )],
        };

        let err = RawCommitMarker::try_from_marker(&marker)
            .expect_err("oversized marker payload must be rejected before persist");

        assert_eq!(err.class, ErrorClass::Unsupported);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message.contains("commit marker exceeds max size"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn commit_marker_rejects_row_op_without_before_or_after() {
        let marker = CommitMarker {
            id: [1u8; 16],
            row_ops: vec![CommitRowOp::new("test::Entity", vec![9u8], None, None)],
        };

        let bytes = serialize(&marker).expect("serialize malformed marker");
        let err = RawCommitMarker(bytes)
            .try_decode()
            .expect_err("row op without before/after should be rejected");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message
                .contains("row op has neither before nor after payload"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn commit_marker_rejects_row_op_with_empty_entity_path() {
        let marker = CommitMarker {
            id: [3u8; 16],
            row_ops: vec![CommitRowOp::new("", vec![9u8], Some(vec![1u8]), None)],
        };

        let bytes = serialize(&marker).expect("serialize malformed marker");
        let err = RawCommitMarker(bytes)
            .try_decode()
            .expect_err("row op with empty entity path should be rejected");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message.contains("row op has empty entity_path"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn commit_marker_rejects_row_op_with_invalid_key_length() {
        let marker = CommitMarker {
            id: [4u8; 16],
            row_ops: vec![CommitRowOp::new(
                "test::Entity",
                vec![9u8],
                Some(vec![1u8]),
                None,
            )],
        };

        let bytes = serialize(&marker).expect("serialize malformed marker");
        let err = RawCommitMarker(bytes)
            .try_decode()
            .expect_err("row op with invalid key length should be rejected");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message.contains("row op key has invalid length"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn commit_marker_rejects_row_op_with_invalid_key_shape() {
        let marker = CommitMarker {
            id: [5u8; 16],
            row_ops: vec![CommitRowOp::new(
                "test::Entity",
                vec![0u8; DataKey::STORED_SIZE_USIZE],
                Some(vec![1u8]),
                None,
            )],
        };

        let bytes = serialize(&marker).expect("serialize malformed marker");
        let err = RawCommitMarker(bytes)
            .try_decode()
            .expect_err("row op with invalid key shape should be rejected");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message.contains("row op key decode failed"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn commit_marker_rejects_row_op_with_oversized_payload() {
        let marker = CommitMarker {
            id: [6u8; 16],
            row_ops: vec![CommitRowOp::new(
                "test::Entity",
                vec![9u8],
                Some(vec![0u8; MAX_ROW_BYTES as usize + 1]),
                None,
            )],
        };

        let bytes = serialize(&marker).expect("serialize malformed marker");
        let err = RawCommitMarker(bytes)
            .try_decode()
            .expect_err("row op with oversized payload should be rejected");

        assert_eq!(err.class, ErrorClass::Corruption);
        assert_eq!(err.origin, ErrorOrigin::Store);
        assert!(
            err.message.contains("payload exceeds max size"),
            "unexpected error: {err:?}"
        );
    }
}
