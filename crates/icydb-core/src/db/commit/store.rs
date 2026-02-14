//! Commit marker storage and access.

use crate::{
    db::commit::{CommitMarker, MAX_COMMIT_BYTES, memory::commit_memory_id},
    error::{ErrorClass, ErrorOrigin, InternalError},
    serialize::{deserialize_bounded, serialize},
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
            return Err(InternalError::new(
                ErrorClass::Unsupported,
                ErrorOrigin::Store,
                format!(
                    "commit marker exceeds max size: {} bytes (limit {MAX_COMMIT_BYTES})",
                    bytes.len()
                ),
            ));
        }
        Ok(Self(bytes))
    }

    // Deserialize the stored payload, treating failures as corruption.
    fn try_decode(&self) -> Result<Option<CommitMarker>, InternalError> {
        if self.is_empty() {
            return Ok(None);
        }
        if self.0.len() > MAX_COMMIT_BYTES as usize {
            return Err(InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Store,
                format!(
                    "commit marker exceeds max size: {} bytes (limit {MAX_COMMIT_BYTES})",
                    self.0.len()
                ),
            ));
        }

        deserialize_bounded::<CommitMarker>(&self.0, MAX_COMMIT_BYTES as usize)
            .map(Some)
            .map_err(|err| {
                InternalError::new(
                    ErrorClass::Corruption,
                    ErrorOrigin::Store,
                    format!("commit marker corrupted: {err}"),
                )
            })
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

pub struct CommitStore {
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

#[cfg(test)]
mod tests {
    use super::RawCommitMarker;
    use crate::{
        db::{
            commit::{CommitMarker, CommitRowOp, MAX_COMMIT_BYTES},
            store::MAX_ROW_BYTES,
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
            SerializeError::Deserialize(message) => assert!(
                !message.contains("payload exceeds maximum allowed size"),
                "size gate should allow commit marker payloads under MAX_COMMIT_BYTES"
            ),
            SerializeError::Serialize(_) => panic!("unexpected serialize error"),
        }
    }
}
