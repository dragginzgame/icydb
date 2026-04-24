//! Module: commit::store
//! Responsibility: persist, load, and clear commit markers in stable memory.
//! Does not own: marker shape semantics, recovery orchestration, or commit-window policy.
//! Boundary: commit::{guard,recovery} -> commit::store (one-way).

mod bytes;
mod control_slot;
mod marker_envelope;
#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::db::commit::marker::{
    COMMIT_MARKER_FORMAT_VERSION_CURRENT, encode_commit_marker_payload,
};
#[cfg(test)]
use crate::db::commit::store::marker_envelope::encode_commit_marker_bytes;
use crate::{
    db::commit::{
        marker::{CommitMarker, CommitRowOp, MAX_COMMIT_BYTES, validate_commit_marker_shape},
        memory::commit_memory_handle,
        store::{
            control_slot::{
                current_control_slot_migration_len, decode_commit_control_slot,
                encode_commit_control_slot, encode_commit_control_slot_from_marker,
                encode_single_row_commit_control_slot, inspect_commit_control_slot,
            },
            marker_envelope::decode_commit_marker,
        },
    },
    error::InternalError,
};
use canic_cdk::structures::{
    Cell as StableCell, DefaultMemoryImpl, Storable, memory::VirtualMemory, storable::Bound,
};
use std::{borrow::Cow, cell::RefCell};

///
/// RawCommitMarker
///
/// Raw, bounded commit control-plane bytes stored in stable memory.
/// This slot persists both commit-marker bytes and migration-state bytes.
/// This type owns only storage-level framing, not semantic validation logic.
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

    const fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Serialize and bound-check a commit marker payload.
    #[cfg(test)]
    fn try_from_marker(marker: &CommitMarker) -> Result<Self, InternalError> {
        let marker_payload = encode_commit_marker_payload(marker)?;
        let bytes =
            encode_commit_marker_bytes(COMMIT_MARKER_FORMAT_VERSION_CURRENT, &marker_payload)?;
        if bytes.len() > MAX_COMMIT_BYTES as usize {
            return Err(
                InternalError::commit_marker_exceeds_max_size_before_persist(
                    bytes.len(),
                    MAX_COMMIT_BYTES,
                ),
            );
        }
        Ok(Self(bytes))
    }

    /// Deserialize the stored payload, treating failures as corruption.
    fn try_decode(&self) -> Result<Option<CommitMarker>, InternalError> {
        // Phase 1: fast empty-marker check.
        if self.is_empty() {
            return Ok(None);
        }

        // Phase 2: enforce byte-size upper bound before decode.
        if self.0.len() > MAX_COMMIT_BYTES as usize {
            return Err(InternalError::commit_marker_exceeds_max_size(
                self.0.len(),
                MAX_COMMIT_BYTES,
            ));
        }

        // Phase 3: decode + semantic shape validation.
        let marker = decode_commit_marker(&self.0)?;
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
///
/// Stable-cell wrapper for commit marker storage.
/// Invariant: an empty cell means "no in-flight marker persisted".
///

pub(super) struct CommitStore {
    cell: StableCell<RawCommitMarker, VirtualMemory<DefaultMemoryImpl>>,
}

impl CommitStore {
    /// Encode one raw commit-control slot payload for recovery tests.
    #[cfg(test)]
    pub(super) fn encode_raw_control_slot_for_tests(
        marker_bytes: Vec<u8>,
        migration_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_control_slot(&marker_bytes, &migration_bytes)
    }

    /// Encode one raw commit-marker envelope for recovery tests.
    #[cfg(test)]
    pub(super) fn encode_raw_marker_envelope_for_tests(
        format_version: u8,
        marker_payload: Vec<u8>,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_marker_bytes(format_version, &marker_payload)
    }

    /// Encode one single-row commit-control slot payload for regression tests.
    #[cfg(test)]
    pub(super) fn encode_raw_single_row_control_slot_for_tests(
        marker_id: [u8; 16],
        row_op: &CommitRowOp,
        migration_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, InternalError> {
        encode_single_row_commit_control_slot(marker_id, row_op, &migration_bytes)
    }

    /// Encode one multi-row commit-control slot payload for regression tests.
    #[cfg(test)]
    pub(super) fn encode_raw_direct_control_slot_for_tests(
        marker: &CommitMarker,
        migration_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_control_slot_from_marker(marker, &migration_bytes)
    }

    /// Initialize one stable-cell-backed commit marker store.
    fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        let cell = StableCell::init(memory, RawCommitMarker::empty());
        Self { cell }
    }

    /// Load and decode the current commit marker (if any).
    pub(super) fn load(&self) -> Result<Option<CommitMarker>, InternalError> {
        let (marker_bytes, _) = decode_commit_control_slot(self.cell.get().as_bytes())?;

        RawCommitMarker(marker_bytes).try_decode()
    }

    /// Return whether the marker slot is empty without decoding.
    pub(super) fn is_empty(&self) -> bool {
        inspect_commit_control_slot(self.cell.get().as_bytes())
            .is_ok_and(|slot| slot.marker_bytes.is_empty())
    }

    /// Return whether the marker payload is empty while still validating the
    /// outer control-slot envelope.
    pub(super) fn marker_is_empty(&self) -> Result<bool, InternalError> {
        inspect_commit_control_slot(self.cell.get().as_bytes())
            .map(|slot| slot.marker_bytes.is_empty())
    }

    /// Persist one commit marker while proving the current slot has no marker.
    pub(super) fn set_if_empty(&mut self, marker: &CommitMarker) -> Result<(), InternalError> {
        // Phase 1: the common runtime path keeps no migration state, so avoid
        // decoding the canonical control-slot envelope when the raw slot is
        // physically empty.
        if self.cell.get().as_bytes().is_empty() {
            let encoded = encode_commit_control_slot_from_marker(marker, &[])?;

            self.cell.set(RawCommitMarker(encoded));
            return Ok(());
        }

        let migration_bytes = self.require_empty_marker_slot()?;
        let encoded = encode_commit_control_slot_from_marker(marker, migration_bytes)?;

        self.cell.set(RawCommitMarker(encoded));
        Ok(())
    }

    /// Persist one single-row marker while proving the current slot has no marker.
    pub(super) fn set_single_row_op_if_empty(
        &mut self,
        marker_id: [u8; 16],
        row_op: &CommitRowOp,
    ) -> Result<(), InternalError> {
        // Phase 1: most hot write-lane opens happen with a physically empty
        // control slot, so skip control-slot decode when no migration bytes
        // need to be preserved.
        if self.cell.get().as_bytes().is_empty() {
            let encoded = encode_single_row_commit_control_slot(marker_id, row_op, &[])?;

            self.cell.set(RawCommitMarker(encoded));
            return Ok(());
        }

        let migration_bytes = self.require_empty_marker_slot()?;
        let encoded = encode_single_row_commit_control_slot(marker_id, row_op, migration_bytes)?;

        self.cell.set(RawCommitMarker(encoded));
        Ok(())
    }

    /// Persist one commit marker payload and migration-state bytes atomically.
    pub(super) fn set_with_migration_state_if_empty(
        &mut self,
        marker: &CommitMarker,
        migration_state_bytes: Vec<u8>,
    ) -> Result<(), InternalError> {
        self.require_empty_marker_slot()?;

        let encoded = encode_commit_control_slot_from_marker(marker, &migration_state_bytes)?;

        self.cell.set(RawCommitMarker(encoded));
        Ok(())
    }

    /// Load persisted migration-state bytes (if any).
    pub(super) fn load_migration_state_bytes(&self) -> Result<Option<Vec<u8>>, InternalError> {
        let (_, migration_bytes) = decode_commit_control_slot(self.cell.get().as_bytes())?;

        if migration_bytes.is_empty() {
            return Ok(None);
        }

        Ok(Some(migration_bytes))
    }

    /// Clear persisted migration-state bytes while preserving marker bytes.
    pub(super) fn clear_migration_state_bytes(&mut self) -> Result<(), InternalError> {
        let (marker_bytes, _) = decode_commit_control_slot(self.cell.get().as_bytes())?;
        let encoded = encode_commit_control_slot(&marker_bytes, &[])?;

        self.cell.set(RawCommitMarker(encoded));

        Ok(())
    }

    /// Clear marker bytes after a verified commit/recovery success.
    pub(super) fn clear_verified(&mut self) -> Result<(), InternalError> {
        let bytes = self.cell.get().as_bytes();

        // Phase 1: the common runtime case persists no migration state. When
        // the header proves that shape, clear directly to the empty physical slot.
        if current_control_slot_migration_len(bytes) == Some(0) {
            self.cell.set(RawCommitMarker::empty());
            return Ok(());
        }

        // Phase 2: less common migration-bearing slots use the fallible helper
        // so malformed control bytes cannot be silently discarded.
        self.clear_preserve_migration_fallible()
    }

    /// Clear marker bytes while preserving migration-state bytes.
    fn clear_preserve_migration_fallible(&mut self) -> Result<(), InternalError> {
        let bytes = self.cell.get().as_bytes();
        let migration_bytes = inspect_commit_control_slot(bytes)?.migration_bytes;
        if migration_bytes.is_empty() {
            self.cell.set(RawCommitMarker::empty());
            return Ok(());
        }

        let encoded = encode_commit_control_slot(&[], migration_bytes)?;
        self.cell.set(RawCommitMarker(encoded));

        Ok(())
    }

    /// Clear the marker slot directly for tests that intentionally persist corruption.
    #[cfg(test)]
    pub(super) fn clear_raw_for_tests(&mut self) {
        self.cell.set(RawCommitMarker::empty());
    }

    /// Overwrite the raw marker bytes directly for recovery tests.
    #[cfg(test)]
    pub(super) fn set_raw_marker_bytes_for_tests(&mut self, bytes: Vec<u8>) {
        self.cell.set(RawCommitMarker(bytes));
    }

    // Decode the control slot once and require that no marker bytes are present
    // before commit-window open persists a fresh marker.
    fn require_empty_marker_slot(&self) -> Result<&[u8], InternalError> {
        let slot = inspect_commit_control_slot(self.cell.get().as_bytes())?;
        if !slot.marker_bytes.is_empty() {
            return Err(InternalError::store_invariant(
                "commit marker already present before begin",
            ));
        }

        Ok(slot.migration_bytes)
    }
}

thread_local! {
    static COMMIT_STORE: RefCell<Option<CommitStore>> = const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn commit_marker_present() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(store.load()?.is_some()))
}

/// Lazily initialize and access the commit marker store.
pub(super) fn with_commit_store<R>(
    f: impl FnOnce(&mut CommitStore) -> Result<R, InternalError>,
) -> Result<R, InternalError> {
    COMMIT_STORE.with(|cell| {
        // Phase 1: lazily initialize storage if this thread has not touched it.
        if cell.borrow().is_none() {
            // StableCell::init performs a benign stable write for the empty marker.
            let store = CommitStore::init(commit_memory_handle()?);
            *cell.borrow_mut() = Some(store);
        }

        // Phase 2: execute the caller closure against initialized store state.
        let mut guard = cell.borrow_mut();
        let store = guard.as_mut().expect("commit store missing after init");
        f(store)
    })
}

/// Fast, observational check for marker presence without decoding.
pub(super) fn commit_marker_present_fast() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(!store.marker_is_empty()?))
}

/// Access the commit store without fallible initialization.
///
/// Invariant: caller must ensure `with_commit_store(...)` was called first
/// on the current thread.
pub(super) fn with_commit_store_infallible<R>(f: impl FnOnce(&mut CommitStore) -> R) -> R {
    COMMIT_STORE.with(|cell| {
        let mut guard = cell.borrow_mut();
        let store = guard.as_mut().expect("commit store not initialized");
        f(store)
    })
}
