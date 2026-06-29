//! Module: db::commit::store
//! Responsibility: persist, load, and clear commit markers in stable memory.
//! Does not own: marker shape semantics, recovery orchestration, or commit-window policy.
//! Boundary: commit::{guard,recovery} -> commit::store (one-way).

mod bytes;
mod control_slot;
mod marker_envelope;
#[cfg(test)]
mod tests;

use crate::{
    db::commit::{
        marker::{CommitMarker, CommitRowOp, MAX_COMMIT_BYTES, validate_commit_marker_shape},
        memory::{CommitMemoryAllocation, commit_memory_handle, current_commit_memory_allocation},
        store::{
            control_slot::{
                decode_commit_control_slot, encode_commit_control_slot_from_marker,
                encode_single_row_commit_control_slot, inspect_commit_control_slot,
            },
            marker_envelope::decode_commit_marker,
        },
    },
    error::InternalError,
};
use ic_memory::stable_structures::{
    Cell as StableCell, DefaultMemoryImpl, Storable, memory_manager::VirtualMemory, storable::Bound,
};
#[cfg(not(test))]
use std::sync::{Mutex, OnceLock};
use std::{borrow::Cow, cell::RefCell};

#[cfg(test)]
use crate::db::commit::failpoint::{CommitFailpoint, hit_commit_failpoint};
#[cfg(test)]
use crate::db::commit::store::control_slot::encode_commit_control_slot;
#[cfg(test)]
use crate::db::commit::store::marker_envelope::encode_commit_marker_bytes;

#[cfg(not(test))]
static COMMIT_MARKER_PRESENCE_HINTS: OnceLock<Mutex<Vec<CommitMarkerPresenceHint>>> =
    OnceLock::new();

#[cfg(not(test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommitMarkerPresenceHint {
    allocation: CommitMemoryAllocation,
    may_be_present: bool,
}

///
/// RawCommitMarker
///
/// Raw, bounded commit control-plane bytes stored in stable memory.
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

    /// Deserialize the stored payload, treating failures as corruption.
    fn try_decode(&self) -> Result<Option<CommitMarker>, InternalError> {
        // Phase 1: fast empty-marker check.
        if self.is_empty() {
            return Ok(None);
        }

        // Phase 2: enforce byte-size upper bound before decode.
        if self.0.len() > MAX_COMMIT_BYTES as usize {
            return Err(InternalError::commit_marker_exceeds_max_size());
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
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_control_slot(&marker_bytes)
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
    ) -> Result<Vec<u8>, InternalError> {
        encode_single_row_commit_control_slot(marker_id, row_op)
    }

    /// Encode one multi-row commit-control slot payload for regression tests.
    #[cfg(test)]
    pub(super) fn encode_raw_direct_control_slot_for_tests(
        marker: &CommitMarker,
    ) -> Result<Vec<u8>, InternalError> {
        encode_commit_control_slot_from_marker(marker)
    }

    /// Initialize one stable-cell-backed commit marker store.
    fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        let cell = StableCell::init(memory, RawCommitMarker::empty());
        Self { cell }
    }

    /// Load and decode the current commit marker (if any).
    pub(super) fn load(&self) -> Result<Option<CommitMarker>, InternalError> {
        let marker_bytes = decode_commit_control_slot(self.cell.get().as_bytes())?;

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
        // Phase 1: avoid decoding the canonical control-slot envelope when the
        // raw slot is physically empty.
        if self.cell.get().as_bytes().is_empty() {
            let encoded = encode_commit_control_slot_from_marker(marker)?;

            #[cfg(test)]
            hit_commit_failpoint(CommitFailpoint::BeforeMarkerWrite)?;
            self.cell.set(RawCommitMarker(encoded));
            mark_commit_marker_may_be_present();
            #[cfg(test)]
            hit_commit_failpoint(CommitFailpoint::AfterMarkerWrite)?;
            return Ok(());
        }

        self.require_empty_marker_slot()?;
        let encoded = encode_commit_control_slot_from_marker(marker)?;

        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::BeforeMarkerWrite)?;
        self.cell.set(RawCommitMarker(encoded));
        mark_commit_marker_may_be_present();
        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::AfterMarkerWrite)?;
        Ok(())
    }

    /// Persist one single-row marker while proving the current slot has no marker.
    pub(super) fn set_single_row_op_if_empty(
        &mut self,
        marker_id: [u8; 16],
        row_op: &CommitRowOp,
    ) -> Result<(), InternalError> {
        // Phase 1: most hot write-lane opens happen with a physically empty
        // control slot, so skip control-slot decode in that common case.
        if self.cell.get().as_bytes().is_empty() {
            let encoded = encode_single_row_commit_control_slot(marker_id, row_op)?;

            #[cfg(test)]
            hit_commit_failpoint(CommitFailpoint::BeforeMarkerWrite)?;
            self.cell.set(RawCommitMarker(encoded));
            mark_commit_marker_may_be_present();
            #[cfg(test)]
            hit_commit_failpoint(CommitFailpoint::AfterMarkerWrite)?;
            return Ok(());
        }

        self.require_empty_marker_slot()?;
        let encoded = encode_single_row_commit_control_slot(marker_id, row_op)?;

        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::BeforeMarkerWrite)?;
        self.cell.set(RawCommitMarker(encoded));
        mark_commit_marker_may_be_present();
        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::AfterMarkerWrite)?;
        Ok(())
    }

    /// Clear marker bytes after a verified commit/recovery success.
    pub(super) fn clear_verified(&mut self) -> Result<(), InternalError> {
        // Phase 1: validate the control-slot envelope before clearing so
        // malformed persisted bytes cannot be silently discarded.
        inspect_commit_control_slot(self.cell.get().as_bytes())?;
        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::BeforeMarkerClear)?;
        self.cell.set(RawCommitMarker::empty());
        mark_commit_marker_verified_absent();
        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::AfterMarkerClear)?;

        Ok(())
    }

    /// Clear the marker slot directly for tests that intentionally persist corruption.
    #[cfg(test)]
    pub(super) fn clear_raw_for_tests(&mut self) {
        self.cell.set(RawCommitMarker::empty());
        mark_commit_marker_verified_absent();
    }

    /// Overwrite the raw marker bytes directly for recovery tests.
    #[cfg(test)]
    pub(super) fn set_raw_marker_bytes_for_tests(&mut self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            mark_commit_marker_verified_absent();
        } else {
            mark_commit_marker_may_be_present();
        }

        self.cell.set(RawCommitMarker(bytes));
    }

    // Decode the control slot once and require that no marker bytes are present
    // before commit-window open persists a fresh marker.
    fn require_empty_marker_slot(&self) -> Result<(), InternalError> {
        let slot = inspect_commit_control_slot(self.cell.get().as_bytes())?;
        if !slot.marker_bytes.is_empty() {
            return Err(InternalError::store_invariant());
        }

        Ok(())
    }
}

struct CommitStoreEntry {
    allocation: CommitMemoryAllocation,
    store: CommitStore,
}

thread_local! {
    static COMMIT_STORES: RefCell<Vec<CommitStoreEntry>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(super) fn commit_marker_present() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(store.load()?.is_some()))
}

/// Lazily initialize and access the commit marker store.
pub(super) fn with_commit_store<R>(
    f: impl FnOnce(&mut CommitStore) -> Result<R, InternalError>,
) -> Result<R, InternalError> {
    let allocation = current_commit_memory_allocation()?;

    COMMIT_STORES.with(|cell| {
        let mut stores = cell.borrow_mut();
        if let Some(index) = stores
            .iter()
            .position(|entry| entry.allocation == allocation)
        {
            return f(&mut stores[index].store);
        }

        // StableCell::init performs a benign stable write for the empty marker.
        #[cfg(test)]
        let store = CommitStore::init(commit_memory_handle(allocation));
        #[cfg(not(test))]
        let store = CommitStore::init(commit_memory_handle(allocation)?);
        stores.push(CommitStoreEntry { allocation, store });
        let index = stores.len().saturating_sub(1);
        f(&mut stores[index].store)
    })
}

/// Fast, observational check for marker presence without decoding.
pub(super) fn commit_marker_present_fast() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(!store.marker_is_empty()?))
}

/// Return whether a process-local commit-window event requires a stable marker check.
#[cfg(not(test))]
pub(super) fn commit_marker_may_be_present() -> bool {
    let Ok(allocation) = current_commit_memory_allocation() else {
        return true;
    };
    let Ok(hints) = commit_marker_presence_hints().lock() else {
        return true;
    };

    hints
        .iter()
        .find(|hint| hint.allocation == allocation)
        .is_none_or(|hint| hint.may_be_present)
}

/// Return whether a process-local commit-window event requires a stable marker check.
#[cfg(test)]
pub(super) const fn commit_marker_may_be_present() -> bool {
    // Core unit tests intentionally exercise many synthetic commit/recovery
    // states in parallel against the same process-local marker machinery.
    // Keeping tests stable-marker authoritative avoids cross-test races in the
    // process-local optimization hint while production builds retain the fast path.
    true
}

/// Mark the process-local marker hint clean after a verified empty-marker observation.
#[cfg(not(test))]
pub(super) fn mark_commit_marker_verified_absent() {
    set_commit_marker_presence_hint(false);
}

/// Mark the process-local marker hint clean after a verified empty-marker observation.
#[cfg(test)]
pub(super) const fn mark_commit_marker_verified_absent() {}

// Mark the process-local marker hint dirty after this process persists marker bytes.
#[cfg(not(test))]
fn mark_commit_marker_may_be_present() {
    set_commit_marker_presence_hint(true);
}

// Mark the process-local marker hint dirty after this process persists marker bytes.
#[cfg(test)]
const fn mark_commit_marker_may_be_present() {}

#[cfg(not(test))]
fn commit_marker_presence_hints() -> &'static Mutex<Vec<CommitMarkerPresenceHint>> {
    COMMIT_MARKER_PRESENCE_HINTS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(not(test))]
fn set_commit_marker_presence_hint(may_be_present: bool) {
    let Ok(allocation) = current_commit_memory_allocation() else {
        return;
    };
    let Ok(mut hints) = commit_marker_presence_hints().lock() else {
        return;
    };

    if let Some(hint) = hints.iter_mut().find(|hint| hint.allocation == allocation) {
        hint.may_be_present = may_be_present;
        return;
    }

    hints.push(CommitMarkerPresenceHint {
        allocation,
        may_be_present,
    });
}

/// Access the commit store without fallible initialization.
///
/// Invariant: caller must ensure `with_commit_store(...)` was called first
/// on the current thread.
pub(super) fn with_commit_store_infallible<R>(f: impl FnOnce(&mut CommitStore) -> R) -> R {
    let allocation =
        current_commit_memory_allocation().expect("commit memory allocation not configured");

    COMMIT_STORES.with(|cell| {
        let mut stores = cell.borrow_mut();
        let store = stores
            .iter_mut()
            .find(|entry| entry.allocation == allocation)
            .map(|entry| &mut entry.store)
            .expect("commit store not initialized");
        f(store)
    })
}
