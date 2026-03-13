//! Module: commit::store
//! Responsibility: persist, load, and clear commit markers in stable memory.
//! Does not own: marker shape semantics, recovery orchestration, or commit-window policy.
//! Boundary: commit::{guard,recovery} -> commit::store (one-way).

use crate::{
    db::{
        codec::deserialize_persisted_payload,
        commit::{
            COMMIT_MARKER_FORMAT_VERSION_CURRENT, COMMIT_MARKER_FORMAT_VERSION_PREVIOUS,
            CommitMarker, MAX_COMMIT_BYTES, commit_corruption, memory::commit_memory_id,
            validate_commit_marker_shape,
        },
    },
    error::InternalError,
    serialize::{SerializeError, deserialize_bounded, serialize},
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
///
/// Raw, bounded commit control-plane bytes stored in stable memory.
/// This slot persists both commit-marker bytes and migration-state bytes.
/// This type owns only storage-level framing, not semantic validation logic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawCommitMarker(Vec<u8>);

// Persisted marker envelope payload: (format_version, encoded_marker_payload_bytes).
type PersistedCommitMarkerEnvelope = (u8, Vec<u8>);
// Persisted commit control-slot payload:
// (magic, control_state_version, commit_marker_bytes, migration_state_bytes).
type PersistedCommitControlEnvelope = ([u8; 4], u8, Vec<u8>, Vec<u8>);
const COMMIT_CONTROL_MAGIC: [u8; 4] = *b"CMCS";
const COMMIT_CONTROL_STATE_VERSION_CURRENT: u8 = 1;

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
    fn try_from_marker(marker: &CommitMarker) -> Result<Self, InternalError> {
        let bytes = serialize_commit_marker(marker)?;
        if bytes.len() > MAX_COMMIT_BYTES as usize {
            return Err(InternalError::store_unsupported(format!(
                "commit marker exceeds max size: {} bytes (limit {MAX_COMMIT_BYTES})",
                bytes.len()
            )));
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
            return Err(commit_corruption(format!(
                "commit marker exceeds max size: {} bytes (limit {MAX_COMMIT_BYTES})",
                self.0.len()
            )));
        }

        // Phase 3: decode + semantic shape validation.
        let marker = decode_commit_marker(&self.0)?;
        validate_commit_marker_shape(&marker)?;

        Ok(Some(marker))
    }
}

// Decode commit control-slot bytes into marker + migration payload bytes.
//
// Compatibility contract:
// - current control-slot version is accepted
// - legacy pre-control-slot payloads are treated as raw commit-marker bytes
//   with empty migration-state bytes
fn decode_commit_control_slot(bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>), InternalError> {
    if bytes.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let envelope = match deserialize_bounded::<PersistedCommitControlEnvelope>(
        bytes,
        MAX_COMMIT_BYTES as usize,
    ) {
        Ok(envelope) => Some(envelope),
        Err(SerializeError::DeserializeSizeLimitExceeded { len, max_bytes }) => {
            return Err(commit_corruption(format!(
                "commit marker exceeds max size: {len} bytes (limit {max_bytes})",
            )));
        }
        Err(SerializeError::Deserialize(_) | SerializeError::Serialize(_)) => None,
    };

    if let Some((magic, control_version, marker_bytes, migration_bytes)) = envelope {
        if magic != COMMIT_CONTROL_MAGIC {
            return Err(InternalError::serialize_incompatible_persisted_format(
                "commit control-slot magic mismatch".to_string(),
            ));
        }
        if control_version != COMMIT_CONTROL_STATE_VERSION_CURRENT {
            return Err(InternalError::serialize_incompatible_persisted_format(
                format!(
                    "commit control-slot version {control_version} is incompatible with runtime version {COMMIT_CONTROL_STATE_VERSION_CURRENT}",
                ),
            ));
        }

        return Ok((marker_bytes, migration_bytes));
    }

    // Legacy fallback: slot stores only commit-marker bytes.
    Ok((bytes.to_vec(), Vec::new()))
}

// Encode marker + migration payload bytes into the persisted control-slot format.
fn encode_commit_control_slot(
    marker_bytes: Vec<u8>,
    migration_bytes: Vec<u8>,
) -> Result<Vec<u8>, InternalError> {
    let encoded = serialize(&(
        COMMIT_CONTROL_MAGIC,
        COMMIT_CONTROL_STATE_VERSION_CURRENT,
        marker_bytes,
        migration_bytes,
    ))
    .map_err(|err| {
        InternalError::serialize_internal(format!(
            "failed to serialize commit control-slot envelope: {err}"
        ))
    })?;

    if encoded.len() > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::store_unsupported(format!(
            "commit control slot exceeds max size: {} bytes (limit {MAX_COMMIT_BYTES})",
            encoded.len()
        )));
    }

    Ok(encoded)
}

// Serialize a commit marker payload under the canonical versioned envelope.
fn serialize_commit_marker(marker: &CommitMarker) -> Result<Vec<u8>, InternalError> {
    let marker_payload = serialize(marker).map_err(|err| {
        InternalError::serialize_internal(format!("failed to serialize commit marker: {err}"))
    })?;

    serialize(&(COMMIT_MARKER_FORMAT_VERSION_CURRENT, marker_payload)).map_err(|err| {
        InternalError::serialize_internal(format!(
            "failed to serialize versioned commit marker envelope: {err}"
        ))
    })
}

// Decode one commit marker with version-aware envelope semantics.
//
// Compatibility contract:
// - current envelope version (V) is accepted
// - previous envelope version (V-1) is accepted
// - legacy unversioned markers are treated as V-1 for migration safety
// - future or too-old envelope versions fail closed as incompatible format
fn decode_commit_marker(bytes: &[u8]) -> Result<CommitMarker, InternalError> {
    // Phase 1: try decoding the explicit versioned envelope first.
    let envelope = match deserialize_bounded::<PersistedCommitMarkerEnvelope>(
        bytes,
        MAX_COMMIT_BYTES as usize,
    ) {
        Ok(envelope) => Some(envelope),
        Err(SerializeError::DeserializeSizeLimitExceeded { len, max_bytes }) => {
            return Err(commit_corruption(format!(
                "commit marker exceeds max size: {len} bytes (limit {max_bytes})",
            )));
        }
        Err(SerializeError::Deserialize(_) | SerializeError::Serialize(_)) => None,
    };

    if let Some((format_version, marker_payload)) = envelope {
        validate_commit_marker_format_version(format_version)?;
        return deserialize_persisted_payload::<CommitMarker>(
            &marker_payload,
            MAX_COMMIT_BYTES as usize,
            "commit marker payload",
        )
        .map_err(commit_corruption);
    }

    // Phase 2: fallback to legacy pre-envelope marker payloads (treated as V-1).
    deserialize_persisted_payload::<CommitMarker>(bytes, MAX_COMMIT_BYTES as usize, "commit marker")
        .map_err(commit_corruption)
}

// Validate marker envelope version against the N-1 compatibility window.
fn validate_commit_marker_format_version(format_version: u8) -> Result<(), InternalError> {
    if format_version == COMMIT_MARKER_FORMAT_VERSION_CURRENT
        || format_version == COMMIT_MARKER_FORMAT_VERSION_PREVIOUS
    {
        return Ok(());
    }

    if format_version > COMMIT_MARKER_FORMAT_VERSION_CURRENT {
        return Err(InternalError::serialize_incompatible_persisted_format(
            format!(
                "commit marker format version {format_version} is newer than runtime version {COMMIT_MARKER_FORMAT_VERSION_CURRENT}",
            ),
        ));
    }

    Err(InternalError::serialize_incompatible_persisted_format(
        format!(
            "commit marker format version {format_version} is outside compatibility window [{COMMIT_MARKER_FORMAT_VERSION_PREVIOUS}, {COMMIT_MARKER_FORMAT_VERSION_CURRENT}]",
        ),
    ))
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
        decode_commit_control_slot(self.cell.get().as_bytes())
            .map(|(marker_bytes, _)| marker_bytes.is_empty())
            .unwrap_or(false)
    }

    /// Persist one commit marker payload.
    pub(super) fn set(&mut self, marker: &CommitMarker) -> Result<(), InternalError> {
        let (_, migration_bytes) = decode_commit_control_slot(self.cell.get().as_bytes())?;
        let marker_bytes = RawCommitMarker::try_from_marker(marker)?.into_bytes();
        let encoded = encode_commit_control_slot(marker_bytes, migration_bytes)?;

        self.cell.set(RawCommitMarker(encoded));
        Ok(())
    }

    /// Persist one commit marker payload and migration-state bytes atomically.
    pub(super) fn set_with_migration_state(
        &mut self,
        marker: &CommitMarker,
        migration_state_bytes: Vec<u8>,
    ) -> Result<(), InternalError> {
        let marker_bytes = RawCommitMarker::try_from_marker(marker)?.into_bytes();
        let encoded = encode_commit_control_slot(marker_bytes, migration_state_bytes)?;

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
        let encoded = encode_commit_control_slot(marker_bytes, Vec::new())?;

        self.cell.set(RawCommitMarker(encoded));

        Ok(())
    }

    /// Clear the marker slot.
    ///
    /// This write is infallible by storage contract and is only used after
    /// successful commit-window completion or successful recovery completion.
    pub(super) fn clear_infallible(&mut self) {
        let migration_bytes = decode_commit_control_slot(self.cell.get().as_bytes())
            .map(|(_, migration_bytes)| migration_bytes)
            .unwrap_or_default();

        let encoded = encode_commit_control_slot(Vec::new(), migration_bytes)
            .unwrap_or_else(|_| RawCommitMarker::empty().into_bytes());
        self.cell.set(RawCommitMarker(encoded));
    }
}

thread_local! {
    static COMMIT_STORE: RefCell<Option<CommitStore>> = const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn commit_marker_present() -> Result<bool, InternalError> {
    with_commit_store(|store| Ok(store.load()?.is_some()))
}

#[cfg(test)]
pub(super) fn set_raw_commit_marker_bytes_for_tests(bytes: Vec<u8>) -> Result<(), InternalError> {
    with_commit_store(|store| {
        store.cell.set(RawCommitMarker(bytes));
        Ok(())
    })
}

/// Lazily initialize and access the commit marker store.
pub(super) fn with_commit_store<R>(
    f: impl FnOnce(&mut CommitStore) -> Result<R, InternalError>,
) -> Result<R, InternalError> {
    COMMIT_STORE.with(|cell| {
        // Phase 1: lazily initialize storage if this thread has not touched it.
        if cell.borrow().is_none() {
            // StableCell::init performs a benign stable write for the empty marker.
            let store = CommitStore::init(commit_memory()?);
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
    with_commit_store(|store| Ok(!store.is_empty()))
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

/// Resolve the virtual memory backing the commit marker store.
fn commit_memory() -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    let id = commit_memory_id()?;
    Ok(MEMORY_MANAGER.with_borrow_mut(|mgr| mgr.get(MemoryId::new(id))))
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{RawCommitMarker, serialize_commit_marker};
    use crate::{
        db::{
            codec::MAX_ROW_BYTES,
            commit::{
                COMMIT_MARKER_FORMAT_VERSION_CURRENT, COMMIT_MARKER_FORMAT_VERSION_PREVIOUS,
                CommitMarker, CommitRowOp, MAX_COMMIT_BYTES,
            },
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
    fn commit_marker_current_version_round_trip_succeeds() {
        let marker = CommitMarker {
            id: [9u8; 16],
            row_ops: Vec::new(),
        };
        let encoded = serialize_commit_marker(&marker)
            .expect("current-version marker envelope encode should succeed");
        let decoded = RawCommitMarker(encoded)
            .try_decode()
            .expect("current-version marker envelope should decode")
            .expect("marker payload should be present");

        assert_eq!(decoded.id, marker.id);
        assert_eq!(decoded.row_ops.len(), 0);
    }

    #[test]
    fn commit_marker_previous_version_round_trip_succeeds() {
        let marker = CommitMarker {
            id: [8u8; 16],
            row_ops: Vec::new(),
        };
        let marker_payload =
            serialize(&marker).expect("marker payload encode for previous version should succeed");
        let encoded = serialize(&(COMMIT_MARKER_FORMAT_VERSION_PREVIOUS, marker_payload))
            .expect("previous-version marker envelope encode should succeed");
        let decoded = RawCommitMarker(encoded)
            .try_decode()
            .expect("previous-version marker envelope should decode")
            .expect("marker payload should be present");

        assert_eq!(decoded.id, marker.id);
        assert_eq!(decoded.row_ops.len(), 0);
    }

    #[test]
    fn commit_marker_legacy_payload_round_trip_succeeds() {
        let marker = CommitMarker {
            id: [7u8; 16],
            row_ops: Vec::new(),
        };
        let legacy_payload = serialize(&marker).expect("legacy marker payload encode should work");
        let decoded = RawCommitMarker(legacy_payload)
            .try_decode()
            .expect("legacy marker payload should decode via compatibility fallback")
            .expect("marker payload should be present");

        assert_eq!(decoded.id, marker.id);
        assert_eq!(decoded.row_ops.len(), 0);
    }

    #[test]
    fn commit_marker_future_version_fails_closed() {
        let marker = CommitMarker {
            id: [6u8; 16],
            row_ops: Vec::new(),
        };
        let marker_payload =
            serialize(&marker).expect("marker payload encode for future-version test should work");
        let future_version = COMMIT_MARKER_FORMAT_VERSION_CURRENT.saturating_add(1);
        let encoded = serialize(&(future_version, marker_payload))
            .expect("future-version marker envelope encode should succeed");
        let err = RawCommitMarker(encoded)
            .try_decode()
            .expect_err("future marker versions must fail closed");

        assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
        assert_eq!(err.origin, ErrorOrigin::Serialize);
    }

    #[test]
    fn commit_marker_older_than_window_fails_closed() {
        let marker = CommitMarker {
            id: [5u8; 16],
            row_ops: Vec::new(),
        };
        let marker_payload =
            serialize(&marker).expect("marker payload encode for old-version test should work");
        let older_than_window = COMMIT_MARKER_FORMAT_VERSION_PREVIOUS.saturating_sub(1);
        let encoded = serialize(&(older_than_window, marker_payload))
            .expect("older-version marker envelope encode should succeed");
        let err = RawCommitMarker(encoded)
            .try_decode()
            .expect_err("older-than-window marker versions must fail closed");

        assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
        assert_eq!(err.origin, ErrorOrigin::Serialize);
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
                [0u8; 16],
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
            row_ops: vec![CommitRowOp::new(
                "test::Entity",
                vec![9u8],
                None,
                None,
                [0u8; 16],
            )],
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
            row_ops: vec![CommitRowOp::new(
                "",
                vec![9u8],
                Some(vec![1u8]),
                None,
                [0u8; 16],
            )],
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
                [0u8; 16],
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
                [0u8; 16],
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
                [0u8; 16],
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
