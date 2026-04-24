//! Module: commit::store
//! Responsibility: persist, load, and clear commit markers in stable memory.
//! Does not own: marker shape semantics, recovery orchestration, or commit-window policy.
//! Boundary: commit::{guard,recovery} -> commit::store (one-way).

#[cfg(test)]
mod tests;

#[cfg(test)]
use crate::db::commit::marker::encode_commit_marker_payload;
use crate::{
    db::commit::{
        marker::{
            COMMIT_MARKER_FORMAT_VERSION_CURRENT, CommitMarker, CommitRowOp, MAX_COMMIT_BYTES,
            commit_marker_payload_capacity, decode_commit_marker_payload,
            single_row_commit_marker_payload_capacity, validate_commit_marker_shape,
            validate_commit_row_op_shape, write_commit_marker_payload,
            write_single_row_commit_marker_payload,
        },
        memory::commit_memory_handle,
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

///
/// CommitControlSlotRef
///
/// Borrowed view of one decoded commit control-slot envelope.
/// This keeps hot-path marker checks allocation-free while preserving the
/// same strict control-slot validation contract as the owned decode helper.
///

struct CommitControlSlotRef<'a> {
    marker_bytes: &'a [u8],
    migration_bytes: &'a [u8],
}

const COMMIT_CONTROL_MAGIC: [u8; 4] = *b"CMCS";
const COMMIT_CONTROL_STATE_VERSION_CURRENT: u8 = 1;
const COMMIT_CONTROL_HEADER_BYTES: usize = 13;
const COMMIT_MARKER_HEADER_BYTES: usize = 5;

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

    // Build the canonical max-size corruption error for raw commit control bytes.
    fn exceeds_max_size(size: usize) -> InternalError {
        InternalError::commit_marker_exceeds_max_size(size, MAX_COMMIT_BYTES)
    }

    // Build the canonical control-slot canonical-envelope corruption error.
    fn control_slot_canonical_envelope_required() -> InternalError {
        InternalError::commit_corruption("commit control-slot decode: expected envelope")
    }

    // Build the canonical marker-envelope canonical-envelope corruption error.
    fn marker_canonical_envelope_required() -> InternalError {
        InternalError::commit_corruption("commit marker decode: expected envelope")
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
            return Err(Self::exceeds_max_size(self.0.len()));
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
// - only the canonical control-slot envelope is accepted
fn decode_commit_control_slot(bytes: &[u8]) -> Result<(Vec<u8>, Vec<u8>), InternalError> {
    let slot = inspect_commit_control_slot(bytes)?;

    Ok((slot.marker_bytes.to_vec(), slot.migration_bytes.to_vec()))
}

// Read the migration-length field from one current-format control-slot header.
//
// This is an internal hot-path helper for success-path marker clearing. When
// the runtime has just authored the slot itself, a zero migration length lets
// clear drop straight to the physically empty slot without decoding the full
// envelope again.
fn current_control_slot_migration_len(bytes: &[u8]) -> Option<u32> {
    if bytes.len() < COMMIT_CONTROL_HEADER_BYTES {
        return None;
    }
    if bytes.get(..COMMIT_CONTROL_MAGIC.len())? != COMMIT_CONTROL_MAGIC {
        return None;
    }
    if *bytes.get(COMMIT_CONTROL_MAGIC.len())? != COMMIT_CONTROL_STATE_VERSION_CURRENT {
        return None;
    }

    let migration_len_start = COMMIT_CONTROL_MAGIC.len() + 1 + 4;
    let migration_len_end = migration_len_start + 4;
    let raw_len: [u8; 4] = bytes
        .get(migration_len_start..migration_len_end)?
        .try_into()
        .ok()?;

    Some(u32::from_le_bytes(raw_len))
}

// Inspect commit control-slot bytes under the canonical envelope without
// allocating owned marker or migration buffers.
fn inspect_commit_control_slot(bytes: &[u8]) -> Result<CommitControlSlotRef<'_>, InternalError> {
    if bytes.is_empty() {
        return Ok(CommitControlSlotRef {
            marker_bytes: &[],
            migration_bytes: &[],
        });
    }

    if bytes.len() > MAX_COMMIT_BYTES as usize {
        return Err(RawCommitMarker::exceeds_max_size(bytes.len()));
    }
    if bytes.len() < COMMIT_CONTROL_HEADER_BYTES {
        return Err(RawCommitMarker::control_slot_canonical_envelope_required());
    }

    let magic: [u8; 4] = bytes
        .get(..COMMIT_CONTROL_MAGIC.len())
        .ok_or_else(RawCommitMarker::control_slot_canonical_envelope_required)?
        .try_into()
        .map_err(|_| RawCommitMarker::control_slot_canonical_envelope_required())?;
    if magic != COMMIT_CONTROL_MAGIC {
        return Err(InternalError::serialize_incompatible_persisted_format(
            "commit control-slot magic mismatch".to_string(),
        ));
    }

    let control_version = *bytes
        .get(COMMIT_CONTROL_MAGIC.len())
        .ok_or_else(RawCommitMarker::control_slot_canonical_envelope_required)?;
    if control_version != COMMIT_CONTROL_STATE_VERSION_CURRENT {
        return Err(InternalError::serialize_incompatible_persisted_format(
            format!(
                "commit control-slot version {control_version} is incompatible with runtime version {COMMIT_CONTROL_STATE_VERSION_CURRENT}",
            ),
        ));
    }

    let mut cursor = COMMIT_CONTROL_MAGIC.len() + 1;
    let marker_len = read_u32_le(bytes, &mut cursor, "commit control-slot")? as usize;
    let migration_len = read_u32_le(bytes, &mut cursor, "commit control-slot")? as usize;
    let remaining = bytes.len().saturating_sub(cursor);
    let expected = marker_len.saturating_add(migration_len);
    if remaining != expected {
        return Err(RawCommitMarker::control_slot_canonical_envelope_required());
    }

    let marker_end = cursor.saturating_add(marker_len);
    let marker_bytes = bytes
        .get(cursor..marker_end)
        .ok_or_else(RawCommitMarker::control_slot_canonical_envelope_required)?;
    cursor = marker_end;
    let migration_end = cursor.saturating_add(migration_len);
    let migration_bytes = bytes
        .get(cursor..migration_end)
        .ok_or_else(RawCommitMarker::control_slot_canonical_envelope_required)?;

    Ok(CommitControlSlotRef {
        marker_bytes,
        migration_bytes,
    })
}

// Encode marker + migration payload bytes into the persisted control-slot format.
fn encode_commit_control_slot(
    marker_bytes: &[u8],
    migration_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let encoded = encode_commit_control_slot_bytes(marker_bytes, migration_bytes)?;

    if encoded.len() > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size(
            encoded.len(),
            MAX_COMMIT_BYTES,
        ));
    }

    Ok(encoded)
}

// Serialize one single-row marker payload under the canonical versioned
// envelope so hot save/delete opens do not build a Vec-shaped marker wrapper.
// Encode the full control slot for a multi-row marker directly so atomic batch
// opens do not allocate intermediate marker payload and marker-envelope buffers.
fn encode_commit_control_slot_from_marker(
    marker: &CommitMarker,
    migration_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    validate_commit_marker_shape(marker)?;

    let marker_payload_len = commit_marker_payload_capacity(marker);
    let marker_bytes_len = COMMIT_MARKER_HEADER_BYTES.saturating_add(marker_payload_len);
    let marker_len = u32::try_from(marker_bytes_len).map_err(|_| {
        InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit(marker_bytes_len)
    })?;
    let migration_len = u32::try_from(migration_bytes.len()).map_err(|_| {
        InternalError::commit_control_slot_migration_bytes_exceed_u32_length_limit(
            migration_bytes.len(),
        )
    })?;
    let total_len = COMMIT_CONTROL_HEADER_BYTES
        .saturating_add(marker_bytes_len)
        .saturating_add(migration_bytes.len());
    if total_len > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size(
            total_len,
            MAX_COMMIT_BYTES,
        ));
    }

    let mut encoded = Vec::with_capacity(total_len);
    write_commit_control_slot_header(&mut encoded, marker_len, migration_len);
    write_commit_marker_envelope_header(&mut encoded, marker_payload_len)?;
    write_commit_marker_payload(&mut encoded, marker)?;
    encoded.extend_from_slice(migration_bytes);

    Ok(encoded)
}

// Encode the full control slot for a single-row marker directly so hot
// save/delete opens do not allocate intermediate marker payload vectors.
fn encode_single_row_commit_control_slot(
    marker_id: [u8; 16],
    row_op: &CommitRowOp,
    migration_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    validate_commit_row_op_shape(row_op)?;

    let marker_payload_len = single_row_commit_marker_payload_capacity(row_op);
    let marker_bytes_len = COMMIT_MARKER_HEADER_BYTES.saturating_add(marker_payload_len);
    let marker_len = u32::try_from(marker_bytes_len).map_err(|_| {
        InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit(marker_bytes_len)
    })?;
    let migration_len = u32::try_from(migration_bytes.len()).map_err(|_| {
        InternalError::commit_control_slot_migration_bytes_exceed_u32_length_limit(
            migration_bytes.len(),
        )
    })?;
    let total_len = COMMIT_CONTROL_HEADER_BYTES
        .saturating_add(marker_bytes_len)
        .saturating_add(migration_bytes.len());
    if total_len > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size(
            total_len,
            MAX_COMMIT_BYTES,
        ));
    }

    let mut encoded = Vec::with_capacity(total_len);
    write_commit_control_slot_header(&mut encoded, marker_len, migration_len);
    write_commit_marker_envelope_header(&mut encoded, marker_payload_len)?;
    write_single_row_commit_marker_payload(&mut encoded, marker_id, row_op)?;
    encoded.extend_from_slice(migration_bytes);

    Ok(encoded)
}

// Decode one commit marker with strict envelope semantics.
fn decode_commit_marker(bytes: &[u8]) -> Result<CommitMarker, InternalError> {
    if bytes.len() > MAX_COMMIT_BYTES as usize {
        return Err(RawCommitMarker::exceeds_max_size(bytes.len()));
    }

    let (format_version, marker_payload) = decode_commit_marker_bytes(bytes)?;
    validate_commit_marker_format_version(format_version)?;

    decode_commit_marker_payload(&marker_payload)
}

// Validate marker envelope version against the single supported format.
fn validate_commit_marker_format_version(format_version: u8) -> Result<(), InternalError> {
    if format_version == COMMIT_MARKER_FORMAT_VERSION_CURRENT {
        return Ok(());
    }

    Err(InternalError::serialize_incompatible_persisted_format(
        format!(
            "commit marker format version {format_version} is unsupported by runtime version {COMMIT_MARKER_FORMAT_VERSION_CURRENT}",
        ),
    ))
}

// Encode the stable control-slot frame directly so recovery only reads one
// bounded binary envelope before marker decode.
fn encode_commit_control_slot_bytes(
    marker_bytes: &[u8],
    migration_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::with_capacity(
        COMMIT_CONTROL_HEADER_BYTES
            .saturating_add(marker_bytes.len())
            .saturating_add(migration_bytes.len()),
    );
    let marker_len = u32::try_from(marker_bytes.len()).map_err(|_| {
        InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit(marker_bytes.len())
    })?;
    let migration_len = u32::try_from(migration_bytes.len()).map_err(|_| {
        InternalError::commit_control_slot_migration_bytes_exceed_u32_length_limit(
            migration_bytes.len(),
        )
    })?;
    write_commit_control_slot_header(&mut encoded, marker_len, migration_len);
    encoded.extend_from_slice(marker_bytes);
    encoded.extend_from_slice(migration_bytes);

    Ok(encoded)
}

// Write the shared commit control-slot header used by all marker write paths.
fn write_commit_control_slot_header(out: &mut Vec<u8>, marker_len: u32, migration_len: u32) {
    out.extend_from_slice(&COMMIT_CONTROL_MAGIC);
    out.push(COMMIT_CONTROL_STATE_VERSION_CURRENT);
    out.extend_from_slice(&marker_len.to_le_bytes());
    out.extend_from_slice(&migration_len.to_le_bytes());
}

// Write the shared versioned marker-envelope header.
fn write_commit_marker_envelope_header(
    out: &mut Vec<u8>,
    marker_payload_len: usize,
) -> Result<(), InternalError> {
    out.push(COMMIT_MARKER_FORMAT_VERSION_CURRENT);
    out.extend_from_slice(
        &(u32::try_from(marker_payload_len).map_err(|_| {
            InternalError::commit_marker_payload_exceeds_u32_length_limit(
                "commit marker payload",
                marker_payload_len,
            )
        })?)
        .to_le_bytes(),
    );

    Ok(())
}

// Encode the versioned marker envelope directly so only the marker payload
// itself still uses persisted-payload decode.
#[cfg(test)]
fn encode_commit_marker_bytes(
    format_version: u8,
    marker_payload: &[u8],
) -> Result<Vec<u8>, InternalError> {
    if marker_payload.len() > u32::MAX as usize {
        return Err(
            InternalError::commit_marker_payload_exceeds_u32_length_limit(
                "commit marker payload",
                marker_payload.len(),
            ),
        );
    }

    let payload_len = u32::try_from(marker_payload.len()).map_err(|_| {
        InternalError::commit_marker_payload_exceeds_u32_length_limit(
            "commit marker payload",
            marker_payload.len(),
        )
    })?;
    let mut encoded =
        Vec::with_capacity(COMMIT_MARKER_HEADER_BYTES.saturating_add(marker_payload.len()));
    // Tests intentionally vary the format version, so this helper cannot reuse
    // `write_commit_marker_envelope_header`, which always emits the live version.
    encoded.push(format_version);
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(marker_payload);

    Ok(encoded)
}

// Decode the marker envelope without routing through generic tuple deserialization.
fn decode_commit_marker_bytes(bytes: &[u8]) -> Result<(u8, Vec<u8>), InternalError> {
    if bytes.len() < COMMIT_MARKER_HEADER_BYTES {
        return Err(RawCommitMarker::marker_canonical_envelope_required());
    }

    let format_version = bytes[0];
    let mut cursor = 1;
    let payload_len = read_u32_le(bytes, &mut cursor, "commit marker")? as usize;
    let payload = bytes
        .get(cursor..)
        .ok_or_else(RawCommitMarker::marker_canonical_envelope_required)?;
    if payload.len() != payload_len {
        return Err(RawCommitMarker::marker_canonical_envelope_required());
    }

    Ok((format_version, payload.to_vec()))
}

// Read one little-endian u32 length from a bounded binary envelope.
fn read_u32_le(
    bytes: &[u8],
    cursor: &mut usize,
    label: &'static str,
) -> Result<u32, InternalError> {
    let next = cursor.saturating_add(4);
    let payload = bytes.get(*cursor..next).ok_or_else(|| {
        InternalError::commit_corruption(format!(
            "{label} decode failed: expected canonical envelope"
        ))
    })?;
    *cursor = next;

    Ok(u32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
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
    pub(super) fn set_with_migration_state(
        &mut self,
        marker: &CommitMarker,
        migration_state_bytes: Vec<u8>,
    ) -> Result<(), InternalError> {
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
