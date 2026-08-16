//! Module: db::commit::store::control_slot
//! Responsibility: encode/decode the durable database convergence and marker control envelope.
//! Does not own: stable-cell lifecycle, marker semantics, or recovery orchestration.
//! Boundary: convergence/commit lifecycle -> one framed control-slot authority.

use crate::{
    db::{
        commit::{
            marker::{
                CommitMarker, MAX_COMMIT_BYTES, commit_marker_payload_capacity,
                validate_commit_marker_shape, write_commit_marker_payload,
            },
            store::{bytes::read_u32_le, marker_envelope::write_commit_marker_envelope_header},
        },
        integrity::DatabaseIncarnationId,
        registry::{StoreAllocationIdentities, StoreAllocationIdentity},
    },
    error::InternalError,
};
use std::ops::Range;

/// Maximum active-plus-retired store allocation quartets in one incarnation.
pub(in crate::db) const MAX_PERSISTED_STORE_ALLOCATIONS: usize = 38;
const STORE_ALLOCATION_ROLES: usize = 4;
const MAX_STABLE_KEY_BYTES: usize = 128;
const COMMIT_CONTROL_MAGIC: [u8; 4] = *b"ICCS";
const COMMIT_CONTROL_STATE_VERSION_PREDECESSOR: u8 = 2;
const COMMIT_CONTROL_STATE_VERSION_CURRENT: u8 = 3;
const DATABASE_INCARNATION_BYTES: usize = 16;
const CURSOR_AUTHENTICATION_KEY_BYTES: usize = 32;
const DATABASE_COMMIT_SEQUENCE_BYTES: usize = 8;
pub(super) const DATABASE_COMMIT_SEQUENCE_OFFSET: usize =
    COMMIT_CONTROL_MAGIC.len() + 1 + DATABASE_INCARNATION_BYTES + CURSOR_AUTHENTICATION_KEY_BYTES;
const REGISTRY_COUNT_BYTES: usize = 1;
const COMMIT_MARKER_LENGTH_BYTES: usize = 4;
const COMMIT_MARKER_HEADER_BYTES: usize = 5;
const CURRENT_CONTROL_PREFIX_BYTES: usize = COMMIT_CONTROL_MAGIC.len()
    + 1
    + DATABASE_INCARNATION_BYTES
    + CURSOR_AUTHENTICATION_KEY_BYTES
    + DATABASE_COMMIT_SEQUENCE_BYTES
    + REGISTRY_COUNT_BYTES;
pub(super) const PREDECESSOR_COMMIT_CONTROL_HEADER_BYTES: usize = COMMIT_CONTROL_MAGIC.len()
    + 1
    + DATABASE_INCARNATION_BYTES
    + CURSOR_AUTHENTICATION_KEY_BYTES
    + COMMIT_MARKER_LENGTH_BYTES;
pub(super) const MAX_CURRENT_COMMIT_CONTROL_HEADER_BYTES: usize = CURRENT_CONTROL_PREFIX_BYTES
    + MAX_PERSISTED_STORE_ALLOCATIONS * (1 + STORE_ALLOCATION_ROLES * (2 + MAX_STABLE_KEY_BYTES))
    + COMMIT_MARKER_LENGTH_BYTES;

/// Current encoded commit-control bytes retained through the live apply phase.
///
/// Journal batch ranges point into the exact bytes already persisted as marker
/// authority, allowing normal apply to append those bytes without recomputing
/// the batch fingerprint.
#[derive(Debug)]
pub(in crate::db::commit) struct EncodedCommitControlSlot {
    bytes: Vec<u8>,
    marker_length_offset: usize,
    journal_batch_ranges: Vec<Range<usize>>,
}

impl EncodedCommitControlSlot {
    pub(in crate::db::commit) const fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub(in crate::db::commit) const fn marker_length_offset(&self) -> usize {
        self.marker_length_offset
    }

    pub(in crate::db::commit) fn journal_batch_bytes(
        &self,
        ordinal: usize,
    ) -> Result<&[u8], InternalError> {
        let range = self
            .journal_batch_ranges
            .get(ordinal)
            .ok_or_else(InternalError::store_invariant)?;
        self.bytes
            .get(range.clone())
            .ok_or_else(InternalError::store_invariant)
    }
}

/// Lifecycle state for one bounded persisted store-allocation quartet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum PersistedStoreAllocationState {
    Active,
    Retired,
}

/// Owned stable-memory allocation identity retained after generated removal.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct PersistedStoreAllocationIdentity {
    memory_id: u8,
    stable_key: String,
}

impl PersistedStoreAllocationIdentity {
    fn from_runtime(identity: StoreAllocationIdentity) -> Result<Self, InternalError> {
        validate_stable_key(identity.stable_key())?;
        Ok(Self {
            memory_id: identity.memory_id(),
            stable_key: identity.stable_key().to_string(),
        })
    }

    pub(in crate::db) const fn memory_id(&self) -> u8 {
        self.memory_id
    }

    pub(in crate::db) const fn stable_key(&self) -> &str {
        self.stable_key.as_str()
    }
}

/// One active or retired data/index/schema/journal allocation quartet.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct PersistedStoreAllocation {
    state: PersistedStoreAllocationState,
    roles: [PersistedStoreAllocationIdentity; STORE_ALLOCATION_ROLES],
}

impl PersistedStoreAllocation {
    pub(in crate::db) fn active(
        allocations: StoreAllocationIdentities,
    ) -> Result<Self, InternalError> {
        let roles = [
            required_allocation(allocations.data())?,
            required_allocation(allocations.index())?,
            required_allocation(allocations.schema())?,
            required_allocation(allocations.journal())?,
        ];
        Ok(Self {
            state: PersistedStoreAllocationState::Active,
            roles: [
                PersistedStoreAllocationIdentity::from_runtime(roles[0])?,
                PersistedStoreAllocationIdentity::from_runtime(roles[1])?,
                PersistedStoreAllocationIdentity::from_runtime(roles[2])?,
                PersistedStoreAllocationIdentity::from_runtime(roles[3])?,
            ],
        })
    }

    pub(in crate::db) const fn state(&self) -> PersistedStoreAllocationState {
        self.state
    }

    pub(in crate::db) const fn roles(
        &self,
    ) -> &[PersistedStoreAllocationIdentity; STORE_ALLOCATION_ROLES] {
        &self.roles
    }

    pub(in crate::db) const fn journal(&self) -> &PersistedStoreAllocationIdentity {
        &self.roles[3]
    }

    pub(in crate::db) const fn retired(mut self) -> Self {
        self.state = PersistedStoreAllocationState::Retired;
        self
    }
}

/// Borrowed current-form commit-control authority.
pub(super) struct CommitControlSlotRef<'a> {
    pub(super) database_incarnation_id: DatabaseIncarnationId,
    pub(super) cursor_authentication_key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES],
    pub(super) database_commit_sequence: u64,
    pub(super) registry: Vec<PersistedStoreAllocation>,
    pub(super) marker_length_offset: usize,
    pub(super) marker_bytes: &'a [u8],
}

pub(super) struct CommitControlHeader {
    pub(super) database_incarnation_id: DatabaseIncarnationId,
    pub(super) header_len: usize,
    pub(super) marker_len: usize,
    pub(super) current_format: bool,
}

struct ControlSlotLengths {
    payload_size: usize,
    marker_length: u32,
    capacity: usize,
}

fn control_slot_exceeds_max_size() -> InternalError {
    InternalError::commit_marker_exceeds_max_size()
}

fn control_slot_canonical_envelope_required() -> InternalError {
    InternalError::commit_corruption()
}

pub(super) fn decode_commit_control_slot(bytes: &[u8]) -> Result<Vec<u8>, InternalError> {
    let slot = inspect_commit_control_slot(bytes)?;
    Ok(slot.marker_bytes.to_vec())
}

pub(super) fn inspect_commit_control_slot(
    bytes: &[u8],
) -> Result<CommitControlSlotRef<'_>, InternalError> {
    if bytes.len() > MAX_COMMIT_BYTES as usize {
        return Err(control_slot_exceeds_max_size());
    }
    let parsed = parse_current_control(bytes)?;
    if parsed.encoded_len != bytes.len() {
        return Err(control_slot_canonical_envelope_required());
    }
    let marker_bytes = bytes
        .get(parsed.marker_offset..parsed.encoded_len)
        .ok_or_else(control_slot_canonical_envelope_required)?;
    let marker_length_offset = parsed
        .marker_offset
        .checked_sub(COMMIT_MARKER_LENGTH_BYTES)
        .ok_or_else(control_slot_canonical_envelope_required)?;
    Ok(CommitControlSlotRef {
        database_incarnation_id: parsed.database_incarnation_id,
        cursor_authentication_key: parsed.cursor_authentication_key,
        database_commit_sequence: parsed.database_commit_sequence,
        registry: parsed.registry,
        marker_length_offset,
        marker_bytes,
    })
}

pub(super) fn commit_control_slot_encoded_len(bytes: &[u8]) -> Result<usize, InternalError> {
    parse_current_control(bytes).map(|parsed| parsed.encoded_len)
}

pub(super) fn inspect_commit_control_header(
    bytes: &[u8],
) -> Result<CommitControlHeader, InternalError> {
    let version = control_version(bytes)?;
    match version {
        COMMIT_CONTROL_STATE_VERSION_CURRENT => {
            let parsed = parse_current_control(bytes)?;
            Ok(CommitControlHeader {
                database_incarnation_id: parsed.database_incarnation_id,
                header_len: parsed.marker_offset,
                marker_len: parsed.marker_len,
                current_format: true,
            })
        }
        COMMIT_CONTROL_STATE_VERSION_PREDECESSOR => {
            let parsed = parse_predecessor_control(bytes)?;
            Ok(CommitControlHeader {
                database_incarnation_id: parsed.database_incarnation_id,
                header_len: PREDECESSOR_COMMIT_CONTROL_HEADER_BYTES,
                marker_len: parsed.marker_len,
                current_format: false,
            })
        }
        _ => Err(InternalError::serialize_incompatible_persisted_format()),
    }
}

pub(super) fn encode_empty_commit_control_slot(
    database_incarnation_id: DatabaseIncarnationId,
    cursor_authentication_key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES],
    database_commit_sequence: u64,
    registry: &[PersistedStoreAllocation],
) -> Result<Vec<u8>, InternalError> {
    let mut encoded = Vec::new();
    write_current_control_prefix(
        &mut encoded,
        database_incarnation_id,
        cursor_authentication_key,
        database_commit_sequence,
        registry,
    )?;
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    Ok(encoded)
}

#[cfg(test)]
pub(super) fn encode_commit_control_slot(
    database_incarnation_id: DatabaseIncarnationId,
    cursor_authentication_key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES],
    marker_bytes: &[u8],
) -> Result<Vec<u8>, InternalError> {
    let marker_len = u32::try_from(marker_bytes.len())
        .map_err(|_| InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit())?;
    let mut encoded = Vec::new();
    write_current_control_prefix(
        &mut encoded,
        database_incarnation_id,
        cursor_authentication_key,
        0,
        &[],
    )?;
    encoded.extend_from_slice(&marker_len.to_le_bytes());
    encoded.extend_from_slice(marker_bytes);
    if encoded.len() > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size());
    }
    Ok(encoded)
}

pub(super) fn encode_commit_control_slot_from_marker(
    current_bytes: &[u8],
    current: &CommitControlSlotRef<'_>,
    database_commit_sequence: u64,
    marker: &CommitMarker,
) -> Result<EncodedCommitControlSlot, InternalError> {
    validate_commit_marker_shape(marker)?;
    let marker_payload_len = commit_marker_payload_capacity(marker);
    let permanent_len = current.marker_length_offset;
    let lengths = checked_control_slot_lengths(permanent_len, marker_payload_len)?;

    let mut encoded = Vec::with_capacity(lengths.capacity);
    encoded.extend_from_slice(
        current_bytes
            .get(..permanent_len)
            .ok_or_else(control_slot_canonical_envelope_required)?,
    );
    let sequence_end = DATABASE_COMMIT_SEQUENCE_OFFSET
        .checked_add(DATABASE_COMMIT_SEQUENCE_BYTES)
        .ok_or_else(control_slot_canonical_envelope_required)?;
    encoded
        .get_mut(DATABASE_COMMIT_SEQUENCE_OFFSET..sequence_end)
        .ok_or_else(control_slot_canonical_envelope_required)?
        .copy_from_slice(&database_commit_sequence.to_le_bytes());
    encoded.extend_from_slice(&lengths.marker_length.to_le_bytes());
    write_commit_marker_envelope_header(&mut encoded, lengths.payload_size)?;
    let journal_batch_ranges = write_commit_marker_payload(&mut encoded, marker)?;

    Ok(EncodedCommitControlSlot {
        bytes: encoded,
        marker_length_offset: permanent_len,
        journal_batch_ranges,
    })
}

fn checked_control_slot_lengths(
    permanent_len: usize,
    marker_payload_len: usize,
) -> Result<ControlSlotLengths, InternalError> {
    let marker_bytes_len = COMMIT_MARKER_HEADER_BYTES
        .checked_add(marker_payload_len)
        .ok_or_else(InternalError::commit_control_slot_exceeds_max_size)?;
    let marker_length = u32::try_from(marker_bytes_len)
        .map_err(|_| InternalError::commit_control_slot_marker_bytes_exceed_u32_length_limit())?;
    let capacity = permanent_len
        .checked_add(COMMIT_MARKER_LENGTH_BYTES)
        .and_then(|len| len.checked_add(marker_bytes_len))
        .ok_or_else(InternalError::commit_control_slot_exceeds_max_size)?;
    if capacity > MAX_COMMIT_BYTES as usize {
        return Err(InternalError::commit_control_slot_exceeds_max_size());
    }
    Ok(ControlSlotLengths {
        payload_size: marker_payload_len,
        marker_length,
        capacity,
    })
}

struct ParsedCurrentControl {
    database_incarnation_id: DatabaseIncarnationId,
    cursor_authentication_key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES],
    database_commit_sequence: u64,
    registry: Vec<PersistedStoreAllocation>,
    marker_offset: usize,
    marker_len: usize,
    encoded_len: usize,
}

fn parse_current_control(bytes: &[u8]) -> Result<ParsedCurrentControl, InternalError> {
    if control_version(bytes)? != COMMIT_CONTROL_STATE_VERSION_CURRENT {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    if bytes.len() < CURRENT_CONTROL_PREFIX_BYTES + COMMIT_MARKER_LENGTH_BYTES {
        return Err(control_slot_canonical_envelope_required());
    }
    let mut cursor = COMMIT_CONTROL_MAGIC.len() + 1;
    let database_incarnation_id = read_incarnation(bytes, &mut cursor)?;
    let cursor_authentication_key = read_cursor_key(bytes, &mut cursor)?;
    let database_commit_sequence = read_u64(bytes, &mut cursor)?;
    let registry_count = usize::from(read_u8(bytes, &mut cursor)?);
    if registry_count > MAX_PERSISTED_STORE_ALLOCATIONS {
        return Err(control_slot_canonical_envelope_required());
    }
    let mut registry = Vec::with_capacity(registry_count);
    for _ in 0..registry_count {
        registry.push(read_registry_entry(bytes, &mut cursor)?);
    }
    validate_registry(&registry)?;
    let marker_len = read_u32_le(bytes, &mut cursor, "commit control-slot")? as usize;
    let encoded_len = cursor
        .checked_add(marker_len)
        .ok_or_else(control_slot_canonical_envelope_required)?;
    if encoded_len > MAX_COMMIT_BYTES as usize {
        return Err(control_slot_exceeds_max_size());
    }
    Ok(ParsedCurrentControl {
        database_incarnation_id,
        cursor_authentication_key,
        database_commit_sequence,
        registry,
        marker_offset: cursor,
        marker_len,
        encoded_len,
    })
}

struct ParsedPredecessorControl {
    database_incarnation_id: DatabaseIncarnationId,
    cursor_authentication_key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES],
    marker_len: usize,
}

fn parse_predecessor_control(bytes: &[u8]) -> Result<ParsedPredecessorControl, InternalError> {
    if bytes.len() < PREDECESSOR_COMMIT_CONTROL_HEADER_BYTES
        || control_version(bytes)? != COMMIT_CONTROL_STATE_VERSION_PREDECESSOR
    {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    let mut cursor = COMMIT_CONTROL_MAGIC.len() + 1;
    let database_incarnation_id = read_incarnation(bytes, &mut cursor)?;
    let cursor_authentication_key = read_cursor_key(bytes, &mut cursor)?;
    let marker_len = read_u32_le(bytes, &mut cursor, "predecessor commit control-slot")? as usize;
    let encoded_len = cursor
        .checked_add(marker_len)
        .ok_or_else(control_slot_canonical_envelope_required)?;
    if encoded_len != bytes.len() || encoded_len > MAX_COMMIT_BYTES as usize {
        return Err(control_slot_canonical_envelope_required());
    }
    Ok(ParsedPredecessorControl {
        database_incarnation_id,
        cursor_authentication_key,
        marker_len,
    })
}

#[cfg(test)]
pub(super) fn encode_empty_predecessor_commit_control_slot(
    database_incarnation_id: DatabaseIncarnationId,
    cursor_authentication_key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES],
) -> Result<Vec<u8>, InternalError> {
    validate_cursor_key(cursor_authentication_key)?;
    let mut encoded = Vec::with_capacity(PREDECESSOR_COMMIT_CONTROL_HEADER_BYTES);
    encoded.extend_from_slice(&COMMIT_CONTROL_MAGIC);
    encoded.push(COMMIT_CONTROL_STATE_VERSION_PREDECESSOR);
    encoded.extend_from_slice(&database_incarnation_id.to_bytes());
    encoded.extend_from_slice(&cursor_authentication_key);
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    Ok(encoded)
}

pub(in crate::db::commit) fn predecessor_empty_control_identity(
    bytes: &[u8],
) -> Result<(DatabaseIncarnationId, [u8; CURSOR_AUTHENTICATION_KEY_BYTES]), InternalError> {
    let parsed = parse_predecessor_control(bytes)?;
    if parsed.marker_len != 0 {
        return Err(InternalError::store_unsupported());
    }
    Ok((
        parsed.database_incarnation_id,
        parsed.cursor_authentication_key,
    ))
}

fn write_current_control_prefix(
    out: &mut Vec<u8>,
    database_incarnation_id: DatabaseIncarnationId,
    cursor_authentication_key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES],
    database_commit_sequence: u64,
    registry: &[PersistedStoreAllocation],
) -> Result<(), InternalError> {
    validate_cursor_key(cursor_authentication_key)?;
    validate_registry(registry)?;
    out.extend_from_slice(&COMMIT_CONTROL_MAGIC);
    out.push(COMMIT_CONTROL_STATE_VERSION_CURRENT);
    out.extend_from_slice(&database_incarnation_id.to_bytes());
    out.extend_from_slice(&cursor_authentication_key);
    out.extend_from_slice(&database_commit_sequence.to_le_bytes());
    out.push(u8::try_from(registry.len()).map_err(|_| InternalError::store_unsupported())?);
    for entry in registry {
        out.push(match entry.state {
            PersistedStoreAllocationState::Active => 1,
            PersistedStoreAllocationState::Retired => 2,
        });
        for role in &entry.roles {
            out.push(role.memory_id);
            out.push(
                u8::try_from(role.stable_key.len())
                    .map_err(|_| InternalError::store_unsupported())?,
            );
            out.extend_from_slice(role.stable_key.as_bytes());
        }
    }
    Ok(())
}

fn read_registry_entry(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<PersistedStoreAllocation, InternalError> {
    let state = match read_u8(bytes, cursor)? {
        1 => PersistedStoreAllocationState::Active,
        2 => PersistedStoreAllocationState::Retired,
        _ => return Err(control_slot_canonical_envelope_required()),
    };
    let mut roles = Vec::with_capacity(STORE_ALLOCATION_ROLES);
    for _ in 0..STORE_ALLOCATION_ROLES {
        let memory_id = read_u8(bytes, cursor)?;
        let key_len = usize::from(read_u8(bytes, cursor)?);
        let key_end = cursor
            .checked_add(key_len)
            .ok_or_else(control_slot_canonical_envelope_required)?;
        let key_bytes = bytes
            .get(*cursor..key_end)
            .ok_or_else(control_slot_canonical_envelope_required)?;
        *cursor = key_end;
        let stable_key = std::str::from_utf8(key_bytes)
            .map_err(|_| control_slot_canonical_envelope_required())?;
        roles.push(PersistedStoreAllocationIdentity {
            memory_id,
            stable_key: stable_key.to_string(),
        });
    }
    let roles = roles
        .try_into()
        .map_err(|_| control_slot_canonical_envelope_required())?;
    Ok(PersistedStoreAllocation { state, roles })
}

fn validate_registry(registry: &[PersistedStoreAllocation]) -> Result<(), InternalError> {
    if registry.len() > MAX_PERSISTED_STORE_ALLOCATIONS {
        return Err(InternalError::store_unsupported());
    }
    let mut prior = None;
    let mut memory_ids = Vec::with_capacity(registry.len() * STORE_ALLOCATION_ROLES);
    let mut stable_keys = Vec::with_capacity(registry.len() * STORE_ALLOCATION_ROLES);
    for entry in registry {
        for role in &entry.roles {
            validate_stable_key(role.stable_key())?;
            if memory_ids.contains(&role.memory_id) || stable_keys.contains(&role.stable_key()) {
                return Err(control_slot_canonical_envelope_required());
            }
            memory_ids.push(role.memory_id);
            stable_keys.push(role.stable_key());
        }
        let key = entry
            .roles
            .iter()
            .map(|role| role.memory_id)
            .collect::<Vec<_>>();
        if prior.as_ref().is_some_and(|prior: &Vec<u8>| prior >= &key) {
            return Err(control_slot_canonical_envelope_required());
        }
        prior = Some(key);
    }
    Ok(())
}

pub(in crate::db) fn canonicalize_store_registry(
    registry: &mut [PersistedStoreAllocation],
) -> Result<(), InternalError> {
    registry.sort_by(|left, right| {
        left.roles
            .iter()
            .map(|role| role.memory_id)
            .cmp(right.roles.iter().map(|role| role.memory_id))
    });
    validate_registry(registry)
}

fn control_version(bytes: &[u8]) -> Result<u8, InternalError> {
    if bytes.get(..COMMIT_CONTROL_MAGIC.len()) != Some(COMMIT_CONTROL_MAGIC.as_slice()) {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    bytes
        .get(COMMIT_CONTROL_MAGIC.len())
        .copied()
        .ok_or_else(control_slot_canonical_envelope_required)
}

fn read_incarnation(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<DatabaseIncarnationId, InternalError> {
    let end = cursor.saturating_add(DATABASE_INCARNATION_BYTES);
    let encoded: [u8; DATABASE_INCARNATION_BYTES] = bytes
        .get(*cursor..end)
        .ok_or_else(control_slot_canonical_envelope_required)?
        .try_into()
        .map_err(|_| control_slot_canonical_envelope_required())?;
    *cursor = end;
    DatabaseIncarnationId::try_from_bytes(encoded)
}

fn read_cursor_key(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<[u8; CURSOR_AUTHENTICATION_KEY_BYTES], InternalError> {
    let end = cursor.saturating_add(CURSOR_AUTHENTICATION_KEY_BYTES);
    let key = bytes
        .get(*cursor..end)
        .ok_or_else(control_slot_canonical_envelope_required)?
        .try_into()
        .map_err(|_| control_slot_canonical_envelope_required())?;
    *cursor = end;
    validate_cursor_key(key)?;
    Ok(key)
}

fn validate_cursor_key(key: [u8; CURSOR_AUTHENTICATION_KEY_BYTES]) -> Result<(), InternalError> {
    if key == [0; CURSOR_AUTHENTICATION_KEY_BYTES] {
        return Err(control_slot_canonical_envelope_required());
    }
    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, InternalError> {
    let value = bytes
        .get(*cursor)
        .copied()
        .ok_or_else(control_slot_canonical_envelope_required)?;
    *cursor = cursor.saturating_add(1);
    Ok(value)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, InternalError> {
    let end = cursor.saturating_add(size_of::<u64>());
    let encoded = bytes
        .get(*cursor..end)
        .ok_or_else(control_slot_canonical_envelope_required)?
        .try_into()
        .map_err(|_| control_slot_canonical_envelope_required())?;
    *cursor = end;
    Ok(u64::from_le_bytes(encoded))
}

fn validate_stable_key(stable_key: &str) -> Result<(), InternalError> {
    ic_memory::StableKey::parse(stable_key)
        .map(|_| ())
        .map_err(|_| control_slot_canonical_envelope_required())
}

fn required_allocation(
    allocation: Option<StoreAllocationIdentity>,
) -> Result<StoreAllocationIdentity, InternalError> {
    allocation.ok_or_else(InternalError::store_invariant)
}
