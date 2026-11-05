//! Module: db::startup::receipt
//! Responsibility: encode and persist one bounded current startup-failure receipt.
//! Does not own: failure classification, recovery progress, or publication policy.
//! Boundary: typed failure and maintained binding -> one 2,048-byte stable control cell.

use ic_storage::{DefaultMemoryImpl, Memory, RuntimeMemory, StableKey};
use icydb_diagnostic_code::{
    DiagnosticFactTag, ErrorCode, ErrorOrigin, MAX_PUBLIC_DIAGNOSTIC_FACTS,
    validate_raw_diagnostic_fact_schema,
};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::cell::RefCell;

use crate::{
    db::{
        codec::{ByteDecodeError, ByteReader},
        integrity::DatabaseIncarnationId,
        journal::JournalTailProofIdentity,
        registry::StoreAllocationIdentity,
        startup::{StartupFailure, StartupFailureKind},
    },
    error::InternalError,
    traits::CanisterKind,
};
use icydb_schema::{ExpectedAcceptedHead, SchemaSubmissionKey};

#[cfg(not(test))]
use crate::db::open_memory;

pub(in crate::db) const MAX_STARTUP_FAILURE_RECEIPT_BYTES: usize = 2_048;
const RECEIPT_MAGIC: &[u8; 8] = b"ICYSUP01";
const RECEIPT_VERSION: u8 = 1;
const RECEIPT_HEADER_BYTES: usize = 15;
const WASM_PAGE_BYTES: u64 = 65_536;
const MAX_BINDING_KEY_BYTES: usize = 128;
const FAILURE_IDENTITY_BYTES: usize = 1 + 2 + 1 + 1;
const MAX_DIAGNOSTIC_FACT_BYTES: usize = MAX_PUBLIC_DIAGNOSTIC_FACTS * (1 + 8);
const MAX_JOURNAL_BINDING_BYTES: usize = 1 + 16 + 8 + 1 + MAX_BINDING_KEY_BYTES + 40;
pub(in crate::db) const MAX_ENCODED_STARTUP_FAILURE_RECEIPT_BYTES: usize = RECEIPT_HEADER_BYTES
    + FAILURE_IDENTITY_BYTES
    + MAX_DIAGNOSTIC_FACT_BYTES
    + MAX_JOURNAL_BINDING_BYTES;
const _: () =
    assert!(MAX_ENCODED_STARTUP_FAILURE_RECEIPT_BYTES <= MAX_STARTUP_FAILURE_RECEIPT_BYTES);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum StartupFailureBinding {
    DatabaseControl {
        commit_memory_id: u64,
        commit_stable_key: String,
        control: Option<DatabaseControlBinding>,
    },
    JournalRecovery {
        incarnation: DatabaseIncarnationId,
        allocation: StoreAllocationIdentityOwned,
        proof: JournalTailProofIdentity,
    },
    SchemaReconciliation {
        incarnation: DatabaseIncarnationId,
        submission_key: String,
        accepted_head: AcceptedHeadBinding,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct DatabaseControlBinding {
    pub(in crate::db) incarnation: DatabaseIncarnationId,
    pub(in crate::db) proof: [u8; 32],
}

impl DatabaseControlBinding {
    pub(in crate::db) const fn new(incarnation: DatabaseIncarnationId, proof: [u8; 32]) -> Self {
        Self { incarnation, proof }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StoreAllocationIdentityOwned {
    pub(in crate::db) memory_id: u64,
    pub(in crate::db) stable_key: String,
}

impl StoreAllocationIdentityOwned {
    pub(in crate::db) fn from_identity(identity: StoreAllocationIdentity) -> Self {
        Self {
            memory_id: identity.memory_id(),
            stable_key: identity.stable_key().to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedHeadBinding {
    Empty,
    Exact {
        revision: u64,
        fingerprint: [u8; 32],
    },
}

impl AcceptedHeadBinding {
    /// Preserve the accepted revision and fingerprint in a startup receipt binding.
    pub(super) const fn from_expected_head(head: &ExpectedAcceptedHead) -> Self {
        match head {
            ExpectedAcceptedHead::Empty => Self::Empty,
            ExpectedAcceptedHead::Exact {
                revision,
                fingerprint,
            } => Self::Exact {
                revision: *revision,
                fingerprint: fingerprint.to_bytes(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct StartupFailureReceipt {
    failure: StartupFailure,
    binding: StartupFailureBinding,
}

impl StartupFailureReceipt {
    pub(in crate::db) fn new(
        failure: StartupFailure,
        binding: StartupFailureBinding,
    ) -> Result<Self, InternalError> {
        let receipt = Self { failure, binding };
        validate_receipt(&receipt)?;
        Ok(receipt)
    }

    pub(in crate::db) const fn failure(&self) -> &StartupFailure {
        &self.failure
    }

    pub(in crate::db) const fn binding(&self) -> &StartupFailureBinding {
        &self.binding
    }
}

pub(in crate::db) fn load<C: CanisterKind>() -> Result<Option<StartupFailureReceipt>, InternalError>
{
    decode_cell(&startup_memory::<C>()?)
}

pub(in crate::db) fn publish<C: CanisterKind>(
    receipt: &StartupFailureReceipt,
) -> Result<bool, InternalError> {
    let encoded = encode_receipt(receipt)?;
    let memory = startup_memory::<C>()?;
    if decode_cell(&memory)?.as_ref() == Some(receipt) {
        return Ok(false);
    }
    write_cell(&memory, encoded.as_slice())?;
    Ok(true)
}

pub(in crate::db) fn clear<C: CanisterKind>() -> Result<bool, InternalError> {
    let memory = startup_memory::<C>()?;
    if decode_cell(&memory)?.is_none() {
        return Ok(false);
    }
    write_cell(&memory, &[])?;
    Ok(true)
}

fn decode_cell<M: Memory>(memory: &M) -> Result<Option<StartupFailureReceipt>, InternalError> {
    if memory.size() == 0 {
        return Ok(None);
    }
    if memory.size() != 1 {
        return Err(InternalError::startup_control_corruption());
    }
    let mut cell = [0_u8; MAX_STARTUP_FAILURE_RECEIPT_BYTES];
    memory.read(0, &mut cell);
    if cell.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    if &cell[..RECEIPT_MAGIC.len()] != RECEIPT_MAGIC {
        return Err(InternalError::startup_control_corruption());
    }
    if cell[8] != RECEIPT_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    let payload_len = usize::from(u16::from_le_bytes([cell[9], cell[10]]));
    let end = RECEIPT_HEADER_BYTES
        .checked_add(payload_len)
        .ok_or_else(InternalError::startup_control_corruption)?;
    if end > cell.len() || cell[end..].iter().any(|byte| *byte != 0) {
        return Err(InternalError::startup_control_corruption());
    }
    let payload = &cell[RECEIPT_HEADER_BYTES..end];
    let stored_checksum = u32::from_le_bytes([cell[11], cell[12], cell[13], cell[14]]);
    if stored_checksum != checksum(payload) {
        return Err(InternalError::startup_control_corruption());
    }
    decode_payload(payload).map(Some)
}

fn write_cell<M: Memory>(memory: &RuntimeMemory<M>, encoded: &[u8]) -> Result<(), InternalError> {
    if encoded.len() > MAX_STARTUP_FAILURE_RECEIPT_BYTES {
        return Err(InternalError::store_invariant());
    }
    if memory.size() == 0 {
        memory
            .grow(1)
            .map_err(|_| InternalError::recovery_database_format_control_unavailable())?;
    }
    let memory_bytes = memory
        .size()
        .checked_mul(WASM_PAGE_BYTES)
        .ok_or_else(InternalError::startup_control_corruption)?;
    if memory.size() != 1 || memory_bytes < MAX_STARTUP_FAILURE_RECEIPT_BYTES as u64 {
        return Err(InternalError::startup_control_corruption());
    }
    let mut cell = [0_u8; MAX_STARTUP_FAILURE_RECEIPT_BYTES];
    cell[..encoded.len()].copy_from_slice(encoded);
    memory.write(0, &cell);
    Ok(())
}

fn encode_receipt(receipt: &StartupFailureReceipt) -> Result<Vec<u8>, InternalError> {
    validate_receipt(receipt)?;
    let mut payload = Vec::new();
    payload.push(encode_kind(receipt.failure.kind()));
    payload.extend_from_slice(
        &receipt
            .failure
            .diagnostic()
            .error_code()
            .raw()
            .to_le_bytes(),
    );
    payload.push(receipt.failure.diagnostic().origin().wire_code());
    payload.push(
        u8::try_from(receipt.failure.facts().len())
            .map_err(|_| InternalError::store_invariant())?,
    );
    for (tag, value) in receipt.failure.facts() {
        payload.push(tag.raw());
        payload.extend_from_slice(&value.to_le_bytes());
    }
    encode_binding(&mut payload, receipt.binding())?;
    let payload_len = u16::try_from(payload.len()).map_err(|_| InternalError::store_invariant())?;
    let total_len = RECEIPT_HEADER_BYTES
        .checked_add(payload.len())
        .ok_or_else(InternalError::store_invariant)?;
    if total_len > MAX_STARTUP_FAILURE_RECEIPT_BYTES {
        return Err(InternalError::store_invariant());
    }
    let mut encoded = Vec::with_capacity(total_len);
    encoded.extend_from_slice(RECEIPT_MAGIC);
    encoded.push(RECEIPT_VERSION);
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(&checksum(payload.as_slice()).to_le_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn decode_payload(payload: &[u8]) -> Result<StartupFailureReceipt, InternalError> {
    let mut reader = ByteReader::new(payload);
    let kind = decode_kind(reader.read_u8().map_err(primitive_decode_error)?)?;
    let code = ErrorCode::known(reader.read_u16_le().map_err(primitive_decode_error)?)
        .ok_or_else(InternalError::startup_control_corruption)?;
    let origin =
        ErrorOrigin::from_known_wire_code(reader.read_u8().map_err(primitive_decode_error)?)
            .ok_or_else(InternalError::startup_control_corruption)?;
    let fact_count = usize::from(reader.read_u8().map_err(primitive_decode_error)?);
    if fact_count > MAX_PUBLIC_DIAGNOSTIC_FACTS {
        return Err(InternalError::startup_control_corruption());
    }
    let mut facts = Vec::with_capacity(fact_count);
    let mut raw_facts = Vec::with_capacity(fact_count);
    for _ in 0..fact_count {
        let raw_tag = reader.read_u8().map_err(primitive_decode_error)?;
        let tag = DiagnosticFactTag::known(raw_tag)
            .ok_or_else(InternalError::startup_control_corruption)?;
        let value = reader.read_u64_le().map_err(primitive_decode_error)?;
        facts.push((tag, value));
        raw_facts.push((raw_tag, value));
    }
    validate_raw_diagnostic_fact_schema(code, raw_facts.as_slice())
        .map_err(|_| InternalError::startup_control_corruption())?;
    let binding = decode_binding(&mut reader)?;
    reader.finish().map_err(primitive_decode_error)?;
    StartupFailureReceipt::new(
        StartupFailure::new(kind, code.diagnostic(origin), facts),
        binding,
    )
    .map_err(|_| InternalError::startup_control_corruption())
}

fn validate_receipt(receipt: &StartupFailureReceipt) -> Result<(), InternalError> {
    if receipt.failure.facts().len() > MAX_PUBLIC_DIAGNOSTIC_FACTS {
        return Err(InternalError::store_invariant());
    }
    let raw_facts = receipt
        .failure
        .facts()
        .iter()
        .map(|(tag, value)| (tag.raw(), *value))
        .collect::<Vec<_>>();
    validate_raw_diagnostic_fact_schema(
        receipt.failure.diagnostic().error_code(),
        raw_facts.as_slice(),
    )
    .map_err(|_| InternalError::store_invariant())?;
    let kind_matches = matches!(
        (receipt.failure.kind(), receipt.binding()),
        (
            StartupFailureKind::DatabaseControl,
            StartupFailureBinding::DatabaseControl { .. }
        ) | (
            StartupFailureKind::JournalRecovery,
            StartupFailureBinding::JournalRecovery { .. }
        ) | (
            StartupFailureKind::SchemaReconciliation,
            StartupFailureBinding::SchemaReconciliation { .. }
        )
    );
    if !kind_matches {
        return Err(InternalError::store_invariant());
    }
    if !super::receipt_code_for_kind(
        receipt.failure.kind(),
        receipt.failure.diagnostic().error_code(),
    ) {
        return Err(InternalError::store_invariant());
    }
    match receipt.binding() {
        StartupFailureBinding::DatabaseControl {
            commit_stable_key, ..
        } => validate_stable_key(commit_stable_key),
        StartupFailureBinding::JournalRecovery {
            allocation, proof, ..
        } => {
            validate_stable_key(&allocation.stable_key)?;
            if !proof.is_well_formed() {
                return Err(InternalError::store_invariant());
            }
            Ok(())
        }
        StartupFailureBinding::SchemaReconciliation {
            submission_key,
            accepted_head,
            ..
        } => {
            SchemaSubmissionKey::try_new(submission_key.clone())
                .map_err(|_| InternalError::store_invariant())?;
            if matches!(
                accepted_head,
                AcceptedHeadBinding::Exact { revision: 0, .. }
            ) {
                return Err(InternalError::store_invariant());
            }
            Ok(())
        }
    }
}

fn encode_binding(out: &mut Vec<u8>, binding: &StartupFailureBinding) -> Result<(), InternalError> {
    match binding {
        StartupFailureBinding::DatabaseControl {
            commit_memory_id,
            commit_stable_key,
            control,
        } => {
            out.push(1);
            out.extend_from_slice(&commit_memory_id.to_le_bytes());
            write_string(out, commit_stable_key)?;
            match control {
                None => out.push(0),
                Some(control) => {
                    out.push(1);
                    out.extend_from_slice(&control.incarnation.to_bytes());
                    out.extend_from_slice(&control.proof);
                }
            }
        }
        StartupFailureBinding::JournalRecovery {
            incarnation,
            allocation,
            proof,
        } => {
            out.push(2);
            out.extend_from_slice(&incarnation.to_bytes());
            out.extend_from_slice(&allocation.memory_id.to_le_bytes());
            write_string(out, &allocation.stable_key)?;
            out.extend_from_slice(&proof.data_mutation_revision().to_le_bytes());
            out.extend_from_slice(&proof.fold_sequence().to_le_bytes());
            out.extend_from_slice(&proof.fold_epoch().to_le_bytes());
            out.extend_from_slice(&proof.next_append_sequence().to_le_bytes());
            out.extend_from_slice(&proof.physical_record_count().to_le_bytes());
        }
        StartupFailureBinding::SchemaReconciliation {
            incarnation,
            submission_key,
            accepted_head,
        } => {
            out.push(3);
            out.extend_from_slice(&incarnation.to_bytes());
            write_string(out, submission_key)?;
            match accepted_head {
                AcceptedHeadBinding::Empty => out.push(0),
                AcceptedHeadBinding::Exact {
                    revision,
                    fingerprint,
                } => {
                    out.push(1);
                    out.extend_from_slice(&revision.to_le_bytes());
                    out.extend_from_slice(fingerprint);
                }
            }
        }
    }
    Ok(())
}

fn decode_binding(reader: &mut ByteReader<'_>) -> Result<StartupFailureBinding, InternalError> {
    match reader.read_u8().map_err(primitive_decode_error)? {
        1 => {
            let commit_memory_id = reader.read_u64_le().map_err(primitive_decode_error)?;
            let commit_stable_key = read_binding_key(reader)?;
            let control = match reader.read_u8().map_err(primitive_decode_error)? {
                0 => None,
                1 => Some(DatabaseControlBinding::new(
                    DatabaseIncarnationId::try_from_bytes(
                        reader.read_array().map_err(primitive_decode_error)?,
                    )
                    .map_err(|_| InternalError::startup_control_corruption())?,
                    reader.read_array().map_err(primitive_decode_error)?,
                )),
                _ => return Err(InternalError::startup_control_corruption()),
            };
            Ok(StartupFailureBinding::DatabaseControl {
                commit_memory_id,
                commit_stable_key,
                control,
            })
        }
        2 => {
            let incarnation = DatabaseIncarnationId::try_from_bytes(
                reader.read_array().map_err(primitive_decode_error)?,
            )
            .map_err(|_| InternalError::startup_control_corruption())?;
            let allocation = StoreAllocationIdentityOwned {
                memory_id: reader.read_u64_le().map_err(primitive_decode_error)?,
                stable_key: read_binding_key(reader)?,
            };
            let proof = JournalTailProofIdentity::from_persisted_parts(
                reader.read_u64_le().map_err(primitive_decode_error)?,
                reader.read_u64_le().map_err(primitive_decode_error)?,
                reader.read_u64_le().map_err(primitive_decode_error)?,
                reader.read_u64_le().map_err(primitive_decode_error)?,
                reader.read_u64_le().map_err(primitive_decode_error)?,
            );
            Ok(StartupFailureBinding::JournalRecovery {
                incarnation,
                allocation,
                proof,
            })
        }
        3 => {
            let incarnation = DatabaseIncarnationId::try_from_bytes(
                reader.read_array().map_err(primitive_decode_error)?,
            )
            .map_err(|_| InternalError::startup_control_corruption())?;
            let submission_key = read_binding_key(reader)?;
            let accepted_head = match reader.read_u8().map_err(primitive_decode_error)? {
                0 => AcceptedHeadBinding::Empty,
                1 => AcceptedHeadBinding::Exact {
                    revision: reader.read_u64_le().map_err(primitive_decode_error)?,
                    fingerprint: reader.read_array().map_err(primitive_decode_error)?,
                },
                _ => return Err(InternalError::startup_control_corruption()),
            };
            Ok(StartupFailureBinding::SchemaReconciliation {
                incarnation,
                submission_key,
                accepted_head,
            })
        }
        _ => Err(InternalError::startup_control_corruption()),
    }
}

const fn encode_kind(kind: StartupFailureKind) -> u8 {
    match kind {
        StartupFailureKind::DatabaseControl => 1,
        StartupFailureKind::JournalRecovery => 2,
        StartupFailureKind::SchemaReconciliation => 3,
    }
}

fn decode_kind(raw: u8) -> Result<StartupFailureKind, InternalError> {
    match raw {
        1 => Ok(StartupFailureKind::DatabaseControl),
        2 => Ok(StartupFailureKind::JournalRecovery),
        3 => Ok(StartupFailureKind::SchemaReconciliation),
        _ => Err(InternalError::startup_control_corruption()),
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), InternalError> {
    validate_bounded_key(value)?;
    let len = u8::try_from(value.len()).map_err(|_| InternalError::store_invariant())?;
    out.push(len);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn validate_bounded_key(value: &str) -> Result<(), InternalError> {
    if value.is_empty() || value.len() > MAX_BINDING_KEY_BYTES {
        return Err(InternalError::store_invariant());
    }
    Ok(())
}

fn validate_stable_key(value: &str) -> Result<(), InternalError> {
    StableKey::parse(value)
        .map(|_| ())
        .map_err(|_| InternalError::store_invariant())
}

fn checksum(payload: &[u8]) -> u32 {
    let mut hasher = Sha256::new();
    hasher.update(b"icydb.startup-failure-receipt.checksum.v1");
    hasher.update(payload);
    let digest: [u8; 32] = hasher.finalize().into();
    u32::from_le_bytes([digest[0], digest[1], digest[2], digest[3]])
}

// Receipt binding keys use a u8 length, not the shared u32 string frame.
fn read_binding_key(reader: &mut ByteReader<'_>) -> Result<String, InternalError> {
    let len = usize::from(reader.read_u8().map_err(primitive_decode_error)?);
    if len == 0 || len > MAX_BINDING_KEY_BYTES {
        return Err(InternalError::startup_control_corruption());
    }
    let bytes = reader.read_exact(len).map_err(primitive_decode_error)?;
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(|_| InternalError::startup_control_corruption())
}

// This codec retains its domain classification for every primitive failure.
fn primitive_decode_error(_: ByteDecodeError) -> InternalError {
    InternalError::startup_control_corruption()
}

#[cfg(test)]
mod tests;

#[cfg(not(test))]
pub(in crate::db) fn startup_memory<C: CanisterKind>()
-> Result<RuntimeMemory<DefaultMemoryImpl>, InternalError> {
    open_memory(C::STARTUP_STABLE_KEY).map_err(|_| InternalError::store_internal())
}

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::startup_memory;
