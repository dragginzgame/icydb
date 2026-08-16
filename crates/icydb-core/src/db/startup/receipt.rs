//! Module: db::startup::receipt
//! Responsibility: encode and persist one bounded current startup-failure receipt.
//! Does not own: failure classification, recovery progress, or publication policy.
//! Boundary: typed failure and maintained binding -> one 2,048-byte stable control cell.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "the publication boundary is wired by the next driver slice"
    )
)]

use ic_memory::StableKey;
use ic_stable_structures::{DefaultMemoryImpl, Memory, memory_manager::VirtualMemory};
use icydb_diagnostic_code::{
    DiagnosticFactTag, ErrorCode, ErrorOrigin, MAX_PUBLIC_DIAGNOSTIC_FACTS,
    validate_raw_diagnostic_fact_schema,
};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::cell::RefCell;

use crate::{
    db::{
        integrity::DatabaseIncarnationId,
        journal::JournalTailProofIdentity,
        registry::StoreAllocationIdentity,
        startup::{StartupFailure, StartupFailureKind},
    },
    error::InternalError,
    traits::CanisterKind,
};
use icydb_schema::SchemaSubmissionKey;

#[cfg(not(test))]
use ic_memory::open_default_memory_manager_memory;

pub(in crate::db) const MAX_STARTUP_FAILURE_RECEIPT_BYTES: usize = 2_048;
const RECEIPT_MAGIC: &[u8; 8] = b"ICYSUP01";
const RECEIPT_VERSION: u8 = 2;
const RECEIPT_HEADER_BYTES: usize = 15;
const WASM_PAGE_BYTES: u64 = 65_536;
const MAX_BINDING_KEY_BYTES: usize = 128;
const FAILURE_IDENTITY_BYTES: usize = 1 + 2 + 1 + 1;
const MAX_DIAGNOSTIC_FACT_BYTES: usize = MAX_PUBLIC_DIAGNOSTIC_FACTS * (1 + 8);
const MAX_JOURNAL_BINDING_BYTES: usize = 1 + 16 + 1 + 1 + MAX_BINDING_KEY_BYTES + 40;
pub(in crate::db) const MAX_ENCODED_STARTUP_FAILURE_RECEIPT_BYTES: usize = RECEIPT_HEADER_BYTES
    + FAILURE_IDENTITY_BYTES
    + MAX_DIAGNOSTIC_FACT_BYTES
    + MAX_JOURNAL_BINDING_BYTES;
const _: () =
    assert!(MAX_ENCODED_STARTUP_FAILURE_RECEIPT_BYTES <= MAX_STARTUP_FAILURE_RECEIPT_BYTES);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum StartupFailureBinding {
    DatabaseControl {
        commit_memory_id: u8,
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
    pub(in crate::db) memory_id: u8,
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

fn write_cell<M: Memory>(memory: &M, encoded: &[u8]) -> Result<(), InternalError> {
    if encoded.len() > MAX_STARTUP_FAILURE_RECEIPT_BYTES {
        return Err(InternalError::store_invariant());
    }
    if memory.size() == 0 && memory.grow(1) < 0 {
        return Err(InternalError::recovery_database_format_control_unavailable());
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
    let mut reader = Reader::new(payload);
    let kind = decode_kind(reader.u8()?)?;
    let code =
        ErrorCode::known(reader.u16()?).ok_or_else(InternalError::startup_control_corruption)?;
    let origin = ErrorOrigin::from_known_wire_code(reader.u8()?)
        .ok_or_else(InternalError::startup_control_corruption)?;
    let fact_count = usize::from(reader.u8()?);
    if fact_count > MAX_PUBLIC_DIAGNOSTIC_FACTS {
        return Err(InternalError::startup_control_corruption());
    }
    let mut facts = Vec::with_capacity(fact_count);
    let mut raw_facts = Vec::with_capacity(fact_count);
    for _ in 0..fact_count {
        let raw_tag = reader.u8()?;
        let tag = DiagnosticFactTag::known(raw_tag)
            .ok_or_else(InternalError::startup_control_corruption)?;
        let value = reader.u64()?;
        facts.push((tag, value));
        raw_facts.push((raw_tag, value));
    }
    validate_raw_diagnostic_fact_schema(code, raw_facts.as_slice())
        .map_err(|_| InternalError::startup_control_corruption())?;
    let binding = decode_binding(&mut reader)?;
    reader.finish()?;
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
    if !super::terminal_code_for_kind(
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
            out.push(*commit_memory_id);
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
            out.push(allocation.memory_id);
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

fn decode_binding(reader: &mut Reader<'_>) -> Result<StartupFailureBinding, InternalError> {
    match reader.u8()? {
        1 => {
            let commit_memory_id = reader.u8()?;
            let commit_stable_key = reader.string()?;
            let control = match reader.u8()? {
                0 => None,
                1 => Some(DatabaseControlBinding::new(
                    DatabaseIncarnationId::try_from_bytes(reader.array()?)
                        .map_err(|_| InternalError::startup_control_corruption())?,
                    reader.array()?,
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
            let incarnation = DatabaseIncarnationId::try_from_bytes(reader.array()?)
                .map_err(|_| InternalError::startup_control_corruption())?;
            let allocation = StoreAllocationIdentityOwned {
                memory_id: reader.u8()?,
                stable_key: reader.string()?,
            };
            let proof = JournalTailProofIdentity::from_persisted_parts(
                reader.u64()?,
                reader.u64()?,
                reader.u64()?,
                reader.u64()?,
                reader.u64()?,
            );
            Ok(StartupFailureBinding::JournalRecovery {
                incarnation,
                allocation,
                proof,
            })
        }
        3 => {
            let incarnation = DatabaseIncarnationId::try_from_bytes(reader.array()?)
                .map_err(|_| InternalError::startup_control_corruption())?;
            let submission_key = reader.string()?;
            let accepted_head = match reader.u8()? {
                0 => AcceptedHeadBinding::Empty,
                1 => AcceptedHeadBinding::Exact {
                    revision: reader.u64()?,
                    fingerprint: reader.array()?,
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

struct Reader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], InternalError> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(InternalError::startup_control_corruption)?;
        let bytes = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(InternalError::startup_control_corruption)?;
        self.cursor = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, InternalError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, InternalError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, InternalError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], InternalError> {
        self.take(N)?
            .try_into()
            .map_err(|_| InternalError::startup_control_corruption())
    }

    fn string(&mut self) -> Result<String, InternalError> {
        let len = usize::from(self.u8()?);
        if len == 0 || len > MAX_BINDING_KEY_BYTES {
            return Err(InternalError::startup_control_corruption());
        }
        std::str::from_utf8(self.take(len)?)
            .map(str::to_string)
            .map_err(|_| InternalError::startup_control_corruption())
    }

    fn finish(self) -> Result<(), InternalError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(InternalError::startup_control_corruption())
        }
    }
}

#[cfg(test)]
thread_local! {
    static TEST_STARTUP_MEMORIES: RefCell<
        Vec<(u8, &'static str, VirtualMemory<DefaultMemoryImpl>)>
    > = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(in crate::db) fn startup_memory<C: CanisterKind>()
-> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    TEST_STARTUP_MEMORIES.with(|memories| {
        let mut memories = memories.borrow_mut();
        if let Some((_, _, memory)) = memories.iter().find(|(memory_id, stable_key, _)| {
            *memory_id == C::STARTUP_MEMORY_ID && *stable_key == C::STARTUP_STABLE_KEY
        }) {
            return Ok(memory.clone());
        }
        let memory = crate::testing::test_memory(C::STARTUP_MEMORY_ID);
        memories.push((C::STARTUP_MEMORY_ID, C::STARTUP_STABLE_KEY, memory.clone()));
        Ok(memory)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::Path;
    use ic_stable_structures::VectorMemory;

    struct ReceiptCanister;

    impl Path for ReceiptCanister {
        const PATH: &'static str = "startup_receipt_tests::ReceiptCanister";
    }

    impl CanisterKind for ReceiptCanister {
        const COMMIT_MEMORY_ID: u8 = 236;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.startup_receipt.commit.v1";
        const STARTUP_MEMORY_ID: u8 = 237;
        const STARTUP_STABLE_KEY: &'static str = "icydb.test.startup_receipt.startup.control.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 238;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.startup_receipt.integrity.progress.v1";
    }

    fn failure(kind: StartupFailureKind) -> StartupFailure {
        StartupFailure::new(
            kind,
            ErrorCode::RUNTIME_CORRUPTION.diagnostic(ErrorOrigin::Recovery),
            Vec::new(),
        )
    }

    fn incarnation() -> DatabaseIncarnationId {
        DatabaseIncarnationId::for_tests(0x35)
    }

    fn round_trip(receipt: &StartupFailureReceipt) {
        let encoded = encode_receipt(receipt).expect("valid receipt should encode");
        assert!(encoded.len() <= MAX_STARTUP_FAILURE_RECEIPT_BYTES);
        let memory = VectorMemory::default();
        assert_eq!(memory.grow(1), 0);
        memory.write(0, &encoded);
        assert_eq!(
            decode_cell(&memory).expect("valid receipt should decode"),
            Some(receipt.clone())
        );
    }

    #[test]
    fn every_closed_binding_round_trips_in_the_current_envelope() {
        assert_eq!(MAX_ENCODED_STARTUP_FAILURE_RECEIPT_BYTES, 927);
        let database = StartupFailureReceipt::new(
            failure(StartupFailureKind::DatabaseControl),
            StartupFailureBinding::DatabaseControl {
                commit_memory_id: 236,
                commit_stable_key: ReceiptCanister::COMMIT_STABLE_KEY.to_string(),
                control: Some(DatabaseControlBinding::new(incarnation(), [7; 32])),
            },
        )
        .expect("database binding should admit");
        round_trip(&database);

        let journal = StartupFailureReceipt::new(
            failure(StartupFailureKind::JournalRecovery),
            StartupFailureBinding::JournalRecovery {
                incarnation: incarnation(),
                allocation: StoreAllocationIdentityOwned::from_identity(
                    StoreAllocationIdentity::new(239, "icydb.test.rows.journal.v1"),
                ),
                proof: JournalTailProofIdentity::from_persisted_parts(1, 0, 0, 1, 2),
            },
        )
        .expect("journal binding should admit");
        round_trip(&journal);

        let schema = StartupFailureReceipt::new(
            failure(StartupFailureKind::SchemaReconciliation),
            StartupFailureBinding::SchemaReconciliation {
                incarnation: incarnation(),
                submission_key: "generated/0123456789abcdef".to_string(),
                accepted_head: AcceptedHeadBinding::Exact {
                    revision: 3,
                    fingerprint: [9; 32],
                },
            },
        )
        .expect("schema binding should admit");
        round_trip(&schema);
    }

    #[test]
    fn malformed_future_and_max_plus_one_receipts_fail_closed() {
        let memory = VectorMemory::default();
        assert_eq!(memory.grow(1), 0);
        let mut future = [0_u8; MAX_STARTUP_FAILURE_RECEIPT_BYTES];
        future[..8].copy_from_slice(RECEIPT_MAGIC);
        future[8] = RECEIPT_VERSION + 1;
        memory.write(0, &future);
        let future = decode_cell(&memory).expect_err("future version must reject");
        assert_eq!(
            future.class(),
            crate::error::ErrorClass::IncompatiblePersistedFormat
        );

        let mut oversized = [0_u8; MAX_STARTUP_FAILURE_RECEIPT_BYTES];
        oversized[..8].copy_from_slice(RECEIPT_MAGIC);
        oversized[8] = RECEIPT_VERSION;
        let max_plus_one_payload =
            u16::try_from(MAX_STARTUP_FAILURE_RECEIPT_BYTES - RECEIPT_HEADER_BYTES + 1)
                .expect("test maximum should fit the persisted length field");
        oversized[9..11].copy_from_slice(&max_plus_one_payload.to_le_bytes());
        memory.write(0, &oversized);
        assert_eq!(
            decode_cell(&memory)
                .expect_err("maximum plus one envelope must reject")
                .class(),
            crate::error::ErrorClass::Corruption,
        );

        let mut max_plus_one = vec![
            encode_kind(StartupFailureKind::DatabaseControl),
            (ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING.raw() & 0xff) as u8,
            (ErrorCode::RUNTIME_BOUNDARY_DATABASE_STARTUP_RECOVERY_PENDING.raw() >> 8) as u8,
            ErrorOrigin::Recovery.wire_code(),
            u8::try_from(MAX_PUBLIC_DIAGNOSTIC_FACTS + 1)
                .expect("maximum plus one fact count should fit the persisted byte"),
        ];
        max_plus_one.resize(
            max_plus_one.len() + (MAX_PUBLIC_DIAGNOSTIC_FACTS + 1) * 9,
            0,
        );
        assert!(decode_payload(&max_plus_one).is_err());

        let mismatch = StartupFailureReceipt::new(
            failure(StartupFailureKind::JournalRecovery),
            StartupFailureBinding::DatabaseControl {
                commit_memory_id: 236,
                commit_stable_key: ReceiptCanister::COMMIT_STABLE_KEY.to_string(),
                control: None,
            },
        );
        assert!(mismatch.is_err());

        let malformed_allocation = StartupFailureReceipt::new(
            failure(StartupFailureKind::DatabaseControl),
            StartupFailureBinding::DatabaseControl {
                commit_memory_id: 236,
                commit_stable_key: "icydb.test.not-canonical.v1".to_string(),
                control: None,
            },
        );
        assert!(malformed_allocation.is_err());
    }

    #[test]
    fn publication_is_idempotent_and_clear_restores_all_zero_absence() {
        let _ = clear::<ReceiptCanister>();
        let receipt = StartupFailureReceipt::new(
            failure(StartupFailureKind::DatabaseControl),
            StartupFailureBinding::DatabaseControl {
                commit_memory_id: ReceiptCanister::COMMIT_MEMORY_ID,
                commit_stable_key: ReceiptCanister::COMMIT_STABLE_KEY.to_string(),
                control: None,
            },
        )
        .expect("receipt should admit");
        assert!(publish::<ReceiptCanister>(&receipt).expect("first publish should succeed"));
        assert!(!publish::<ReceiptCanister>(&receipt).expect("replay should succeed"));
        assert_eq!(
            load::<ReceiptCanister>().expect("receipt should load"),
            Some(receipt)
        );
        let replacement = StartupFailureReceipt::new(
            failure(StartupFailureKind::DatabaseControl),
            StartupFailureBinding::DatabaseControl {
                commit_memory_id: ReceiptCanister::COMMIT_MEMORY_ID,
                commit_stable_key: ReceiptCanister::COMMIT_STABLE_KEY.to_string(),
                control: Some(DatabaseControlBinding::new(incarnation(), [0xa5; 32])),
            },
        )
        .expect("replacement receipt should admit");
        assert!(publish::<ReceiptCanister>(&replacement).expect("changed binding should replace"));
        assert_eq!(
            load::<ReceiptCanister>().expect("replacement should load"),
            Some(replacement),
        );
        assert!(clear::<ReceiptCanister>().expect("receipt should clear"));
        assert_eq!(
            load::<ReceiptCanister>().expect("absence should load"),
            None
        );
    }
}

#[cfg(not(test))]
pub(in crate::db) fn startup_memory<C: CanisterKind>()
-> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    open_default_memory_manager_memory(C::STARTUP_STABLE_KEY, C::STARTUP_MEMORY_ID)
        .map_err(InternalError::database_format_memory_registration_failed)
}
