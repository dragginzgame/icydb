//! Module: db::session::sql::resumable_update
//! Responsibility: durable mutation-job preparation, eligibility proof,
//! private continuation encoding, bounded Forward execution, and bounded Verify scanning.
//! Does not own: application authorization, operation identity, or durable custody.
//! Boundary: accepted SQL update plan plus accepted schema -> canonical intent
//! and private engine checkpoint; each advance performs one bounded engine step.

use crate::{
    db::{
        DbSession, MutationJobAdvanceReceipt, MutationJobAdvanceRequest, MutationJobError,
        MutationJobId, MutationJobPhase, MutationJobStatus, QueryError,
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_u64,
        },
        commit::database_incarnation_id,
        data::{
            AcceptedFixedUpdatePatch, DecodedDataStoreKey, RawDataStoreKey, StoreVisit,
            StructuralRowContract, StructuralSlotReader,
        },
        database_format::crc32c,
        executor::eval_compiled_filter_expr_with_required_slot_reader,
        integrity::{MutationProgressRecordOp, replace_mutation_progress_record_op},
        journal::JournalTailStore,
        key_taxonomy::RawDataStoreKeyRange,
        mutation_job::{CanonicalMutationIntent, MutationJobRecord, MutationJobTransition},
        query::{
            plan::expr::{
                CompiledExpr, Expr, collect_scalar_expr_field_roots,
                compile_scalar_projection_expr_with_schema,
            },
            resumable_update_scope_fingerprint,
        },
        registry::{StoreAllocationIdentity, StoreHandle, StoreRuntimeStorageMode},
        schema::{
            AcceptedFieldDependencyError, AcceptedRowLayoutRuntimeContract, PersistedSchemaSnapshot,
        },
        session::sql::{
            SqlResumableUpdatePolicyReport, SqlUpdatePolicyRejection,
            classify_sql_resumable_update_policy, with_accepted_sql_update_policy_context,
        },
        session::{
            AcceptedSchemaCatalogContext, AcceptedStructuralMutation,
            AcceptedStructuralMutationTarget,
        },
        write_context::MutationMode,
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, record},
    traits::CanisterKind,
    types::{CurrentTimestamp, EntityTag, Timestamp, Ulid},
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;
use sha2::Digest;
use std::{collections::BTreeSet, ops::Bound};

const RESUMABLE_UPDATE_CONTINUATION_MAGIC: &[u8; 4] = b"ICYU";
const RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION: u8 = 1;
const RESUMABLE_UPDATE_PHASE_FORWARD: u8 = 1;
const RESUMABLE_UPDATE_PHASE_VERIFY: u8 = 2;
const RESUMABLE_UPDATE_TARGET_IDENTITY_DOMAIN: &[u8] = b"icydb.resumable-update-target.v1";
const MUTATION_JOB_OPERATION_ID_DOMAIN: &[u8] = b"icydb.mutation-job.operation-id.v1";
macro_rules! resumable_policy_bound {
    ($runtime:ident, $identity:ident, $value:expr) => {
        const $runtime: usize = $value;
        const $identity: u32 = $value;
    };
}

resumable_policy_bound!(
    MAX_RESUMABLE_UPDATE_CONTINUATION_BYTES,
    RESUMABLE_UPDATE_CONTINUATION_BYTES_POLICY,
    2 * 1024
);
resumable_policy_bound!(
    MAX_RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED,
    RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED_POLICY,
    256
);
resumable_policy_bound!(
    MAX_RESUMABLE_UPDATE_FORWARD_ROWS,
    RESUMABLE_UPDATE_FORWARD_ROWS_POLICY,
    64
);
// Bump the owning component whenever its semantics change. Numeric bounds and
// the continuation format participate directly, so their drift changes the
// identity without a separate manual edit.
const RESUMABLE_UPDATE_PACKING_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_CHECKPOINT_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_NEEDS_PATCH_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_REVISION_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_MARKER_ACCOUNTING_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_OPERATION_TIMESTAMP_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_BATCH_POLICY_INPUTS: [u32; 11] = [
    u32::from_be_bytes(*RESUMABLE_UPDATE_CONTINUATION_MAGIC),
    RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION as u32,
    RESUMABLE_UPDATE_CONTINUATION_BYTES_POLICY,
    RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED_POLICY,
    RESUMABLE_UPDATE_FORWARD_ROWS_POLICY,
    RESUMABLE_UPDATE_PACKING_POLICY_VERSION,
    RESUMABLE_UPDATE_CHECKPOINT_POLICY_VERSION,
    RESUMABLE_UPDATE_NEEDS_PATCH_POLICY_VERSION,
    RESUMABLE_UPDATE_REVISION_POLICY_VERSION,
    RESUMABLE_UPDATE_MARKER_ACCOUNTING_POLICY_VERSION,
    RESUMABLE_UPDATE_OPERATION_TIMESTAMP_POLICY_VERSION,
];
const RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY: u32 =
    resumable_update_batch_policy_identity(RESUMABLE_UPDATE_BATCH_POLICY_INPUTS);

const fn resumable_update_batch_policy_identity(inputs: [u32; 11]) -> u32 {
    let mut identity = 0x811c_9dc5_u32;
    let mut index = 0;
    while index < inputs.len() {
        identity ^= inputs[index];
        identity = identity.wrapping_mul(0x0100_0193);
        index += 1;
    }

    identity
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MutationJobEnginePhase {
    Forward,
    Verify,
}

impl MutationJobEnginePhase {
    const fn wire(self) -> u8 {
        match self {
            Self::Forward => RESUMABLE_UPDATE_PHASE_FORWARD,
            Self::Verify => RESUMABLE_UPDATE_PHASE_VERIFY,
        }
    }

    fn from_wire(value: u8) -> Result<Self, QueryError> {
        match value {
            RESUMABLE_UPDATE_PHASE_FORWARD => Ok(Self::Forward),
            RESUMABLE_UPDATE_PHASE_VERIFY => Ok(Self::Verify),
            _ => Err(malformed_continuation()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MutationJobEngineContinuation {
    bytes: Vec<u8>,
}

impl MutationJobEngineContinuation {
    #[must_use]
    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the current continuation constructor binds every persisted proof component explicitly"
    )]
    fn initial(
        operation_id: Ulid,
        entity_tag: u64,
        target_identity: [u8; 32],
        schema_fingerprint_method_version: u8,
        schema_fingerprint: [u8; 16],
        scope_fingerprint: [u8; 32],
        patch_fingerprint: [u8; 32],
        operation_timestamp: Timestamp,
    ) -> Result<Self, QueryError> {
        DecodedMutationJobEngineContinuation {
            operation_id,
            entity_tag,
            target_identity,
            schema_fingerprint_method_version,
            schema_fingerprint,
            scope_fingerprint,
            patch_fingerprint,
            operation_timestamp,
            phase: MutationJobEnginePhase::Forward,
            checkpoint: None,
            verify_revision: None,
            batch_policy_identity: RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY,
        }
        .encode()
    }
}

/// Decoded current continuation with all phase-dependent state kept together.
struct DecodedMutationJobEngineContinuation {
    operation_id: Ulid,
    entity_tag: u64,
    target_identity: [u8; 32],
    schema_fingerprint_method_version: u8,
    schema_fingerprint: [u8; 16],
    scope_fingerprint: [u8; 32],
    patch_fingerprint: [u8; 32],
    operation_timestamp: Timestamp,
    phase: MutationJobEnginePhase,
    checkpoint: Option<RawDataStoreKey>,
    verify_revision: Option<u64>,
    batch_policy_identity: u32,
}

impl DecodedMutationJobEngineContinuation {
    fn encode(&self) -> Result<MutationJobEngineContinuation, QueryError> {
        let checkpoint = self
            .checkpoint
            .as_ref()
            .map_or(&[][..], RawDataStoreKey::as_bytes);
        let checkpoint_len =
            u32::try_from(checkpoint.len()).map_err(|_| malformed_continuation())?;
        let mut bytes = Vec::with_capacity(160usize.saturating_add(checkpoint.len()));
        bytes.extend_from_slice(RESUMABLE_UPDATE_CONTINUATION_MAGIC);
        bytes.push(RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION);
        bytes.extend_from_slice(&self.operation_id.to_bytes());
        bytes.extend_from_slice(&self.entity_tag.to_be_bytes());
        bytes.extend_from_slice(&self.target_identity);
        bytes.push(self.schema_fingerprint_method_version);
        bytes.extend_from_slice(&self.schema_fingerprint);
        bytes.extend_from_slice(&self.scope_fingerprint);
        bytes.extend_from_slice(&self.patch_fingerprint);
        bytes.extend_from_slice(&self.operation_timestamp.as_millis().to_be_bytes());
        bytes.push(self.phase.wire());
        bytes.extend_from_slice(&checkpoint_len.to_be_bytes());
        bytes.extend_from_slice(checkpoint);
        match self.verify_revision {
            Some(revision) => {
                bytes.push(1);
                bytes.extend_from_slice(&revision.to_be_bytes());
            }
            None => bytes.push(0),
        }
        bytes.extend_from_slice(&self.batch_policy_identity.to_be_bytes());

        if bytes.len().saturating_add(size_of::<u32>()) > MAX_RESUMABLE_UPDATE_CONTINUATION_BYTES {
            return Err(malformed_continuation());
        }
        let checksum = crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_be_bytes());

        Ok(MutationJobEngineContinuation { bytes })
    }

    fn decode(bytes: &[u8]) -> Result<Self, QueryError> {
        if bytes.len() > MAX_RESUMABLE_UPDATE_CONTINUATION_BYTES
            || bytes.len() < 164
            || bytes.get(..4) != Some(RESUMABLE_UPDATE_CONTINUATION_MAGIC)
        {
            return Err(malformed_continuation());
        }
        let (payload, checksum) = bytes
            .split_at_checked(bytes.len().saturating_sub(size_of::<u32>()))
            .ok_or_else(malformed_continuation)?;
        let expected_checksum =
            u32::from_be_bytes(checksum.try_into().map_err(|_| malformed_continuation())?);
        if crc32c(payload) != expected_checksum {
            return Err(malformed_continuation());
        }

        let mut reader = ResumableTokenReader::new(payload);
        if reader.read_array::<4>()? != *RESUMABLE_UPDATE_CONTINUATION_MAGIC
            || reader.read_u8()? != RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION
        {
            return Err(malformed_continuation());
        }
        let operation_id = Ulid::from_bytes(reader.read_array()?);
        let entity_tag = reader.read_u64()?;
        let target_identity = reader.read_array()?;
        let schema_fingerprint_method_version = reader.read_u8()?;
        let schema_fingerprint = reader.read_array()?;
        let scope_fingerprint = reader.read_array()?;
        let patch_fingerprint = reader.read_array()?;
        let operation_timestamp = Timestamp::from_millis(reader.read_i64()?);
        let phase = MutationJobEnginePhase::from_wire(reader.read_u8()?)?;
        let checkpoint_bytes = reader.read_len_prefixed_bytes()?;
        let checkpoint = if checkpoint_bytes.is_empty() {
            None
        } else {
            let raw = RawDataStoreKey::from_persisted_bytes(checkpoint_bytes.to_vec());
            let decoded =
                DecodedDataStoreKey::try_from_raw(&raw).map_err(|_| malformed_continuation())?;
            if decoded.entity_tag() != EntityTag::new(entity_tag) {
                return Err(malformed_continuation());
            }
            Some(raw)
        };
        let verify_revision = match reader.read_u8()? {
            0 => None,
            1 => Some(reader.read_u64()?),
            _ => return Err(malformed_continuation()),
        };
        let batch_policy_identity = reader.read_u32()?;
        if !reader.is_exhausted()
            || (phase == MutationJobEnginePhase::Forward && verify_revision.is_some())
            || (phase == MutationJobEnginePhase::Verify && verify_revision.is_none())
        {
            return Err(malformed_continuation());
        }

        Ok(Self {
            operation_id,
            entity_tag,
            target_identity,
            schema_fingerprint_method_version,
            schema_fingerprint,
            scope_fingerprint,
            patch_fingerprint,
            operation_timestamp,
            phase,
            checkpoint,
            verify_revision,
            batch_policy_identity,
        })
    }
}

struct ResumableTokenReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ResumableTokenReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], QueryError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(malformed_continuation)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(malformed_continuation)?;
        self.offset = end;
        Ok(value)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], QueryError> {
        self.read_exact(N)?
            .try_into()
            .map_err(|_| malformed_continuation())
    }

    fn read_u8(&mut self) -> Result<u8, QueryError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, QueryError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, QueryError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_i64(&mut self) -> Result<i64, QueryError> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    fn read_len_prefixed_bytes(&mut self) -> Result<&'a [u8], QueryError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| malformed_continuation())?;
        self.read_exact(len)
    }

    const fn is_exhausted(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

/// Schema-owned eligibility proof consumed by continuation preparation.
///
/// Keeping the normalized scope and fixed patch paired prevents preparation
/// from hashing one representation while a later executor consumes another.
struct ResumableUpdateEligibility {
    scope_fingerprint: [u8; 32],
    patch_fingerprint: [u8; 32],
}

pub(in crate::db::session) struct PreparedMutationJobStart {
    pub(in crate::db::session) canonical_intent: Vec<u8>,
    pub(in crate::db::session) engine_continuation: Vec<u8>,
}

struct PreparedResumableUpdateStart {
    continuation: MutationJobEngineContinuation,
    target_identity: [u8; 32],
    target_store_path: String,
    target_entity_path: String,
    target_entity_tag: u64,
    accepted_schema_revision: u64,
    accepted_schema_fingerprint_method: u8,
    accepted_schema_fingerprint: [u8; 16],
    scope: Expr,
    fixed_patch: AcceptedFixedUpdatePatch,
    operation_timestamp: Timestamp,
}

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session) fn prepare_mutation_job_start(
        &self,
        job_id: MutationJobId,
        sql: &str,
    ) -> Result<PreparedMutationJobStart, MutationJobError> {
        let operation_timestamp = Timestamp::now();
        let prepared = self
            .prepare_resumable_update_start(
                mutation_job_operation_id(job_id),
                sql,
                operation_timestamp,
            )
            .map_err(|_| MutationJobError::IneligibleIntent)?;
        let intent = CanonicalMutationIntent::new(
            database_incarnation_id()
                .map_err(|_| MutationJobError::Internal)?
                .to_bytes(),
            prepared.target_identity,
            prepared.target_store_path,
            prepared.target_entity_path,
            prepared.target_entity_tag,
            prepared.accepted_schema_revision,
            prepared.accepted_schema_fingerprint_method,
            prepared.accepted_schema_fingerprint,
            &prepared.scope,
            &prepared.fixed_patch,
            prepared.operation_timestamp,
            RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY,
        )?;
        Ok(PreparedMutationJobStart {
            canonical_intent: intent.encode()?,
            engine_continuation: prepared.continuation.into_bytes(),
        })
    }

    /// Advance one authority-bound durable mutation job through one Forward page.
    #[expect(
        clippy::too_many_lines,
        reason = "one coordinator keeps authority validation, bounded scan, next-record construction, and atomic target/progress publication in review order"
    )]
    pub(in crate::db::session) fn advance_mutation_job_forward(
        &self,
        before: &MutationJobRecord,
        request: &MutationJobAdvanceRequest,
    ) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
        if before.state().phase != MutationJobPhase::Forward {
            return Err(MutationJobError::Internal);
        }
        let intent = CanonicalMutationIntent::decode(before.canonical_intent())?;
        validate_mutation_job_database_authority(&intent)?;
        if intent.batch_policy_identity() != RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY {
            return Err(MutationJobError::IneligibleIntent);
        }

        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(intent.target_entity_path()))
            .map_err(|_| MutationJobError::AuthorityMismatch)?;
        validate_mutation_job_catalog_authority(&intent, &catalog)?;
        let identity = catalog.identity();
        let store = self
            .db
            .recovered_store(identity.store_path())
            .map_err(|_| MutationJobError::TargetQueryFailed)?;
        if store.storage_capabilities().storage_mode() != StoreRuntimeStorageMode::Journaled {
            return Err(MutationJobError::AuthorityMismatch);
        }
        let target_identity = resumable_update_target_identity(
            &store,
            identity.store_path(),
            identity.entity_path(),
            identity.entity_tag().value(),
        )
        .map_err(|_| MutationJobError::TargetQueryFailed)?;
        if target_identity != intent.target_store_identity() {
            return Err(MutationJobError::AuthorityMismatch);
        }

        let mut continuation =
            DecodedMutationJobEngineContinuation::decode(before.engine_continuation())
                .map_err(|_| MutationJobError::CorruptProgressStore)?;
        if continuation.operation_id != mutation_job_operation_id(request.job_id)
            || continuation.operation_timestamp != intent.operation_timestamp()
            || continuation.phase != MutationJobEnginePhase::Forward
        {
            return Err(MutationJobError::CorruptProgressStore);
        }
        let scope = intent.decode_scope()?;
        let fixed_patch = intent.decode_fixed_patch()?;
        let eligibility = ResumableUpdateEligibility {
            scope_fingerprint: resumable_update_scope_fingerprint(&scope),
            patch_fingerprint: fixed_patch.fingerprint(),
        };
        validate_resumable_update_bindings(
            &continuation,
            identity.entity_tag().value(),
            target_identity,
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
            &eligibility,
        )
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
        validate_resumable_update_checkpoint(&continuation, identity.entity_tag())
            .map_err(|_| MutationJobError::CorruptProgressStore)?;

        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .map_err(|_| MutationJobError::AuthorityMismatch)?;
        let compiled_scope =
            compile_scalar_projection_expr_with_schema(catalog.accepted_schema_info(), &scope)
                .map(|expr| CompiledExpr::compile(&expr))
                .ok_or(MutationJobError::IneligibleIntent)?;
        let row_contract = StructuralRowContract::from_accepted_decode_contract(
            identity.entity_path(),
            descriptor.row_decode_contract(catalog.value_catalog_handle().clone()),
        );
        let scan = scan_resumable_update_forward(
            &store,
            continuation.checkpoint.as_ref(),
            identity.entity_tag(),
            &compiled_scope,
            &row_contract,
            &fixed_patch,
        )
        .map_err(|_| MutationJobError::TargetQueryFailed)?;
        record_resumable_rows_scanned(identity.entity_path(), scan.physical_keys_scanned);

        let patch = fixed_patch.to_update_intent();
        let candidate_rows = scan
            .candidates
            .iter()
            .map(|key| {
                AcceptedStructuralMutation::save(
                    MutationMode::Update,
                    AcceptedStructuralMutationTarget::expected(key.clone()),
                    patch.clone(),
                )
            })
            .collect::<Vec<_>>();
        continuation.checkpoint = scan.final_checkpoint;
        if scan.exhausted {
            continuation.phase = MutationJobEnginePhase::Verify;
            continuation.checkpoint = None;
            continuation.verify_revision = Some(if candidate_rows.is_empty() {
                durable_store_revision(&store).map_err(|_| MutationJobError::TargetQueryFailed)?
            } else {
                durable_store_revision_after_next_mutation(&store)?
            });
        }
        let next_continuation = continuation
            .encode()
            .map_err(|_| MutationJobError::CorruptProgressStore)?
            .into_bytes();
        let keys_scanned = u64::try_from(scan.physical_keys_scanned)
            .map_err(|_| MutationJobError::CounterOverflow)?;
        let rows_updated =
            u64::try_from(candidate_rows.len()).map_err(|_| MutationJobError::CounterOverflow)?;
        let phase = if scan.exhausted {
            MutationJobPhase::Verify
        } else {
            MutationJobPhase::Forward
        };
        let (after, receipt) = before.apply_transition(
            request,
            MutationJobTransition::new(
                MutationJobStatus::Active,
                phase,
                next_continuation,
                keys_scanned,
                rows_updated,
                0,
            ),
        )?;
        let progress_operation = MutationProgressRecordOp::replace(before, &after)?;
        if candidate_rows.is_empty() {
            replace_mutation_progress_record_op::<C>(&progress_operation)?;
        } else {
            let committed_rows = self
                .execute_accepted_structural_update_with_mutation_progress(
                    &catalog,
                    &descriptor,
                    candidate_rows,
                    intent.operation_timestamp(),
                    progress_operation,
                )
                .map_err(|_| MutationJobError::TargetMutationFailed)?;
            if u64::try_from(committed_rows).ok() != Some(rows_updated) {
                return Err(MutationJobError::TargetMutationFailed);
            }
        }
        Ok(receipt)
    }

    fn prepare_resumable_update_start(
        &self,
        operation_id: Ulid,
        sql: &str,
        operation_timestamp: Timestamp,
    ) -> Result<PreparedResumableUpdateStart, QueryError> {
        let entity_name = crate::db::session::sql::sql_statement_entity_name(sql)?
            .ok_or_else(QueryError::unsupported_query)?;
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(entity_name.as_str()))
            .map_err(QueryError::execute)?;
        let identity = catalog.identity();
        let store = self
            .db
            .recovered_store(identity.store_path())
            .map_err(QueryError::execute)?;
        if store.storage_capabilities().storage_mode() != StoreRuntimeStorageMode::Journaled {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::ResumableUpdateRequiresJournaledStore,
            ));
        }
        let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
            .map_err(QueryError::execute)?;
        let report = with_accepted_sql_update_policy_context(&descriptor, |context| {
            classify_sql_resumable_update_policy(
                sql,
                catalog.snapshot().persisted_snapshot().entity_name(),
                context,
            )
        })?;
        let plan = require_resumable_update_plan(report)?;
        let selector =
            Self::sql_update_selector_query(catalog.accepted_schema_info(), plan.statement())?;
        let patch = Self::sql_structural_patch(&descriptor, plan.statement())?;
        let fixed_patch = AcceptedFixedUpdatePatch::from_update_intent(
            identity.entity_path(),
            identity.entity_tag().value(),
            descriptor.row_decode_contract(catalog.value_catalog_handle().clone()),
            catalog.fingerprint(),
            catalog.accepted_row_constraints(),
            &patch,
        )
        .map_err(QueryError::execute)?;
        let eligibility = prove_resumable_update_eligibility(
            catalog.snapshot().persisted_snapshot(),
            &descriptor,
            &selector,
            &fixed_patch,
        )?;
        let target_identity = resumable_update_target_identity(
            &store,
            identity.store_path(),
            identity.entity_path(),
            identity.entity_tag().value(),
        )?;

        let scope = selector.scalar_filter_expr().cloned().ok_or_else(|| {
            QueryError::sql_write_boundary(SqlWriteBoundaryCode::UpdateMissingWherePredicate)
        })?;
        let continuation = MutationJobEngineContinuation::initial(
            operation_id,
            identity.entity_tag().value(),
            target_identity,
            catalog.fingerprint_method_version(),
            catalog.fingerprint(),
            eligibility.scope_fingerprint,
            eligibility.patch_fingerprint,
            operation_timestamp,
        )?;
        Ok(PreparedResumableUpdateStart {
            continuation,
            target_identity,
            target_store_path: identity.store_path().to_string(),
            target_entity_path: identity.entity_path().to_string(),
            target_entity_tag: identity.entity_tag().value(),
            accepted_schema_revision: catalog.revision().get(),
            accepted_schema_fingerprint_method: catalog.fingerprint_method_version(),
            accepted_schema_fingerprint: catalog.fingerprint(),
            scope,
            fixed_patch,
            operation_timestamp,
        })
    }
}

struct ResumableForwardScan<K> {
    candidates: Vec<K>,
    final_checkpoint: Option<RawDataStoreKey>,
    physical_keys_scanned: usize,
    exhausted: bool,
}

fn scan_resumable_update_forward(
    store: &StoreHandle,
    checkpoint: Option<&RawDataStoreKey>,
    entity_tag: EntityTag,
    compiled_scope: &CompiledExpr,
    row_contract: &StructuralRowContract,
    fixed_patch: &AcceptedFixedUpdatePatch,
) -> Result<ResumableForwardScan<DecodedDataStoreKey>, QueryError> {
    let range = RawDataStoreKeyRange::entity_prefix(entity_tag);
    let lower = checkpoint.cloned().map_or_else(
        || Bound::Included(RawDataStoreKey::store_range_lower_key(&range)),
        Bound::Excluded,
    );
    let upper = range
        .upper_exclusive()
        .map(RawDataStoreKey::from_store_range_bound)
        .map_or(Bound::Unbounded, Bound::Excluded);
    let mut candidates = Vec::with_capacity(MAX_RESUMABLE_UPDATE_FORWARD_ROWS);
    let mut final_checkpoint = checkpoint.cloned();
    let mut physical_keys_scanned = 0usize;
    let mut has_more = false;

    store
        .with_data(|data| {
            data.visit_range((lower, upper), |raw_key, raw_row| {
                if physical_keys_scanned == MAX_RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED
                    || candidates.len() == MAX_RESUMABLE_UPDATE_FORWARD_ROWS
                {
                    has_more = true;
                    return Ok(StoreVisit::Stop);
                }

                let decoded_key = DecodedDataStoreKey::try_from_raw(raw_key)
                    .map_err(|_| InternalError::identity_corruption())?;
                if decoded_key.entity_tag() != entity_tag {
                    return Err(InternalError::identity_corruption());
                }
                let row = StructuralSlotReader::from_raw_row_with_validated_contract(
                    raw_row,
                    row_contract.clone(),
                )?;
                if resumable_row_needs_patch(compiled_scope, fixed_patch, &row)? {
                    candidates.push(decoded_key);
                }
                physical_keys_scanned = physical_keys_scanned.saturating_add(1);
                final_checkpoint = Some(raw_key.clone());

                Ok(StoreVisit::Continue)
            })
        })
        .map_err(QueryError::execute)?;

    Ok(ResumableForwardScan {
        candidates,
        final_checkpoint,
        physical_keys_scanned,
        exhausted: !has_more,
    })
}

#[expect(
    dead_code,
    reason = "Patch 5 retains the bounded private Verify result for the Patch 6 durable coordinator"
)]
struct ResumableVerifyScan {
    final_checkpoint: Option<RawDataStoreKey>,
    keys_scanned: usize,
    exhausted: bool,
    residual_work: bool,
}

#[expect(
    dead_code,
    reason = "Patch 5 retains the bounded private Verify scan for the Patch 6 durable coordinator"
)]
fn scan_mutation_job_verify(
    store: &StoreHandle,
    checkpoint: Option<&RawDataStoreKey>,
    entity_tag: EntityTag,
    compiled_scope: &CompiledExpr,
    row_contract: &StructuralRowContract,
    fixed_patch: &AcceptedFixedUpdatePatch,
) -> Result<ResumableVerifyScan, QueryError> {
    let range = RawDataStoreKeyRange::entity_prefix(entity_tag);
    let lower = checkpoint.cloned().map_or_else(
        || Bound::Included(RawDataStoreKey::store_range_lower_key(&range)),
        Bound::Excluded,
    );
    let upper = range
        .upper_exclusive()
        .map(RawDataStoreKey::from_store_range_bound)
        .map_or(Bound::Unbounded, Bound::Excluded);
    let mut final_checkpoint = checkpoint.cloned();
    let mut keys_scanned = 0usize;
    let mut has_more = false;
    let mut residual_work = false;

    store
        .with_data(|data| {
            data.visit_range((lower, upper), |raw_key, raw_row| {
                if keys_scanned == MAX_RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED {
                    has_more = true;
                    return Ok(StoreVisit::Stop);
                }

                let decoded_key = DecodedDataStoreKey::try_from_raw(raw_key)
                    .map_err(|_| InternalError::identity_corruption())?;
                if decoded_key.entity_tag() != entity_tag {
                    return Err(InternalError::identity_corruption());
                }
                let row = StructuralSlotReader::from_raw_row_with_validated_contract(
                    raw_row,
                    row_contract.clone(),
                )?;
                keys_scanned = keys_scanned.saturating_add(1);
                if resumable_row_needs_patch(compiled_scope, fixed_patch, &row)? {
                    residual_work = true;
                    return Ok(StoreVisit::Stop);
                }
                final_checkpoint = Some(raw_key.clone());

                Ok(StoreVisit::Continue)
            })
        })
        .map_err(QueryError::execute)?;

    Ok(ResumableVerifyScan {
        final_checkpoint,
        keys_scanned,
        exhausted: !has_more,
        residual_work,
    })
}

fn resumable_row_needs_patch(
    compiled_scope: &CompiledExpr,
    fixed_patch: &AcceptedFixedUpdatePatch,
    row: &StructuralSlotReader,
) -> Result<bool, InternalError> {
    Ok(
        eval_compiled_filter_expr_with_required_slot_reader(compiled_scope, row)?
            && !fixed_patch.is_satisfied_by(row)?,
    )
}

fn record_resumable_rows_scanned(entity_path: &str, keys_scanned: usize) {
    record(MetricsEvent::RowsScanned {
        entity_path: entity_path.into(),
        rows_scanned: u64::try_from(keys_scanned).unwrap_or(u64::MAX),
    });
}

fn durable_store_revision(store: &StoreHandle) -> Result<u64, QueryError> {
    let journal = store
        .journal_tail_store()
        .ok_or_else(QueryError::invariant)?;
    journal
        .with_borrow(JournalTailStore::data_mutation_revision)
        .map_err(QueryError::execute)
}

fn durable_store_revision_after_next_mutation(
    store: &StoreHandle,
) -> Result<u64, MutationJobError> {
    let journal = store
        .journal_tail_store()
        .ok_or(MutationJobError::TargetMutationFailed)?;
    journal
        .with_borrow(|tail| {
            tail.next_mutation_append_sequence()?
                .next()
                .map(crate::db::journal::JournalSequence::get)
                .ok_or_else(InternalError::journal_mutation_revision_exhausted)
        })
        .map_err(|_| MutationJobError::TargetMutationFailed)
}

fn validate_mutation_job_database_authority(
    intent: &CanonicalMutationIntent,
) -> Result<(), MutationJobError> {
    let current = database_incarnation_id()
        .map_err(|_| MutationJobError::Internal)?
        .to_bytes();
    if current != intent.database_incarnation() {
        return Err(MutationJobError::AuthorityMismatch);
    }
    Ok(())
}

fn validate_mutation_job_catalog_authority(
    intent: &CanonicalMutationIntent,
    catalog: &AcceptedSchemaCatalogContext,
) -> Result<(), MutationJobError> {
    let identity = catalog.identity();
    if identity.store_path() != intent.target_store_path()
        || identity.entity_path() != intent.target_entity_path()
        || identity.entity_tag().value() != intent.target_entity_tag()
        || catalog.revision().get() != intent.accepted_schema_revision()
        || catalog.fingerprint_method_version() != intent.accepted_schema_fingerprint_method()
        || catalog.fingerprint() != intent.accepted_schema_fingerprint()
    {
        return Err(MutationJobError::AuthorityMismatch);
    }
    Ok(())
}

fn validate_resumable_update_bindings(
    continuation: &DecodedMutationJobEngineContinuation,
    entity_tag: u64,
    target_identity: [u8; 32],
    schema_fingerprint_method_version: u8,
    schema_fingerprint: [u8; 16],
    eligibility: &ResumableUpdateEligibility,
) -> Result<(), QueryError> {
    if continuation.entity_tag != entity_tag || continuation.target_identity != target_identity {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ResumableUpdateContinuationTargetMismatch,
        ));
    }
    if continuation.schema_fingerprint_method_version != schema_fingerprint_method_version
        || continuation.schema_fingerprint != schema_fingerprint
    {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ResumableUpdateContinuationSchemaMismatch,
        ));
    }
    if continuation.scope_fingerprint != eligibility.scope_fingerprint {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ResumableUpdateContinuationScopeMismatch,
        ));
    }
    if continuation.patch_fingerprint != eligibility.patch_fingerprint {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ResumableUpdateContinuationPatchMismatch,
        ));
    }
    if continuation.batch_policy_identity != RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ResumableUpdateContinuationBatchPolicyMismatch,
        ));
    }

    Ok(())
}

fn validate_resumable_update_checkpoint(
    continuation: &DecodedMutationJobEngineContinuation,
    entity_tag: EntityTag,
) -> Result<(), QueryError> {
    let Some(checkpoint) = continuation.checkpoint.as_ref() else {
        return Ok(());
    };
    let decoded =
        DecodedDataStoreKey::try_from_raw(checkpoint).map_err(|_| malformed_continuation())?;
    if decoded.entity_tag() != entity_tag {
        return Err(malformed_continuation());
    }

    Ok(())
}

fn malformed_continuation() -> QueryError {
    QueryError::sql_write_boundary(SqlWriteBoundaryCode::ResumableUpdateContinuationMalformed)
}

fn require_resumable_update_plan(
    report: SqlResumableUpdatePolicyReport,
) -> Result<crate::db::session::sql::SqlTrustedResumableUpdatePlan, QueryError> {
    let rejection = match report {
        Ok(plan) => return Ok(plan),
        Err(rejection) => rejection,
    };

    let boundary = match rejection {
        SqlUpdatePolicyRejection::MissingWhere => SqlWriteBoundaryCode::UpdateMissingWherePredicate,
        SqlUpdatePolicyRejection::PrimaryKeyMutation => {
            SqlWriteBoundaryCode::UpdatePrimaryKeyMutation
        }
        SqlUpdatePolicyRejection::GeneratedFieldMutation => {
            SqlWriteBoundaryCode::ExplicitGeneratedField
        }
        SqlUpdatePolicyRejection::ManagedFieldMutation => {
            SqlWriteBoundaryCode::ExplicitManagedField
        }
        SqlUpdatePolicyRejection::ResumableWindowUnsupported => {
            SqlWriteBoundaryCode::ResumableUpdateWindowUnsupported
        }
        SqlUpdatePolicyRejection::ResumableReturningUnsupported => {
            SqlWriteBoundaryCode::ResumableUpdateReturningUnsupported
        }
        SqlUpdatePolicyRejection::NotUpdate
        | SqlUpdatePolicyRejection::PrimaryKeyProofFailed
        | SqlUpdatePolicyRejection::MissingCanonicalPrimaryKeyOrder
        | SqlUpdatePolicyRejection::DescendingOrder
        | SqlUpdatePolicyRejection::MissingLimit
        | SqlUpdatePolicyRejection::OffsetUnsupported
        | SqlUpdatePolicyRejection::LimitTooHigh
        | SqlUpdatePolicyRejection::ExactWindowUnsupported => {
            return Err(QueryError::unsupported_query());
        }
    };

    Err(QueryError::sql_write_boundary(boundary))
}

fn prove_resumable_update_eligibility(
    snapshot: &PersistedSchemaSnapshot,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    selector: &crate::db::query::intent::StructuralQuery,
    patch: &AcceptedFixedUpdatePatch,
) -> Result<ResumableUpdateEligibility, QueryError> {
    if snapshot.update_management_requires_global_write_validation() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ResumableUpdateManagedFieldHasGlobalConstraint,
        ));
    }
    let scope = selector.scalar_filter_expr().ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::UpdateMissingWherePredicate)
    })?;
    let mut scope_roots = BTreeSet::new();
    if !collect_scalar_expr_field_roots(scope, &mut scope_roots) {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ResumableUpdateScopeDependencyUnknown,
        ));
    }
    let scope_dependencies = snapshot
        .accepted_field_dependency_closure(scope_roots.iter().map(String::as_str))
        .map_err(|error| match error {
            AcceptedFieldDependencyError::UnknownField => QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::ResumableUpdateScopeDependencyUnknown,
            ),
        })?;

    for target in patch.fields() {
        let field = descriptor
            .field_for_slot_index(target.slot().index())
            .ok_or_else(QueryError::invariant)?;
        if scope_dependencies.contains(&field.field_id()) {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::ResumableUpdateScopeDependsOnAssignedField,
            ));
        }
        if snapshot.field_requires_global_write_validation(field.field_id(), field.name()) {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::ResumableUpdateAssignedFieldHasGlobalConstraint,
            ));
        }
    }

    Ok(ResumableUpdateEligibility {
        scope_fingerprint: resumable_update_scope_fingerprint(scope),
        patch_fingerprint: patch.fingerprint(),
    })
}

fn resumable_update_target_identity(
    store: &StoreHandle,
    store_path: &str,
    entity_path: &str,
    entity_tag: u64,
) -> Result<[u8; 32], QueryError> {
    let allocations = store.allocation_identities();
    let identities = [
        allocations.data(),
        allocations.index(),
        allocations.schema(),
        allocations.journal(),
    ];
    let mut hasher = new_hash_sha256_prefixed(RESUMABLE_UPDATE_TARGET_IDENTITY_DOMAIN);
    write_hash_str_u32(&mut hasher, "store_path");
    write_hash_str_u32(&mut hasher, store_path);
    write_hash_str_u32(&mut hasher, "entity_path");
    write_hash_str_u32(&mut hasher, entity_path);
    write_hash_u64(&mut hasher, entity_tag);
    for identity in identities {
        let identity = identity.ok_or_else(QueryError::invariant)?;
        hash_store_allocation_identity(&mut hasher, identity);
    }

    Ok(finalize_hash_sha256(hasher))
}

fn mutation_job_operation_id(job_id: MutationJobId) -> Ulid {
    let mut hasher = new_hash_sha256_prefixed(MUTATION_JOB_OPERATION_ID_DOMAIN);
    hasher.update(job_id.to_bytes());
    let digest = finalize_hash_sha256(hasher);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    Ulid::from_bytes(bytes)
}

fn hash_store_allocation_identity(hasher: &mut sha2::Sha256, identity: StoreAllocationIdentity) {
    hasher.update([identity.memory_id()]);
    write_hash_str_u32(hasher, identity.stable_key());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resumable_batch_policy_identity_covers_every_compatibility_input() {
        assert_eq!(RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY, 0x81a4_6027);
        assert_ne!(RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY, 1);

        for index in 0..RESUMABLE_UPDATE_BATCH_POLICY_INPUTS.len() {
            let mut changed = RESUMABLE_UPDATE_BATCH_POLICY_INPUTS;
            changed[index] = changed[index].wrapping_add(1);
            assert_ne!(
                resumable_update_batch_policy_identity(changed),
                RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY,
                "compatibility input {index} must participate in the batch-policy identity",
            );
        }
    }
}
