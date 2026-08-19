//! Module: db::session::sql::resumable_update
//! Responsibility: durable mutation-job preparation, eligibility proof,
//! private continuation encoding, bounded Forward execution, and bounded Verify scanning.
//! Does not own: application authorization, operation identity, or durable custody.
//! Boundary: accepted SQL update plan plus accepted schema -> canonical intent
//! and private engine checkpoint; each advance performs one bounded engine step.

use crate::{
    db::{
        DbSession, MutationJobAdvanceReceipt, MutationJobAdvanceRequest, MutationJobError,
        MutationJobId, MutationJobPhase, MutationJobRestartReason, MutationJobStatus, QueryError,
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_u64,
        },
        commit::database_incarnation_id,
        data::{
            AcceptedFixedUpdatePatch, DecodedDataStoreKey, RawDataStoreKey, StoreVisit,
            StructuralRowContract, StructuralSlotReader, managed_timestamp_progression_regresses,
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
            write::{
                AcceptedLoadedStructuralRow, AcceptedStructuralMutationCommitDirective,
                AcceptedStructuralMutationPackingReport,
                STRUCTURAL_MUTATION_BATCH_STAGED_BYTES_POLICY,
            },
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
use std::{cell::RefCell, collections::BTreeSet, ops::Bound};

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
    208
);
resumable_policy_bound!(
    MAX_RESUMABLE_UPDATE_FORWARD_ROWS,
    RESUMABLE_UPDATE_FORWARD_ROWS_POLICY,
    56
);
resumable_policy_bound!(
    MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES,
    RESUMABLE_UPDATE_FORWARD_SCAN_BYTES_POLICY,
    16 * 1024 * 1024
);
resumable_policy_bound!(
    MAX_RESUMABLE_UPDATE_VERIFY_KEYS_SCANNED,
    RESUMABLE_UPDATE_VERIFY_KEYS_SCANNED_POLICY,
    208
);
resumable_policy_bound!(
    MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
    RESUMABLE_UPDATE_VERIFY_SCAN_BYTES_POLICY,
    16 * 1024 * 1024
);
const RESUMABLE_UPDATE_FORWARD_STAGED_BYTES_POLICY: u32 =
    STRUCTURAL_MUTATION_BATCH_STAGED_BYTES_POLICY;
// Bump the owning component whenever its semantics change. Numeric bounds and
// the continuation format participate directly, so their drift changes the
// identity without a separate manual edit.
const RESUMABLE_UPDATE_PACKING_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_CHECKPOINT_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_NEEDS_PATCH_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_REVISION_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_MARKER_ACCOUNTING_POLICY_VERSION: u32 = 1;
const RESUMABLE_UPDATE_MANAGED_WRITE_TIME_POLICY_REVISION: u32 = 2;
const RESUMABLE_UPDATE_BATCH_POLICY_INPUTS: [u32; 15] = [
    u32::from_be_bytes(*RESUMABLE_UPDATE_CONTINUATION_MAGIC),
    RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION as u32,
    RESUMABLE_UPDATE_CONTINUATION_BYTES_POLICY,
    RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED_POLICY,
    RESUMABLE_UPDATE_FORWARD_ROWS_POLICY,
    RESUMABLE_UPDATE_FORWARD_SCAN_BYTES_POLICY,
    RESUMABLE_UPDATE_FORWARD_STAGED_BYTES_POLICY,
    RESUMABLE_UPDATE_VERIFY_KEYS_SCANNED_POLICY,
    RESUMABLE_UPDATE_VERIFY_SCAN_BYTES_POLICY,
    RESUMABLE_UPDATE_PACKING_POLICY_VERSION,
    RESUMABLE_UPDATE_CHECKPOINT_POLICY_VERSION,
    RESUMABLE_UPDATE_NEEDS_PATCH_POLICY_VERSION,
    RESUMABLE_UPDATE_REVISION_POLICY_VERSION,
    RESUMABLE_UPDATE_MARKER_ACCOUNTING_POLICY_VERSION,
    RESUMABLE_UPDATE_MANAGED_WRITE_TIME_POLICY_REVISION,
];
const RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY: u32 =
    resumable_update_batch_policy_identity(RESUMABLE_UPDATE_BATCH_POLICY_INPUTS);

const fn resumable_update_batch_policy_identity<const N: usize>(inputs: [u32; N]) -> u32 {
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

    fn restart_forward(&mut self) {
        self.phase = MutationJobEnginePhase::Forward;
        self.checkpoint = None;
        self.verify_revision = None;
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

struct PreparedMutationJobExecution {
    catalog: AcceptedSchemaCatalogContext,
    store: StoreHandle,
    continuation: DecodedMutationJobEngineContinuation,
    scope: Expr,
    fixed_patch: AcceptedFixedUpdatePatch,
}

struct PreparedMutationJobAuthority {
    intent: CanonicalMutationIntent,
    catalog: AcceptedSchemaCatalogContext,
    store: StoreHandle,
    target_identity: [u8; 32],
}

struct PreparedMutationJobForwardRuntime<'a> {
    descriptor: AcceptedRowLayoutRuntimeContract<'a>,
    compiled_scope: CompiledExpr,
    row_contract: StructuralRowContract,
}

struct PreparedMutationJobTraversalRuntime {
    compiled_scope: CompiledExpr,
    row_contract: StructuralRowContract,
}

enum MutationJobExecutionPreparationError {
    Restart(MutationJobRestartReason),
    Failure(MutationJobError),
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

    fn prepare_mutation_job_authority(
        &self,
        before: &MutationJobRecord,
    ) -> Result<PreparedMutationJobAuthority, MutationJobExecutionPreparationError> {
        let intent = CanonicalMutationIntent::decode(before.canonical_intent())
            .map_err(MutationJobExecutionPreparationError::Failure)?;
        let current_incarnation = database_incarnation_id()
            .map_err(|_| MutationJobExecutionPreparationError::Failure(MutationJobError::Internal))?
            .to_bytes();
        if current_incarnation != intent.database_incarnation() {
            return Err(MutationJobExecutionPreparationError::Restart(
                MutationJobRestartReason::TargetAllocationChanged,
            ));
        }
        if intent.batch_policy_identity() != RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY {
            return Err(MutationJobExecutionPreparationError::Restart(
                MutationJobRestartReason::BatchPolicyChanged,
            ));
        }

        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(intent.target_entity_path()))
            .map_err(|_| {
                MutationJobExecutionPreparationError::Restart(
                    MutationJobRestartReason::AcceptedSchemaChanged,
                )
            })?;
        if !mutation_job_catalog_authority_matches(&intent, &catalog) {
            return Err(MutationJobExecutionPreparationError::Restart(
                MutationJobRestartReason::AcceptedSchemaChanged,
            ));
        }
        let identity = catalog.identity();
        let store = self
            .db
            .recovered_store(identity.store_path())
            .map_err(|_| {
                MutationJobExecutionPreparationError::Failure(MutationJobError::TargetQueryFailed)
            })?;
        if store.storage_capabilities().storage_mode() != StoreRuntimeStorageMode::Journaled {
            return Err(MutationJobExecutionPreparationError::Restart(
                MutationJobRestartReason::TargetAllocationChanged,
            ));
        }
        let target_identity = resumable_update_target_identity(
            &store,
            identity.store_path(),
            identity.entity_path(),
            identity.entity_tag().value(),
        )
        .map_err(|_| {
            MutationJobExecutionPreparationError::Failure(MutationJobError::TargetQueryFailed)
        })?;
        if target_identity != intent.target_store_identity() {
            return Err(MutationJobExecutionPreparationError::Restart(
                MutationJobRestartReason::TargetAllocationChanged,
            ));
        }

        Ok(PreparedMutationJobAuthority {
            intent,
            catalog,
            store,
            target_identity,
        })
    }

    fn prepare_mutation_job_execution(
        &self,
        before: &MutationJobRecord,
        request: &MutationJobAdvanceRequest,
        expected_phase: MutationJobEnginePhase,
    ) -> Result<PreparedMutationJobExecution, MutationJobExecutionPreparationError> {
        let PreparedMutationJobAuthority {
            intent,
            catalog,
            store,
            target_identity,
        } = self.prepare_mutation_job_authority(before)?;
        let identity = catalog.identity();

        let continuation = decode_retained_mutation_job_continuation(before.engine_continuation())?;
        if continuation.batch_policy_identity != RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY {
            return Err(MutationJobExecutionPreparationError::Restart(
                MutationJobRestartReason::BatchPolicyChanged,
            ));
        }
        if continuation.operation_id != mutation_job_operation_id(request.job_id)
            || continuation.operation_timestamp != intent.operation_timestamp()
            || continuation.phase != expected_phase
        {
            return Err(MutationJobExecutionPreparationError::Failure(
                MutationJobError::CorruptProgressStore,
            ));
        }
        let scope = intent
            .decode_scope()
            .map_err(MutationJobExecutionPreparationError::Failure)?;
        let fixed_patch = intent
            .decode_fixed_patch()
            .map_err(MutationJobExecutionPreparationError::Failure)?;
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
        .map_err(|_| {
            MutationJobExecutionPreparationError::Failure(MutationJobError::CorruptProgressStore)
        })?;
        validate_resumable_update_checkpoint(&continuation, identity.entity_tag()).map_err(
            |_| {
                MutationJobExecutionPreparationError::Failure(
                    MutationJobError::CorruptProgressStore,
                )
            },
        )?;

        Ok(PreparedMutationJobExecution {
            catalog,
            store,
            continuation,
            scope,
            fixed_patch,
        })
    }

    /// Advance one authority-bound durable mutation job through one Forward page.
    pub(in crate::db::session) fn advance_mutation_job_forward(
        &self,
        before: &MutationJobRecord,
        request: &MutationJobAdvanceRequest,
    ) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
        if before.state().phase != MutationJobPhase::Forward {
            return Err(MutationJobError::Internal);
        }
        let PreparedMutationJobExecution {
            catalog,
            store,
            mut continuation,
            scope,
            fixed_patch,
        } = match self.prepare_mutation_job_execution(
            before,
            request,
            MutationJobEnginePhase::Forward,
        ) {
            Ok(prepared) => prepared,
            Err(MutationJobExecutionPreparationError::Restart(reason)) => {
                return persist_terminal_mutation_job_restart::<C>(before, request, reason);
            }
            Err(MutationJobExecutionPreparationError::Failure(error)) => return Err(error),
        };
        let identity = catalog.identity();
        let PreparedMutationJobForwardRuntime {
            descriptor,
            compiled_scope,
            row_contract,
        } = match prepare_mutation_job_forward_runtime(&catalog, &scope, &fixed_patch) {
            Ok(runtime) => runtime,
            Err(MutationJobExecutionPreparationError::Restart(reason)) => {
                return persist_terminal_mutation_job_restart::<C>(before, request, reason);
            }
            Err(MutationJobExecutionPreparationError::Failure(error)) => return Err(error),
        };
        let advance_timestamp = Timestamp::now();
        let patch = fixed_patch.to_update_intent();
        let scanner = RefCell::new(ResumableForwardScanner::new(
            &store,
            continuation.checkpoint.as_ref(),
            identity.entity_tag(),
            &compiled_scope,
            &row_contract,
            &fixed_patch,
            advance_timestamp,
        ));
        let outcome = self
            .execute_accepted_structural_update_bounded_prefix(
                &catalog,
                &descriptor,
                MAX_RESUMABLE_UPDATE_FORWARD_ROWS,
                || Ok(scanner.borrow_mut().next_mutation(&patch)),
                advance_timestamp,
                |packing| {
                    let scan = scanner.borrow().finish(packing);
                    Ok(prepare_packed_forward_outcome(
                        before,
                        request,
                        &mut continuation,
                        &store,
                        scan,
                        packing,
                    ))
                },
            )
            .map_err(|_| MutationJobError::TargetMutationFailed)?;

        match outcome {
            PackedForwardOutcome::Ready {
                receipt,
                progress_only,
                physical_keys_scanned,
            } => {
                record_resumable_rows_scanned(identity.entity_path(), physical_keys_scanned);
                if let Some(operation) = progress_only {
                    replace_mutation_progress_record_op::<C>(&operation)?;
                }
                Ok(receipt)
            }
            PackedForwardOutcome::Restart(reason) => {
                persist_terminal_mutation_job_restart::<C>(before, request, reason)
            }
            PackedForwardOutcome::Failure(error) => Err(error),
        }
    }

    /// Advance one authority-bound durable mutation job through one stable Verify page.
    pub(in crate::db::session) fn advance_mutation_job_verify(
        &self,
        before: &MutationJobRecord,
        request: &MutationJobAdvanceRequest,
    ) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
        if before.state().phase != MutationJobPhase::Verify {
            return Err(MutationJobError::Internal);
        }
        let PreparedMutationJobExecution {
            catalog,
            store,
            mut continuation,
            scope,
            fixed_patch,
        } = match self.prepare_mutation_job_execution(
            before,
            request,
            MutationJobEnginePhase::Verify,
        ) {
            Ok(prepared) => prepared,
            Err(MutationJobExecutionPreparationError::Restart(reason)) => {
                return persist_terminal_mutation_job_restart::<C>(before, request, reason);
            }
            Err(MutationJobExecutionPreparationError::Failure(error)) => return Err(error),
        };
        let captured_revision = continuation
            .verify_revision
            .ok_or(MutationJobError::CorruptProgressStore)?;
        if durable_store_revision(&store).map_err(|_| MutationJobError::TargetQueryFailed)?
            != captured_revision
        {
            continuation.restart_forward();
            return persist_verify_restart::<C>(before, request, &continuation, 0);
        }

        let identity = catalog.identity();
        let PreparedMutationJobTraversalRuntime {
            compiled_scope,
            row_contract,
        } = match prepare_mutation_job_traversal_runtime(&catalog, &scope, &fixed_patch) {
            Ok(runtime) => runtime,
            Err(MutationJobExecutionPreparationError::Restart(reason)) => {
                return persist_terminal_mutation_job_restart::<C>(before, request, reason);
            }
            Err(MutationJobExecutionPreparationError::Failure(error)) => return Err(error),
        };
        let scan = scan_mutation_job_verify(
            &store,
            continuation.checkpoint.as_ref(),
            identity.entity_tag(),
            &compiled_scope,
            &row_contract,
            &fixed_patch,
        )
        .map_err(|_| MutationJobError::TargetQueryFailed)?;
        record_resumable_rows_scanned(identity.entity_path(), scan.keys_scanned);
        let keys_scanned =
            u64::try_from(scan.keys_scanned).map_err(|_| MutationJobError::CounterOverflow)?;
        let revision_after_scan =
            durable_store_revision(&store).map_err(|_| MutationJobError::TargetQueryFailed)?;
        if scan.residual_work || revision_after_scan != captured_revision {
            continuation.restart_forward();
            return persist_verify_restart::<C>(before, request, &continuation, keys_scanned);
        }

        if scan.exhausted {
            return persist_mutation_job_progress_transition::<C>(
                before,
                request,
                MutationJobTransition::new(
                    MutationJobStatus::Completed,
                    MutationJobPhase::Verify,
                    Vec::new(),
                    keys_scanned,
                    0,
                    0,
                ),
            );
        }

        continuation.checkpoint = scan.final_checkpoint;
        let next_continuation = continuation
            .encode()
            .map_err(|_| MutationJobError::CorruptProgressStore)?
            .into_bytes();
        persist_mutation_job_progress_transition::<C>(
            before,
            request,
            MutationJobTransition::new(
                MutationJobStatus::Active,
                MutationJobPhase::Verify,
                next_continuation,
                keys_scanned,
                0,
                0,
            ),
        )
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
            catalog.inspection_plan().row_contract(),
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

struct ResumableForwardScan {
    final_checkpoint: Option<RawDataStoreKey>,
    physical_keys_scanned: usize,
    exhausted: bool,
    failure: Option<ResumableForwardScanError>,
}

#[derive(Clone, Copy)]
enum ResumableForwardScanError {
    ManagedTimestampRegression,
    Query,
}

struct ResumableForwardScanner<'a> {
    store: &'a StoreHandle,
    checkpoint: Option<RawDataStoreKey>,
    pending_candidate_checkpoint: Option<RawDataStoreKey>,
    entity_tag: EntityTag,
    compiled_scope: &'a CompiledExpr,
    row_contract: &'a StructuralRowContract,
    fixed_patch: &'a AcceptedFixedUpdatePatch,
    advance_timestamp: Timestamp,
    physical_keys_scanned: usize,
    scan_bytes: usize,
    candidates_yielded: usize,
    exhausted: bool,
    boundary_reached: bool,
    failure: Option<ResumableForwardScanError>,
}

impl<'a> ResumableForwardScanner<'a> {
    fn new(
        store: &'a StoreHandle,
        checkpoint: Option<&RawDataStoreKey>,
        entity_tag: EntityTag,
        compiled_scope: &'a CompiledExpr,
        row_contract: &'a StructuralRowContract,
        fixed_patch: &'a AcceptedFixedUpdatePatch,
        advance_timestamp: Timestamp,
    ) -> Self {
        Self {
            store,
            checkpoint: checkpoint.cloned(),
            pending_candidate_checkpoint: None,
            entity_tag,
            compiled_scope,
            row_contract,
            fixed_patch,
            advance_timestamp,
            physical_keys_scanned: 0,
            scan_bytes: 0,
            candidates_yielded: 0,
            exhausted: false,
            boundary_reached: false,
            failure: None,
        }
    }

    fn next_mutation(
        &mut self,
        patch: &crate::db::data::AcceptedMutationIntentPatch,
    ) -> Option<AcceptedStructuralMutation> {
        if self.failure.is_some() || self.exhausted || self.boundary_reached {
            return None;
        }
        if let Some(checkpoint) = self.pending_candidate_checkpoint.take() {
            self.checkpoint = Some(checkpoint);
        }
        if self.candidates_yielded == MAX_RESUMABLE_UPDATE_FORWARD_ROWS {
            self.boundary_reached = true;
            return None;
        }

        let range = RawDataStoreKeyRange::entity_prefix(self.entity_tag);
        let lower = self.checkpoint.clone().map_or_else(
            || Bound::Included(RawDataStoreKey::store_range_lower_key(&range)),
            Bound::Excluded,
        );
        let upper = range
            .upper_exclusive()
            .map(RawDataStoreKey::from_store_range_bound)
            .map_or(Bound::Unbounded, Bound::Excluded);
        let mut candidate = None;
        let mut stopped = false;
        let visit = self.store.with_data(|data| {
            data.visit_range((lower, upper), |raw_key, raw_row| {
                if self.physical_keys_scanned == MAX_RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED {
                    self.boundary_reached = true;
                    stopped = true;
                    return Ok(StoreVisit::Stop);
                }
                let Some(next_scan_bytes) = resumable_scan_bytes_after_row(
                    self.scan_bytes,
                    raw_key.as_bytes().len(),
                    raw_row.len(),
                    MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES,
                ) else {
                    self.boundary_reached = true;
                    stopped = true;
                    return Ok(StoreVisit::Stop);
                };
                self.scan_bytes = next_scan_bytes;
                self.physical_keys_scanned = self.physical_keys_scanned.saturating_add(1);

                let decoded_key = DecodedDataStoreKey::try_from_raw(raw_key)
                    .map_err(|_| InternalError::identity_corruption())?;
                if decoded_key.entity_tag() != self.entity_tag {
                    return Err(InternalError::identity_corruption());
                }
                let row = StructuralSlotReader::from_raw_row_with_validated_contract(
                    raw_row,
                    self.row_contract.clone(),
                )?;
                row.validate_primary_key(&decoded_key)?;
                if resumable_row_needs_patch(self.compiled_scope, self.fixed_patch, &row)? {
                    if managed_timestamp_progression_regresses(
                        self.row_contract,
                        &row,
                        self.advance_timestamp,
                    )? {
                        self.failure = Some(ResumableForwardScanError::ManagedTimestampRegression);
                        stopped = true;
                        return Ok(StoreVisit::Stop);
                    }
                    candidate = Some(AcceptedLoadedStructuralRow::from_validated_parts(
                        decoded_key,
                        raw_row.clone(),
                    ));
                    self.pending_candidate_checkpoint = Some(raw_key.clone());
                    self.candidates_yielded = self.candidates_yielded.saturating_add(1);
                    stopped = true;
                    return Ok(StoreVisit::Stop);
                }
                self.checkpoint = Some(raw_key.clone());
                Ok(StoreVisit::Continue)
            })
        });
        if visit.is_err() {
            self.failure = Some(ResumableForwardScanError::Query);
            return None;
        }
        if !stopped {
            self.exhausted = true;
        }

        candidate.map(|row| {
            AcceptedStructuralMutation::save(
                MutationMode::Update,
                AcceptedStructuralMutationTarget::expected_loaded(row),
                patch.clone(),
            )
        })
    }

    fn finish(&self, packing: AcceptedStructuralMutationPackingReport) -> ResumableForwardScan {
        ResumableForwardScan {
            final_checkpoint: self.checkpoint.clone(),
            physical_keys_scanned: self.physical_keys_scanned,
            exhausted: self.exhausted && !packing.stopped_before_candidate(),
            failure: self.failure,
        }
    }
}

enum PackedForwardOutcome {
    Ready {
        receipt: MutationJobAdvanceReceipt,
        progress_only: Option<MutationProgressRecordOp>,
        physical_keys_scanned: usize,
    },
    Restart(MutationJobRestartReason),
    Failure(MutationJobError),
}

#[expect(
    clippy::too_many_lines,
    reason = "one read-only preparation orders scan failure, policy stop, continuation, counters, and exact progress replacement before the writer commit decision"
)]
fn prepare_packed_forward_outcome(
    before: &MutationJobRecord,
    request: &MutationJobAdvanceRequest,
    continuation: &mut DecodedMutationJobEngineContinuation,
    store: &StoreHandle,
    scan: ResumableForwardScan,
    packing: AcceptedStructuralMutationPackingReport,
) -> (
    PackedForwardOutcome,
    AcceptedStructuralMutationCommitDirective,
) {
    if let Some(failure) = scan.failure {
        let outcome = match failure {
            ResumableForwardScanError::ManagedTimestampRegression => {
                PackedForwardOutcome::Restart(MutationJobRestartReason::ManagedTimestampRegression)
            }
            ResumableForwardScanError::Query => {
                PackedForwardOutcome::Failure(MutationJobError::TargetQueryFailed)
            }
        };
        return (outcome, AcceptedStructuralMutationCommitDirective::Skip);
    }
    if packing.candidate_exceeds_batch_policy() {
        return (
            PackedForwardOutcome::Restart(MutationJobRestartReason::CandidateExceedsBatchPolicy),
            AcceptedStructuralMutationCommitDirective::Skip,
        );
    }

    continuation.checkpoint = scan.final_checkpoint;
    if scan.exhausted {
        continuation.phase = MutationJobEnginePhase::Verify;
        continuation.checkpoint = None;
        let revision = if packing.admitted_mutations() == 0 {
            durable_store_revision(store).map_err(|_| MutationJobError::TargetQueryFailed)
        } else {
            durable_store_revision_after_next_mutation(store)
        };
        let revision = match revision {
            Ok(revision) => revision,
            Err(error) => {
                return (
                    PackedForwardOutcome::Failure(error),
                    AcceptedStructuralMutationCommitDirective::Skip,
                );
            }
        };
        continuation.verify_revision = Some(revision);
    }
    let Ok(next_continuation) = continuation.encode() else {
        return (
            PackedForwardOutcome::Failure(MutationJobError::CorruptProgressStore),
            AcceptedStructuralMutationCommitDirective::Skip,
        );
    };
    let Ok(keys_scanned) = u64::try_from(scan.physical_keys_scanned) else {
        return (
            PackedForwardOutcome::Failure(MutationJobError::CounterOverflow),
            AcceptedStructuralMutationCommitDirective::Skip,
        );
    };
    let Ok(rows_updated) = u64::try_from(packing.admitted_mutations()) else {
        return (
            PackedForwardOutcome::Failure(MutationJobError::CounterOverflow),
            AcceptedStructuralMutationCommitDirective::Skip,
        );
    };
    let phase = if scan.exhausted {
        MutationJobPhase::Verify
    } else {
        MutationJobPhase::Forward
    };
    let (after, receipt) = match before.apply_transition(
        request,
        MutationJobTransition::new(
            MutationJobStatus::Active,
            phase,
            next_continuation.into_bytes(),
            keys_scanned,
            rows_updated,
            0,
        ),
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            return (
                PackedForwardOutcome::Failure(error),
                AcceptedStructuralMutationCommitDirective::Skip,
            );
        }
    };
    let operation = match MutationProgressRecordOp::replace(before, &after) {
        Ok(operation) => operation,
        Err(error) => {
            return (
                PackedForwardOutcome::Failure(error),
                AcceptedStructuralMutationCommitDirective::Skip,
            );
        }
    };
    let physical_keys_scanned = scan.physical_keys_scanned;
    if packing.admitted_mutations() == 0 {
        (
            PackedForwardOutcome::Ready {
                receipt,
                progress_only: Some(operation),
                physical_keys_scanned,
            },
            AcceptedStructuralMutationCommitDirective::Skip,
        )
    } else {
        (
            PackedForwardOutcome::Ready {
                receipt,
                progress_only: None,
                physical_keys_scanned,
            },
            AcceptedStructuralMutationCommitDirective::WithMutationProgress(operation),
        )
    }
}

fn resumable_scan_bytes_after_row(
    current: usize,
    raw_key_bytes: usize,
    raw_row_bytes: usize,
    limit: usize,
) -> Option<usize> {
    current
        .checked_add(raw_key_bytes)?
        .checked_add(raw_row_bytes)
        .filter(|total| *total <= limit)
}

struct ResumableVerifyScan {
    final_checkpoint: Option<RawDataStoreKey>,
    keys_scanned: usize,
    exhausted: bool,
    residual_work: bool,
}

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
    let mut scan_bytes = 0usize;
    let mut has_more = false;
    let mut residual_work = false;

    store
        .with_data(|data| {
            data.visit_range((lower, upper), |raw_key, raw_row| {
                if keys_scanned == MAX_RESUMABLE_UPDATE_VERIFY_KEYS_SCANNED {
                    has_more = true;
                    return Ok(StoreVisit::Stop);
                }
                let Some(next_scan_bytes) = resumable_scan_bytes_after_row(
                    scan_bytes,
                    raw_key.as_bytes().len(),
                    raw_row.len(),
                    MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
                ) else {
                    has_more = true;
                    return Ok(StoreVisit::Stop);
                };
                scan_bytes = next_scan_bytes;
                keys_scanned = keys_scanned.saturating_add(1);

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

fn prepare_mutation_job_forward_runtime<'a>(
    catalog: &'a AcceptedSchemaCatalogContext,
    scope: &Expr,
    fixed_patch: &AcceptedFixedUpdatePatch,
) -> Result<PreparedMutationJobForwardRuntime<'a>, MutationJobExecutionPreparationError> {
    let descriptor = AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
        .map_err(|_| MutationJobExecutionPreparationError::Failure(MutationJobError::Internal))?;
    let PreparedMutationJobTraversalRuntime {
        compiled_scope,
        row_contract,
    } = prepare_mutation_job_traversal_runtime(catalog, scope, fixed_patch)?;
    Ok(PreparedMutationJobForwardRuntime {
        descriptor,
        compiled_scope,
        row_contract,
    })
}

fn prepare_mutation_job_traversal_runtime(
    catalog: &AcceptedSchemaCatalogContext,
    scope: &Expr,
    fixed_patch: &AcceptedFixedUpdatePatch,
) -> Result<PreparedMutationJobTraversalRuntime, MutationJobExecutionPreparationError> {
    let row_contract = catalog.inspection_plan().row_contract().clone();
    prove_resumable_update_fixed_eligibility(
        catalog.snapshot().persisted_snapshot(),
        &row_contract,
        scope,
        fixed_patch,
    )
    .map_err(|_| {
        MutationJobExecutionPreparationError::Restart(MutationJobRestartReason::IntentIneligible)
    })?;
    let compiled_scope =
        compile_scalar_projection_expr_with_schema(catalog.accepted_schema_info(), scope)
            .map(|expr| CompiledExpr::compile(&expr))
            .ok_or(MutationJobExecutionPreparationError::Restart(
                MutationJobRestartReason::IntentIneligible,
            ))?;

    Ok(PreparedMutationJobTraversalRuntime {
        compiled_scope,
        row_contract,
    })
}

fn mutation_job_catalog_authority_matches(
    intent: &CanonicalMutationIntent,
    catalog: &AcceptedSchemaCatalogContext,
) -> bool {
    let identity = catalog.identity();
    identity.store_path() == intent.target_store_path()
        && identity.entity_path() == intent.target_entity_path()
        && identity.entity_tag().value() == intent.target_entity_tag()
        && catalog.revision().get() == intent.accepted_schema_revision()
        && catalog.fingerprint_method_version() == intent.accepted_schema_fingerprint_method()
        && catalog.fingerprint() == intent.accepted_schema_fingerprint()
}

/// Validate the current-format initial continuation used by sequence-zero cancellation.
///
/// The retained policy identity is deliberately not compared with today's
/// identity: cancellation executes no target work and remains available to an
/// otherwise exact zero-effect predecessor policy.
pub(in crate::db::session) fn validate_current_initial_mutation_job_continuation(
    bytes: &[u8],
) -> Result<(), MutationJobError> {
    let continuation = DecodedMutationJobEngineContinuation::decode(bytes)
        .map_err(|_| MutationJobError::CorruptProgressStore)?;
    if continuation.phase != MutationJobEnginePhase::Forward
        || continuation.checkpoint.is_some()
        || continuation.verify_revision.is_some()
    {
        return Err(MutationJobError::CorruptProgressStore);
    }
    Ok(())
}

fn decode_retained_mutation_job_continuation(
    bytes: &[u8],
) -> Result<DecodedMutationJobEngineContinuation, MutationJobExecutionPreparationError> {
    if bytes.get(..4) == Some(RESUMABLE_UPDATE_CONTINUATION_MAGIC)
        && bytes.get(4).copied() != Some(RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION)
        && bytes.len() <= MAX_RESUMABLE_UPDATE_CONTINUATION_BYTES
        && let Some((payload, checksum)) =
            bytes.split_at_checked(bytes.len().saturating_sub(size_of::<u32>()))
        && checksum
            .try_into()
            .map(u32::from_be_bytes)
            .is_ok_and(|expected| crc32c(payload) == expected)
    {
        return Err(MutationJobExecutionPreparationError::Restart(
            MutationJobRestartReason::UnsupportedContinuation,
        ));
    }

    DecodedMutationJobEngineContinuation::decode(bytes).map_err(|_| {
        MutationJobExecutionPreparationError::Failure(MutationJobError::CorruptProgressStore)
    })
}

fn persist_mutation_job_progress_transition<C: CanisterKind>(
    before: &MutationJobRecord,
    request: &MutationJobAdvanceRequest,
    transition: MutationJobTransition,
) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
    let (after, receipt) = before.apply_transition(request, transition)?;
    let progress_operation = MutationProgressRecordOp::replace(before, &after)?;
    replace_mutation_progress_record_op::<C>(&progress_operation)?;
    Ok(receipt)
}

fn persist_terminal_mutation_job_restart<C: CanisterKind>(
    before: &MutationJobRecord,
    request: &MutationJobAdvanceRequest,
    reason: MutationJobRestartReason,
) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
    persist_mutation_job_progress_transition::<C>(
        before,
        request,
        MutationJobTransition::new(
            MutationJobStatus::RestartRequired(reason),
            before.state().phase,
            Vec::new(),
            0,
            0,
            0,
        ),
    )
}

fn persist_verify_restart<C: CanisterKind>(
    before: &MutationJobRecord,
    request: &MutationJobAdvanceRequest,
    continuation: &DecodedMutationJobEngineContinuation,
    keys_scanned: u64,
) -> Result<MutationJobAdvanceReceipt, MutationJobError> {
    let next_continuation = continuation
        .encode()
        .map_err(|_| MutationJobError::CorruptProgressStore)?
        .into_bytes();
    persist_mutation_job_progress_transition::<C>(
        before,
        request,
        MutationJobTransition::new(
            MutationJobStatus::Active,
            MutationJobPhase::Forward,
            next_continuation,
            keys_scanned,
            0,
            1,
        ),
    )
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
    row_contract: &StructuralRowContract,
    selector: &crate::db::query::intent::StructuralQuery,
    patch: &AcceptedFixedUpdatePatch,
) -> Result<ResumableUpdateEligibility, QueryError> {
    let scope = selector.scalar_filter_expr().ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::UpdateMissingWherePredicate)
    })?;
    prove_resumable_update_fixed_eligibility(snapshot, row_contract, scope, patch)
}

fn prove_resumable_update_fixed_eligibility(
    snapshot: &PersistedSchemaSnapshot,
    row_contract: &StructuralRowContract,
    scope: &Expr,
    patch: &AcceptedFixedUpdatePatch,
) -> Result<ResumableUpdateEligibility, QueryError> {
    if snapshot.update_management_requires_global_write_validation() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ResumableUpdateManagedFieldHasGlobalConstraint,
        ));
    }
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
        let field = row_contract
            .required_accepted_field_contract(target.slot().index())
            .map_err(QueryError::execute)?;
        let field_name = field.decode_contract().field_name();
        if scope_dependencies.contains(&field.field_id()) {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::ResumableUpdateScopeDependsOnAssignedField,
            ));
        }
        if snapshot.field_requires_global_write_validation(field.field_id(), field_name) {
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
    fn cancellation_accepts_only_current_initial_continuation_shape() {
        let initial = MutationJobEngineContinuation::initial(
            Ulid::from_bytes([1; 16]),
            7,
            [2; 32],
            1,
            [3; 16],
            [4; 32],
            [5; 32],
            Timestamp::from_millis(6),
        )
        .expect("current initial continuation should encode");
        assert_eq!(
            validate_current_initial_mutation_job_continuation(&initial.bytes),
            Ok(()),
        );

        let mut predecessor_policy = DecodedMutationJobEngineContinuation::decode(&initial.bytes)
            .expect("current initial continuation should decode");
        predecessor_policy.batch_policy_identity =
            predecessor_policy.batch_policy_identity.wrapping_sub(1);
        let predecessor_policy = predecessor_policy
            .encode()
            .expect("prior policy remains current-format");
        assert_eq!(
            validate_current_initial_mutation_job_continuation(&predecessor_policy.bytes),
            Ok(()),
            "a zero-effect cancellation must not execute retained page policy",
        );

        let mut verify = DecodedMutationJobEngineContinuation::decode(&initial.bytes)
            .expect("current initial continuation should decode");
        verify.phase = MutationJobEnginePhase::Verify;
        verify.verify_revision = Some(9);
        let verify = verify.encode().expect("Verify continuation should encode");
        assert_eq!(
            validate_current_initial_mutation_job_continuation(&verify.bytes),
            Err(MutationJobError::CorruptProgressStore),
        );

        let mut corrupt = initial.bytes;
        corrupt[0] ^= 0xff;
        assert_eq!(
            validate_current_initial_mutation_job_continuation(&corrupt),
            Err(MutationJobError::CorruptProgressStore),
        );
    }

    #[test]
    fn resumable_batch_policy_identity_covers_every_compatibility_input() {
        assert_eq!(RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY, 0xda80_2fe2);
        assert_ne!(RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY, 1);

        let mut predecessor = RESUMABLE_UPDATE_BATCH_POLICY_INPUTS;
        let timestamp_policy_index = predecessor.len() - 1;
        predecessor[timestamp_policy_index] = 1;
        assert_eq!(
            resumable_update_batch_policy_identity(predecessor),
            0xd980_2e4f
        );
        assert_ne!(
            resumable_update_batch_policy_identity(predecessor),
            RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY,
        );

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

    #[test]
    fn scan_and_staging_limits_admit_one_maximum_valid_row() {
        let maximum_scan_charge =
            RawDataStoreKey::MAX_STORED_SIZE_USIZE + crate::db::codec::MAX_ROW_BYTES as usize;
        assert!(maximum_scan_charge <= MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES);
        assert!(maximum_scan_charge <= MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES);

        let maximum_staged_charge =
            RawDataStoreKey::MAX_STORED_SIZE_USIZE + (2 * crate::db::codec::MAX_ROW_BYTES as usize);
        assert!(
            maximum_staged_charge
                <= crate::db::session::write::MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES
        );

        let maximum_row_buffer_peak =
            crate::db::session::write::MAX_STRUCTURAL_MUTATION_BATCH_STAGED_BYTES
                + maximum_scan_charge
                + crate::db::codec::MAX_ROW_BYTES as usize;
        assert!(maximum_row_buffer_peak < 32 * 1024 * 1024);
    }

    #[test]
    fn scan_byte_formula_accepts_exact_limit_and_stops_before_max_plus_one() {
        assert_eq!(
            resumable_scan_bytes_after_row(
                0,
                RawDataStoreKey::MAX_STORED_SIZE_USIZE,
                MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES - RawDataStoreKey::MAX_STORED_SIZE_USIZE,
                MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES,
            ),
            Some(MAX_RESUMABLE_UPDATE_FORWARD_SCAN_BYTES),
        );
        assert_eq!(
            resumable_scan_bytes_after_row(
                MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
                0,
                0,
                MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
            ),
            Some(MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES),
        );
        assert_eq!(
            resumable_scan_bytes_after_row(
                MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
                0,
                1,
                MAX_RESUMABLE_UPDATE_VERIFY_SCAN_BYTES,
            ),
            None,
        );
    }

    #[test]
    fn retained_continuation_distinguishes_unsupported_format_from_corruption() {
        let mut unsupported_version = RESUMABLE_UPDATE_CONTINUATION_MAGIC.to_vec();
        unsupported_version.push(RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION.saturating_add(1));
        unsupported_version.extend_from_slice(&crc32c(&unsupported_version).to_be_bytes());
        assert!(matches!(
            decode_retained_mutation_job_continuation(&unsupported_version),
            Err(MutationJobExecutionPreparationError::Restart(
                MutationJobRestartReason::UnsupportedContinuation
            ))
        ));

        let malformed_current = [
            RESUMABLE_UPDATE_CONTINUATION_MAGIC[0],
            RESUMABLE_UPDATE_CONTINUATION_MAGIC[1],
            RESUMABLE_UPDATE_CONTINUATION_MAGIC[2],
            RESUMABLE_UPDATE_CONTINUATION_MAGIC[3],
            RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION,
        ];
        assert!(matches!(
            decode_retained_mutation_job_continuation(&malformed_current),
            Err(MutationJobExecutionPreparationError::Failure(
                MutationJobError::CorruptProgressStore
            ))
        ));

        let unknown_magic = [
            b'O',
            b'L',
            b'D',
            b'U',
            RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION,
        ];
        assert!(matches!(
            decode_retained_mutation_job_continuation(&unknown_magic),
            Err(MutationJobExecutionPreparationError::Failure(
                MutationJobError::CorruptProgressStore
            ))
        ));
    }
}
