//! Module: db::session::sql::resumable_update
//! Responsibility: trusted resumable-update preparation, eligibility proof,
//! current continuation encoding, bounded Forward execution, and stable-revision verification.
//! Does not own: application authorization, operation identity, or durable custody.
//! Boundary: accepted SQL update plan plus accepted schema -> opaque trusted
//! continuation; each resume performs one bounded Forward or Verify step.

use crate::{
    db::{
        DbSession, PersistedRow, QueryError,
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_u64,
        },
        data::{
            AcceptedFixedUpdatePatch, AcceptedMutationIntentPatch, DecodedDataStoreKey,
            RawDataStoreKey, StoreVisit, StructuralRowContract, StructuralSlotReader,
        },
        database_format::crc32c,
        executor::{
            StructuralMutationTargetKey, eval_compiled_filter_expr_with_required_slot_reader,
        },
        journal::JournalTailStore,
        key_taxonomy::RawDataStoreKeyRange,
        query::{
            plan::expr::{
                CompiledExpr, collect_scalar_expr_field_roots,
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
            AcceptedSchemaCatalogContext,
            accepted_schema::accepted_save_contract_for_catalog_context,
        },
    },
    error::InternalError,
    metrics::sink::{MetricsEvent, record},
    sanitize::{SanitizeWriteContext, SanitizeWriteMode},
    traits::{CanisterKind, Path},
    types::{EntityTag, Timestamp, Ulid},
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;
use sha2::Digest;
use std::{collections::BTreeSet, ops::Bound};

const RESUMABLE_UPDATE_CONTINUATION_MAGIC: &[u8; 4] = b"ICYU";
const RESUMABLE_UPDATE_CONTINUATION_FORMAT_VERSION: u8 = 1;
const RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY: u32 = 1;
const RESUMABLE_UPDATE_PHASE_FORWARD: u8 = 1;
const RESUMABLE_UPDATE_PHASE_VERIFY: u8 = 2;
const RESUMABLE_UPDATE_TARGET_IDENTITY_DOMAIN: &[u8] = b"icydb.resumable-update-target.v1";
const MAX_RESUMABLE_UPDATE_CONTINUATION_BYTES: usize = 2 * 1024;
const MAX_RESUMABLE_UPDATE_FORWARD_KEYS_SCANNED: usize = 256;
const MAX_RESUMABLE_UPDATE_FORWARD_ROWS: usize = 64;

/// Current trusted resumable-update execution phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrustedResumableUpdatePhase {
    /// Bounded primary-key traversal and atomic convergence batches.
    Forward,
    /// Stable-revision verification before completion.
    Verify,
}

/// Reason one stable verification sweep restarted Forward convergence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrustedResumableUpdateRestartReason {
    /// The durable target-store mutation revision changed during verification.
    RevisionChanged,
    /// A scoped row still needed the fixed authored patch.
    ResidualWork,
}

impl TrustedResumableUpdatePhase {
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

/// Opaque current-format continuation for one trusted resumable SQL update.
///
/// Applications must store these bytes durably outside the target store before
/// asking IcyDB to execute the first page. The bytes are proof-bearing engine
/// state, not authorization, and must never be accepted from an untrusted
/// public or generated endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedResumableUpdateContinuation {
    bytes: Vec<u8>,
}

impl TrustedResumableUpdateContinuation {
    /// Borrow the bounded current-format bytes for trusted application custody.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Consume the continuation into bytes for trusted application custody.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Reconstruct one trusted continuation from application-custodied bytes.
    ///
    /// This validates only the bounded current wire form. Resume separately
    /// binds the token to current target, schema, scope, patch, and batch policy.
    pub fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, QueryError> {
        let _ = DecodedResumableUpdateContinuation::decode(bytes.as_slice())?;

        Ok(Self { bytes })
    }

    fn initial(
        operation_id: Ulid,
        entity_tag: u64,
        target_identity: [u8; 32],
        schema_fingerprint_method_version: u8,
        schema_fingerprint: [u8; 16],
        scope_fingerprint: [u8; 32],
        patch_fingerprint: [u8; 32],
    ) -> Result<Self, QueryError> {
        DecodedResumableUpdateContinuation {
            operation_id,
            entity_tag,
            target_identity,
            schema_fingerprint_method_version,
            schema_fingerprint,
            scope_fingerprint,
            patch_fingerprint,
            phase: TrustedResumableUpdatePhase::Forward,
            checkpoint: None,
            verify_revision: None,
            batch_policy_identity: RESUMABLE_UPDATE_BATCH_POLICY_IDENTITY,
        }
        .encode()
    }
}

/// Per-call trusted resumable-update progress.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedResumableUpdateReceipt {
    phase: TrustedResumableUpdatePhase,
    keys_scanned: u32,
    rows_updated: u32,
    restart_reason: Option<TrustedResumableUpdateRestartReason>,
    continuation: Option<TrustedResumableUpdateContinuation>,
    complete: bool,
}

impl TrustedResumableUpdateReceipt {
    /// Return the phase selected for the next resume call.
    #[must_use]
    pub const fn phase(&self) -> TrustedResumableUpdatePhase {
        self.phase
    }

    /// Return authoritative entity keys fully examined by this call.
    #[must_use]
    pub const fn keys_scanned(&self) -> u32 {
        self.keys_scanned
    }

    /// Return rows atomically committed by this call.
    #[must_use]
    pub const fn rows_updated(&self) -> u32 {
        self.rows_updated
    }

    /// Return why this call restarted Forward convergence, when applicable.
    #[must_use]
    pub const fn restart_reason(&self) -> Option<TrustedResumableUpdateRestartReason> {
        self.restart_reason
    }

    /// Borrow the next proof-bearing trusted continuation while in progress.
    #[must_use]
    pub const fn continuation(&self) -> Option<&TrustedResumableUpdateContinuation> {
        self.continuation.as_ref()
    }

    /// Consume this receipt into the next trusted continuation while in progress.
    #[must_use]
    pub fn into_continuation(self) -> Option<TrustedResumableUpdateContinuation> {
        self.continuation
    }

    /// Return whether stable full verification has completed.
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }
}

/// Decoded current continuation with all phase-dependent state kept together.
struct DecodedResumableUpdateContinuation {
    operation_id: Ulid,
    entity_tag: u64,
    target_identity: [u8; 32],
    schema_fingerprint_method_version: u8,
    schema_fingerprint: [u8; 16],
    scope_fingerprint: [u8; 32],
    patch_fingerprint: [u8; 32],
    phase: TrustedResumableUpdatePhase,
    checkpoint: Option<RawDataStoreKey>,
    verify_revision: Option<u64>,
    batch_policy_identity: u32,
}

impl DecodedResumableUpdateContinuation {
    fn encode(&self) -> Result<TrustedResumableUpdateContinuation, QueryError> {
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

        Ok(TrustedResumableUpdateContinuation { bytes })
    }

    fn decode(bytes: &[u8]) -> Result<Self, QueryError> {
        if bytes.len() > MAX_RESUMABLE_UPDATE_CONTINUATION_BYTES
            || bytes.len() < 156
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
        let phase = TrustedResumableUpdatePhase::from_wire(reader.read_u8()?)?;
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
            || (phase == TrustedResumableUpdatePhase::Forward && verify_revision.is_some())
            || (phase == TrustedResumableUpdatePhase::Verify && verify_revision.is_none())
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
            phase,
            checkpoint,
            verify_revision,
            batch_policy_identity,
        })
    }

    fn restart_forward(&mut self) {
        self.phase = TrustedResumableUpdatePhase::Forward;
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

impl<C: CanisterKind> DbSession<C> {
    /// Prepare one trusted resumable SQL `UPDATE` without reading or mutating rows.
    ///
    /// The statement must describe a fixed convergence patch over a stable
    /// single-entity scope in a journaled store. This call only validates and
    /// binds current accepted authority. The application must durably custody
    /// the returned continuation before a later resume call may mutate rows.
    pub fn prepare_trusted_sql_resumable_update<E>(
        &self,
        operation_id: Ulid,
        sql: &str,
    ) -> Result<TrustedResumableUpdateContinuation, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let store = self
            .db
            .recovered_store(E::Store::PATH)
            .map_err(QueryError::execute)?;
        if store.storage_capabilities().storage_mode() != StoreRuntimeStorageMode::Journaled {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::ResumableUpdateRequiresJournaledStore,
            ));
        }
        self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
            None,
            None,
            |catalog, descriptor| {
                let report = with_accepted_sql_update_policy_context(&descriptor, |context| {
                    classify_sql_resumable_update_policy(
                        sql,
                        catalog.snapshot().persisted_snapshot().entity_name(),
                        context,
                    )
                })?;
                let plan = require_resumable_update_plan(report)?;
                let selector = Self::sql_update_selector_query::<E>(
                    &catalog.accepted_schema_info_for::<E>(),
                    plan.statement(),
                )?;
                let patch = Self::sql_structural_patch(&descriptor, plan.statement())?;
                let fixed_patch = AcceptedFixedUpdatePatch::from_update_intent(
                    E::PATH,
                    descriptor.row_decode_contract(catalog.value_catalog_handle().clone()),
                    &patch,
                )
                .map_err(QueryError::execute)?;
                let eligibility = prove_resumable_update_eligibility::<E>(
                    catalog.snapshot().persisted_snapshot(),
                    &descriptor,
                    &selector,
                    &fixed_patch,
                )?;
                let target_identity = resumable_update_target_identity(
                    &store,
                    E::Store::PATH,
                    E::PATH,
                    E::ENTITY_TAG.value(),
                )?;

                TrustedResumableUpdateContinuation::initial(
                    operation_id,
                    E::ENTITY_TAG.value(),
                    target_identity,
                    catalog.fingerprint_method_version(),
                    catalog.fingerprint(),
                    eligibility.scope_fingerprint,
                    eligibility.patch_fingerprint,
                )
            },
        )
    }

    /// Resume one trusted resumable SQL `UPDATE` for one bounded engine step.
    ///
    /// The application must supply the same SQL meaning used at preparation
    /// and a continuation loaded from trusted durable custody. This method
    /// rebinds every proof before row access, scans at most 256 authoritative
    /// keys, stages at most 64 updates, and commits at most one atomic batch.
    /// Verify steps are read-only and return complete only after a stable full
    /// accepted-keyspace sweep.
    pub fn resume_trusted_sql_resumable_update<E>(
        &self,
        sql: &str,
        continuation: &TrustedResumableUpdateContinuation,
    ) -> Result<TrustedResumableUpdateReceipt, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        let mut decoded = DecodedResumableUpdateContinuation::decode(continuation.as_bytes())?;

        let store = self
            .db
            .recovered_store(E::Store::PATH)
            .map_err(QueryError::execute)?;
        if store.storage_capabilities().storage_mode() != StoreRuntimeStorageMode::Journaled {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::ResumableUpdateRequiresJournaledStore,
            ));
        }

        self.with_checked_accepted_write_descriptor_for_returning::<E, _>(
            None,
            None,
            |catalog, descriptor| {
                resume_resumable_update_with_authority::<C, E>(
                    self,
                    sql,
                    &store,
                    &mut decoded,
                    catalog,
                    descriptor,
                )
            },
        )
    }
}

fn resume_resumable_update_with_authority<C, E>(
    session: &DbSession<C>,
    sql: &str,
    store: &StoreHandle,
    continuation: &mut DecodedResumableUpdateContinuation,
    catalog: &AcceptedSchemaCatalogContext,
    descriptor: AcceptedRowLayoutRuntimeContract<'_>,
) -> Result<TrustedResumableUpdateReceipt, QueryError>
where
    C: CanisterKind,
    E: PersistedRow<Canister = C>,
{
    let report = with_accepted_sql_update_policy_context(&descriptor, |context| {
        classify_sql_resumable_update_policy(
            sql,
            catalog.snapshot().persisted_snapshot().entity_name(),
            context,
        )
    })?;
    let plan = require_resumable_update_plan(report)?;
    let schema_info = catalog.accepted_schema_info_for::<E>();
    let selector = DbSession::<C>::sql_update_selector_query::<E>(&schema_info, plan.statement())?;
    let patch = DbSession::<C>::sql_structural_patch(&descriptor, plan.statement())?;
    let fixed_patch = AcceptedFixedUpdatePatch::from_update_intent(
        E::PATH,
        descriptor.row_decode_contract(catalog.value_catalog_handle().clone()),
        &patch,
    )
    .map_err(QueryError::execute)?;
    let eligibility = prove_resumable_update_eligibility::<E>(
        catalog.snapshot().persisted_snapshot(),
        &descriptor,
        &selector,
        &fixed_patch,
    )?;
    let target_identity =
        resumable_update_target_identity(store, E::Store::PATH, E::PATH, E::ENTITY_TAG.value())?;
    validate_resumable_update_bindings(
        continuation,
        E::ENTITY_TAG.value(),
        target_identity,
        catalog.fingerprint_method_version(),
        catalog.fingerprint(),
        &eligibility,
    )?;
    validate_resumable_update_checkpoint::<E>(continuation)?;

    let scope = selector.scalar_filter_expr().ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::UpdateMissingWherePredicate)
    })?;
    let compiled_scope = compile_scalar_projection_expr_with_schema(&schema_info, scope)
        .map(|expr| CompiledExpr::compile(&expr))
        .ok_or_else(|| {
            QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::ResumableUpdateScopeDependencyUnknown,
            )
        })?;
    let row_contract = StructuralRowContract::from_accepted_decode_contract(
        E::PATH,
        descriptor.row_decode_contract(catalog.value_catalog_handle().clone()),
    );

    match continuation.phase {
        TrustedResumableUpdatePhase::Forward => resume_resumable_update_forward::<C, E>(
            session,
            store,
            continuation,
            catalog,
            &descriptor,
            &compiled_scope,
            &row_contract,
            &fixed_patch,
            &patch,
        ),
        TrustedResumableUpdatePhase::Verify => resume_resumable_update_verify::<E>(
            store,
            continuation,
            &compiled_scope,
            &row_contract,
            &fixed_patch,
        ),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the Forward boundary keeps each already-bound authority explicit"
)]
fn resume_resumable_update_forward<C, E>(
    session: &DbSession<C>,
    store: &StoreHandle,
    continuation: &mut DecodedResumableUpdateContinuation,
    catalog: &AcceptedSchemaCatalogContext,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    compiled_scope: &CompiledExpr,
    row_contract: &StructuralRowContract,
    fixed_patch: &AcceptedFixedUpdatePatch,
    patch: &AcceptedMutationIntentPatch,
) -> Result<TrustedResumableUpdateReceipt, QueryError>
where
    C: CanisterKind,
    E: PersistedRow<Canister = C>,
{
    let scan = scan_resumable_update_forward::<E>(
        store,
        continuation.checkpoint.as_ref(),
        compiled_scope,
        row_contract,
        fixed_patch,
    )?;
    record_resumable_rows_scanned::<E>(scan.physical_keys_scanned);

    let candidate_rows = scan
        .candidates
        .iter()
        .map(|candidate| {
            (
                StructuralMutationTargetKey::expected(candidate.key),
                patch.clone(),
            )
        })
        .collect::<Vec<_>>();
    let committed_rows = if candidate_rows.is_empty() {
        0
    } else {
        let (
            row_decode_contract,
            mutation_row_decode_contract,
            accepted_schema_info,
            accepted_schema_fingerprint,
        ) = accepted_save_contract_for_catalog_context::<E>(catalog, descriptor);
        session
            .execute_save_with_checked_accepted_row_contract::<E, _, _>(
                row_decode_contract,
                accepted_schema_info,
                accepted_schema_fingerprint,
                |save| {
                    save.apply_internal_lowered_resumable_structural_update_prefix(
                        candidate_rows,
                        SanitizeWriteContext::new(SanitizeWriteMode::Update, Timestamp::now()),
                        mutation_row_decode_contract,
                    )
                },
                std::convert::identity,
            )
            .map_err(QueryError::execute)?
    };

    let progress = scan.progress_after_committed_rows(committed_rows)?;
    continuation.checkpoint = progress.checkpoint;
    if progress.exhausted {
        continuation.phase = TrustedResumableUpdatePhase::Verify;
        continuation.checkpoint = None;
        continuation.verify_revision = Some(durable_store_revision(store)?);
    }

    in_progress_receipt(continuation, progress.keys_scanned, committed_rows, None)
}

fn in_progress_receipt(
    continuation: &DecodedResumableUpdateContinuation,
    keys_scanned: usize,
    rows_updated: usize,
    restart_reason: Option<TrustedResumableUpdateRestartReason>,
) -> Result<TrustedResumableUpdateReceipt, QueryError> {
    Ok(TrustedResumableUpdateReceipt {
        phase: continuation.phase,
        keys_scanned: u32::try_from(keys_scanned).map_err(|_| QueryError::invariant())?,
        rows_updated: u32::try_from(rows_updated).map_err(|_| QueryError::invariant())?,
        restart_reason,
        continuation: Some(continuation.encode()?),
        complete: false,
    })
}

fn complete_receipt(keys_scanned: usize) -> Result<TrustedResumableUpdateReceipt, QueryError> {
    Ok(TrustedResumableUpdateReceipt {
        phase: TrustedResumableUpdatePhase::Verify,
        keys_scanned: u32::try_from(keys_scanned).map_err(|_| QueryError::invariant())?,
        rows_updated: 0,
        restart_reason: None,
        continuation: None,
        complete: true,
    })
}

struct ResumableForwardCandidate<K> {
    key: K,
    checkpoint_before: Option<RawDataStoreKey>,
    keys_scanned_before: usize,
}

struct ResumableForwardScan<K> {
    candidates: Vec<ResumableForwardCandidate<K>>,
    final_checkpoint: Option<RawDataStoreKey>,
    physical_keys_scanned: usize,
    exhausted: bool,
}

struct ResumableForwardProgress {
    checkpoint: Option<RawDataStoreKey>,
    keys_scanned: usize,
    exhausted: bool,
}

impl<K> ResumableForwardScan<K> {
    fn progress_after_committed_rows(
        &self,
        committed_rows: usize,
    ) -> Result<ResumableForwardProgress, QueryError> {
        if committed_rows > self.candidates.len() {
            return Err(QueryError::invariant());
        }
        if committed_rows < self.candidates.len() {
            let deferred = &self.candidates[committed_rows];
            return Ok(ResumableForwardProgress {
                checkpoint: deferred.checkpoint_before.clone(),
                keys_scanned: deferred.keys_scanned_before,
                exhausted: false,
            });
        }

        Ok(ResumableForwardProgress {
            checkpoint: self.final_checkpoint.clone(),
            keys_scanned: self.physical_keys_scanned,
            exhausted: self.exhausted,
        })
    }
}

fn scan_resumable_update_forward<E>(
    store: &StoreHandle,
    checkpoint: Option<&RawDataStoreKey>,
    compiled_scope: &CompiledExpr,
    row_contract: &StructuralRowContract,
    fixed_patch: &AcceptedFixedUpdatePatch,
) -> Result<ResumableForwardScan<E::Key>, QueryError>
where
    E: PersistedRow,
{
    let range = RawDataStoreKeyRange::entity_prefix(E::ENTITY_TAG);
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
                if decoded_key.entity_tag() != E::ENTITY_TAG {
                    return Err(InternalError::identity_corruption());
                }
                let row = StructuralSlotReader::from_raw_row_with_validated_contract(
                    raw_row,
                    row_contract.clone(),
                )?;
                if resumable_row_needs_patch(compiled_scope, fixed_patch, &row)? {
                    candidates.push(ResumableForwardCandidate {
                        key: decoded_key.try_key::<E>()?,
                        checkpoint_before: final_checkpoint.clone(),
                        keys_scanned_before: physical_keys_scanned,
                    });
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

struct ResumableVerifyScan {
    final_checkpoint: Option<RawDataStoreKey>,
    keys_scanned: usize,
    exhausted: bool,
    residual_work: bool,
}

fn resume_resumable_update_verify<E>(
    store: &StoreHandle,
    continuation: &mut DecodedResumableUpdateContinuation,
    compiled_scope: &CompiledExpr,
    row_contract: &StructuralRowContract,
    fixed_patch: &AcceptedFixedUpdatePatch,
) -> Result<TrustedResumableUpdateReceipt, QueryError>
where
    E: PersistedRow,
{
    let captured_revision = continuation
        .verify_revision
        .ok_or_else(QueryError::invariant)?;
    if durable_store_revision(store)? != captured_revision {
        continuation.restart_forward();
        return in_progress_receipt(
            continuation,
            0,
            0,
            Some(TrustedResumableUpdateRestartReason::RevisionChanged),
        );
    }

    let scan = scan_resumable_update_verify::<E>(
        store,
        continuation.checkpoint.as_ref(),
        compiled_scope,
        row_contract,
        fixed_patch,
    )?;
    record_resumable_rows_scanned::<E>(scan.keys_scanned);
    if scan.residual_work {
        continuation.restart_forward();
        return in_progress_receipt(
            continuation,
            scan.keys_scanned,
            0,
            Some(TrustedResumableUpdateRestartReason::ResidualWork),
        );
    }

    continuation.checkpoint = scan.final_checkpoint;
    if !scan.exhausted {
        return in_progress_receipt(continuation, scan.keys_scanned, 0, None);
    }
    if durable_store_revision(store)? != captured_revision {
        continuation.restart_forward();
        return in_progress_receipt(
            continuation,
            scan.keys_scanned,
            0,
            Some(TrustedResumableUpdateRestartReason::RevisionChanged),
        );
    }

    complete_receipt(scan.keys_scanned)
}

fn scan_resumable_update_verify<E>(
    store: &StoreHandle,
    checkpoint: Option<&RawDataStoreKey>,
    compiled_scope: &CompiledExpr,
    row_contract: &StructuralRowContract,
    fixed_patch: &AcceptedFixedUpdatePatch,
) -> Result<ResumableVerifyScan, QueryError>
where
    E: PersistedRow,
{
    let range = RawDataStoreKeyRange::entity_prefix(E::ENTITY_TAG);
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
                if decoded_key.entity_tag() != E::ENTITY_TAG {
                    return Err(InternalError::identity_corruption());
                }
                let _ = decoded_key.try_key::<E>()?;
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

fn record_resumable_rows_scanned<E>(keys_scanned: usize)
where
    E: PersistedRow,
{
    record(MetricsEvent::RowsScanned {
        entity_path: E::PATH,
        rows_scanned: u64::try_from(keys_scanned).unwrap_or(u64::MAX),
    });
}

fn durable_store_revision(store: &StoreHandle) -> Result<u64, QueryError> {
    let journal = store
        .journal_tail_store()
        .ok_or_else(QueryError::invariant)?;
    let next = journal
        .with_borrow(JournalTailStore::next_append_sequence)
        .map_err(QueryError::execute)?;

    Ok(next.get())
}

fn validate_resumable_update_bindings(
    continuation: &DecodedResumableUpdateContinuation,
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

fn validate_resumable_update_checkpoint<E>(
    continuation: &DecodedResumableUpdateContinuation,
) -> Result<(), QueryError>
where
    E: PersistedRow,
{
    let Some(checkpoint) = continuation.checkpoint.as_ref() else {
        return Ok(());
    };
    let decoded =
        DecodedDataStoreKey::try_from_raw(checkpoint).map_err(|_| malformed_continuation())?;
    let _ = decoded
        .try_key::<E>()
        .map_err(|_| malformed_continuation())?;

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

fn prove_resumable_update_eligibility<E>(
    snapshot: &PersistedSchemaSnapshot,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    selector: &crate::db::query::intent::StructuralQuery,
    patch: &AcceptedFixedUpdatePatch,
) -> Result<ResumableUpdateEligibility, QueryError>
where
    E: PersistedRow,
{
    require_resumable_update_without_application_callbacks::<E>()
        .map_err(QueryError::sql_write_boundary)?;
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

fn require_resumable_update_without_application_callbacks<E>() -> Result<(), SqlWriteBoundaryCode>
where
    E: crate::visitor::Visitable,
{
    if E::requires_application_write_callbacks() {
        return Err(SqlWriteBoundaryCode::ResumableUpdateApplicationCallbacksUnsupported);
    }

    Ok(())
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

fn hash_store_allocation_identity(hasher: &mut sha2::Sha256, identity: StoreAllocationIdentity) {
    hasher.update([identity.memory_id()]);
    write_hash_str_u32(hasher, identity.stable_key());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable};

    struct ApplicationCallbackProfile;

    impl SanitizeAuto for ApplicationCallbackProfile {}
    impl SanitizeCustom for ApplicationCallbackProfile {}
    impl ValidateAuto for ApplicationCallbackProfile {}
    impl ValidateCustom for ApplicationCallbackProfile {}

    impl Visitable for ApplicationCallbackProfile {
        fn requires_application_write_callbacks() -> bool {
            true
        }
    }

    #[test]
    fn application_callback_profile_rejects_resumable_execution() {
        assert_eq!(
            require_resumable_update_without_application_callbacks::<ApplicationCallbackProfile>(),
            Err(SqlWriteBoundaryCode::ResumableUpdateApplicationCallbacksUnsupported),
        );
    }
}
