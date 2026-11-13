//! Module: db::schema::application
//! Responsibility: issue proposal targets and admit exact schema-application requests.
//! Does not own: proposal lowering, accepted candidate construction, or activation progress.
//! Boundary: recovered runtime/catalog authority plus one proposal -> atomic publication and receipt.

use crate::{
    db::{
        Db,
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_len_u32, write_hash_str_u32,
            write_hash_tag_u8, write_hash_u64,
        },
        commit::{
            AcceptedSchemaPublication, DatabaseControlOp, database_incarnation_id,
            ensure_recovery_admitted, publish_accepted_schema_candidates_with_application_record,
            publish_accepted_schema_candidates_with_database_control,
            publish_generated_row_local_abort_with_application_record,
        },
        data::{DataStore, RawDataStoreKey, StoreVisit},
        index::{IndexState, IndexStore},
        integrity::DatabaseIncarnationId,
        key_taxonomy::RawDataStoreKeyRange,
        registry::{
            StoreAllocationIdentity, StoreAllocationIdentityCapability, StoreCommitParticipation,
            StoreDurability, StoreHandle, StoreRecoveryCapability, StoreRelationSourceCapability,
            StoreRelationTargetCapability, StoreRuntimeStorageMode, StoreSchemaMetadataCapability,
        },
        relation::prove_empty_reverse_relation_domain,
        schema::ensure_schema_migration_ready_for_schema_changes,
        schema::{
            AcceptedSchemaRevision, AcceptedSchemaRevisionBundle, ApplicationRecordKey,
            CandidateSchemaRevision, ConstraintActivationKind, ConstraintActivationState,
            ConstraintId, ConstraintOrigin, ConstraintValidationPhase,
            ConstraintValidationProgress, ExistingProposalStore,
            MAX_IDENTITY_STATE_RECORDS_PER_DATABASE, PersistedSchemaSnapshot, ProposalStoreTarget,
            SchemaApplicationRecord, SchemaApplicationRecordOp, SchemaChangeActivation,
            SchemaChangeJob, SchemaChangeJobId, SchemaChangeOutcome, SchemaChangeProgress,
            SchemaChangeProgressStatus, SchemaChangeReceipt, SchemaChangeValidationPhase,
            StagedUserIndexDomainError, UnpublishedRowLocalValidation,
            advance_accepted_row_local_constraint_activation, constraint_validation_finding_output,
            derive_schema_change_job_id,
            live_schema_checkpoint::load_entity_source_lineage_catalog,
            load_schema_application_record_read_only, lower_existing_schema_proposal,
            lower_generated_existing_schema_proposal, lower_initial_schema_proposal,
            migration_lineage::{
                EntitySourceLineageCatalogOp, complete_pending_source_lineage,
                prepare_ordinary_source_lineage, require_pending_source_lineage,
            },
            prove_empty_user_index_domain, validate_unpublished_row_local_candidate_bounded,
            with_schema_application_store,
        },
    },
    error::InternalError,
    metrics::{ExecutionMetricsPhase, ExecutionMetricsSpan},
    traits::CanisterKind,
    types::EntityTag,
};

use icydb_schema::{
    ExpectedAcceptedHead, ExpectedSchemaFingerprint, SchemaProposal, SchemaProposalDigest,
    SchemaSubmissionKey, TargetDatabaseIdentity, TargetStoreIdentity,
};
use serde::Deserialize;
use sha2::Digest;
#[cfg(feature = "migration")]
use std::collections::BTreeMap;
use std::{cell::Cell, ops::Bound};

#[cfg(feature = "migration")]
use crate::db::commit::{
    RecoveryProgress, StartupRecoveryFailure, continue_recovery_with_failure_authority,
};

#[cfg(feature = "migration")]
use crate::db::schema::{
    PersistedSchemaMigrationEntity, PersistedSchemaMigrationFindingKind,
    PersistedSchemaMigrationIndex, PersistedSchemaMigrationPhase,
    PersistedSchemaMigrationTransformReason, PersistedSchemaMigrationTransition,
    SchemaMigrationCommand, SchemaMigrationEntityTransition, SchemaMigrationFinding,
    SchemaMigrationFindingKind, SchemaMigrationPhase, SchemaMigrationReceipt,
    SchemaMigrationRecord, SchemaMigrationRecordOp, SchemaMigrationStatusPage,
    SchemaMigrationStatusRequest, SchemaMigrationTransformReason,
    live_schema_checkpoint::load_schema_migration_record,
    migration_execution::{
        cleanup_migration_staging_page, final_validate_migration_page,
        migration_derived_domain_count, publish_migration_rewrite_page, rewrite_migration_page,
    },
    migration_lineage::{
        AcceptedEntitySourceLineage, AcceptedEntitySourceLineageCatalog,
        AcceptedEntitySourceLineageState,
    },
    migration_planner::{
        PlannedEntitySourceLineage, SchemaMigrationPlanningError, plan_entity_source_adoption,
        plan_new_entity_source_lineage, plan_schema_migration,
    },
    migration_validation::{stage_migration_index_entries, validate_migration_page},
};

#[cfg(feature = "migration")]
use icydb_diagnostic_code::SchemaMigrationCode;
#[cfg(feature = "migration")]
use icydb_schema::{EntitySourceKey, SchemaMigrationPlanDigest};

const DATABASE_TARGET_FINGERPRINT_PROFILE: &[u8] = b"icydb.schema-target.incarnation.v1";
const STORE_TARGET_FINGERPRINT_PROFILE: &[u8] = b"icydb.schema-target.store.v1";
const ACCEPTED_DATABASE_HEAD_FINGERPRINT_PROFILE: &[u8] = b"icydb.accepted-schema.database-head.v1";
#[cfg(feature = "migration")]
const SCHEMA_MIGRATION_SUBMISSION_PROFILE: &[u8] = b"icydb.schema-migration.submission.v1";

#[derive(Clone, Copy)]
struct GeneratedDatabaseIdentityCacheEntry {
    incarnation: DatabaseIncarnationId,
    identity: TargetDatabaseIdentity,
}

thread_local! {
    // Database identity belongs to the durable incarnation, not its current
    // store topology. This cache holds no readiness or accepted-schema facts.
    static GENERATED_DATABASE_IDENTITY: Cell<Option<GeneratedDatabaseIdentityCacheEntry>> =
        const { Cell::new(None) };
}

///
/// SchemaApplicationStore
///
/// One registered store path paired with the opaque routing token accepted by
/// the current database incarnation.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct SchemaApplicationStore {
    path: String,
    identity: TargetStoreIdentity,
}

impl SchemaApplicationStore {
    /// Borrow the registered store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return the opaque routing identity for this store.
    #[must_use]
    pub const fn identity(&self) -> TargetStoreIdentity {
        self.identity
    }
}

///
/// SchemaApplicationTarget
///
/// Point-in-time optimistic application context issued from recovered runtime
/// authority. Callers compose proposals against these opaque identities and
/// this exact database-wide accepted head.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct SchemaApplicationTarget {
    database_identity: TargetDatabaseIdentity,
    accepted_head: ExpectedAcceptedHead,
    stores: Vec<SchemaApplicationStore>,
}

impl SchemaApplicationTarget {
    /// Return the opaque current database identity.
    #[must_use]
    pub const fn database_identity(&self) -> TargetDatabaseIdentity {
        self.database_identity
    }

    /// Borrow the exact optimistic accepted head.
    #[must_use]
    pub const fn accepted_head(&self) -> &ExpectedAcceptedHead {
        &self.accepted_head
    }

    /// Borrow registered stores in canonical path order.
    #[must_use]
    pub const fn stores(&self) -> &[SchemaApplicationStore] {
        self.stores.as_slice()
    }
}

///
/// StoreApplicationAuthority
///
/// Canonically ordered registry facts used to derive opaque proposal routing
/// identities without exposing physical allocation details.
///

#[derive(Clone, Copy)]
struct StoreApplicationAuthority {
    path: &'static str,
    handle: StoreHandle,
}

/// Catalog authority resolved for one pending generated row-local abort.
struct PendingApplicationAbort {
    authority: StoreApplicationAuthority,
    current: AcceptedSchemaRevisionBundle,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    remove_validation_job: bool,
}

///
/// AcceptedStoreHead
///
/// Exact store-local root facts contributing to the database-wide optimistic
/// accepted head. Absence is represented explicitly by the enclosing option.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AcceptedStoreHead {
    revision: u64,
    fingerprint: [u8; 32],
}

/// One new generated row-local activation awaiting direct bounded proof.
#[derive(Clone)]
struct DirectGeneratedRowLocalProof {
    candidate_index: usize,
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: crate::types::EntityTag,
    entity_path: String,
    constraint_id: ConstraintId,
    has_rows: bool,
}

/// One generated row-local constraint whose proof requires durable continuation.
#[derive(Clone)]
struct PendingGeneratedRowLocalConstraint {
    proof: DirectGeneratedRowLocalProof,
}

/// Catalog-native application staging retained until marker publication.
struct LoweredApplication {
    current_bundles: Vec<Option<AcceptedSchemaRevisionBundle>>,
    candidates: Vec<CandidateSchemaRevision>,
    pending: Option<PendingGeneratedRowLocalConstraint>,
}

/// Issue the current proposal-application target from recovered authority.
pub(in crate::db) fn schema_application_target<C: CanisterKind>(
    db: &Db<C>,
) -> Result<SchemaApplicationTarget, InternalError> {
    ensure_recovery_admitted(db)?;
    let incarnation = database_incarnation_id()?;
    let mut stores = db.with_store_registry(|registry| {
        registry
            .iter()
            .map(|(path, handle)| StoreApplicationAuthority { path, handle })
            .collect::<Vec<_>>()
    });
    icydb_schema::compact_sort_unstable_by(&mut stores, |left, right| left.path.cmp(right.path));

    let database_identity = derive_database_identity(incarnation.to_bytes());
    let mut accepted_heads = Vec::with_capacity(stores.len());
    let mut application_stores = Vec::with_capacity(stores.len());
    for store in &stores {
        let root = store
            .handle
            .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_root)?
            .map(|selection| AcceptedStoreHead {
                revision: selection.root().revision().get(),
                fingerprint: selection.root().fingerprint().as_bytes(),
            });
        accepted_heads.push((store.path, root));
        application_stores.push(SchemaApplicationStore {
            path: store.path.to_string(),
            identity: derive_store_identity(database_identity, store),
        });
    }

    Ok(SchemaApplicationTarget {
        database_identity,
        accepted_head: derive_accepted_head(accepted_heads.as_slice()),
        stores: application_stores,
    })
}

/// Load one durable schema-application receipt by its exact idempotency
/// identity.
pub(in crate::db) fn schema_application_receipt<C: CanisterKind>(
    db: &Db<C>,
    database_identity: TargetDatabaseIdentity,
    submission_key: &SchemaSubmissionKey,
) -> Result<Option<SchemaChangeReceipt>, InternalError> {
    ensure_recovery_admitted(db)?;
    with_schema_application_store(|store| {
        store
            .load(database_identity, submission_key)
            .map(|record| record.map(|record| record.receipt().clone()))
    })
}

fn exact_schema_application_receipt(
    proposal: &SchemaProposal,
    proposal_digest: SchemaProposalDigest,
) -> Result<Option<SchemaChangeReceipt>, InternalError> {
    let Some(record) = with_schema_application_store(|store| {
        store.load(proposal.target_database(), proposal.submission_key())
    })?
    else {
        return Ok(None);
    };
    let receipt = record.receipt();
    if !receipt.is_exact_submission(
        proposal.target_database(),
        proposal.submission_key(),
        proposal_digest,
        proposal.expected_head(),
    ) {
        return Err(InternalError::schema_application_conflict());
    }
    Ok(Some(receipt.clone()))
}

/// Advance one durable pending schema application by at most one canonical
/// validation step.
pub(in crate::db) fn continue_schema_application<C: CanisterKind>(
    db: &Db<C>,
    job_id: SchemaChangeJobId,
    acknowledged_receipt: Option<u64>,
) -> Result<SchemaChangeProgress, InternalError> {
    ensure_recovery_admitted(db)?;
    ensure_schema_migration_ready_for_schema_changes()?;
    let record = with_schema_application_store(|store| store.load_job(job_id))?
        .ok_or_else(InternalError::schema_application_conflict)?;
    let target = schema_application_target(db)?;
    if target.database_identity() != record.receipt().database_identity() {
        return Err(InternalError::schema_application_conflict());
    }
    match record.receipt().outcome() {
        SchemaChangeOutcome::Pending { job, .. } if job.id() == job_id => {}
        SchemaChangeOutcome::Applied { .. } => {
            return Ok(SchemaChangeProgress::new(
                record.receipt().clone(),
                SchemaChangeProgressStatus::Applied,
            ));
        }
        SchemaChangeOutcome::Aborted { .. } => {
            return Ok(SchemaChangeProgress::new(
                record.receipt().clone(),
                SchemaChangeProgressStatus::Aborted,
            ));
        }
        _ => return Err(InternalError::store_corruption()),
    }
    let [activation] = record.activations() else {
        return Err(InternalError::store_corruption());
    };
    require_pending_source_lineage(
        load_entity_source_lineage_catalog()?.as_ref(),
        activation,
        record.pending_source_lineage(),
    )?;
    let authorities = application_authorities(db);
    let authority = authorities
        .iter()
        .find(|authority| {
            derive_store_identity(target.database_identity(), authority) == activation.store()
        })
        .ok_or_else(InternalError::store_corruption)?;
    let entity_tag = EntityTag::new(activation.entity_tag());
    let constraint_id = ConstraintId::new(activation.constraint_id())
        .ok_or_else(InternalError::store_corruption)?;
    let bundle = authority
        .handle
        .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
        .ok_or_else(InternalError::store_corruption)?;
    if bundle.store_path() != authority.path {
        return Err(InternalError::store_corruption());
    }
    let snapshot = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;

    let accepted = snapshot
        .constraint_catalog()
        .constraints()
        .iter()
        .any(|constraint| {
            constraint.id() == constraint_id
                && constraint.origin() == ConstraintOrigin::Generated
                && matches!(
                    constraint.kind(),
                    crate::db::schema::AcceptedConstraintKind::Check { .. }
                        | crate::db::schema::AcceptedConstraintKind::TargetedRule { .. }
                )
        });
    let pending = snapshot.constraint_catalog().activation(constraint_id);
    if accepted && pending.is_none() {
        return finalize_schema_application(db, &record, SchemaChangeProgressStatus::Applied);
    }
    let pending = pending.ok_or_else(InternalError::store_corruption)?;
    if pending.origin() != ConstraintOrigin::Generated
        || !matches!(
            pending.kind(),
            ConstraintActivationKind::Check { .. } | ConstraintActivationKind::TargetedRule { .. }
        )
    {
        return Err(InternalError::store_corruption());
    }
    let entity_path = snapshot.entity_path().to_string();
    let progress = advance_accepted_row_local_constraint_activation(
        authority.handle,
        authority.path,
        entity_tag,
        entity_path.as_str(),
        constraint_id,
        acknowledged_receipt,
    )?;
    let status = schema_change_progress_status(snapshot, entity_tag, constraint_id, progress)?;
    if status == SchemaChangeProgressStatus::Applied {
        finalize_schema_application(db, &record, status)
    } else {
        Ok(SchemaChangeProgress::new(record.receipt().clone(), status))
    }
}

/// Abort one pending generated row-local application.
///
/// A retained finding page must be acknowledged by exact sequence before the
/// activation and its validation job can be retired. Terminal outcomes replay
/// without mutating accepted authority.
pub(in crate::db) fn abort_schema_application<C: CanisterKind>(
    db: &Db<C>,
    job_id: SchemaChangeJobId,
    acknowledged_receipt: Option<u64>,
) -> Result<SchemaChangeProgress, InternalError> {
    ensure_recovery_admitted(db)?;
    ensure_schema_migration_ready_for_schema_changes()?;
    let record = with_schema_application_store(|store| store.load_job(job_id))?
        .ok_or_else(InternalError::schema_application_conflict)?;
    let target = schema_application_target(db)?;
    if target.database_identity() != record.receipt().database_identity() {
        return Err(InternalError::schema_application_conflict());
    }
    match record.receipt().outcome() {
        SchemaChangeOutcome::Applied { .. } => {
            return Ok(SchemaChangeProgress::new(
                record.receipt().clone(),
                SchemaChangeProgressStatus::Applied,
            ));
        }
        SchemaChangeOutcome::Aborted { .. } => {
            return Ok(SchemaChangeProgress::new(
                record.receipt().clone(),
                SchemaChangeProgressStatus::Aborted,
            ));
        }
        SchemaChangeOutcome::Pending { job, .. } if job.id() == job_id => {}
        SchemaChangeOutcome::NoOp { .. } | SchemaChangeOutcome::Pending { .. } => {
            return Err(InternalError::store_corruption());
        }
    }

    let authorities = application_authorities(db);
    let [activation] = record.activations() else {
        return Err(InternalError::store_corruption());
    };
    require_pending_source_lineage(
        load_entity_source_lineage_catalog()?.as_ref(),
        activation,
        record.pending_source_lineage(),
    )?;
    let abort = prepare_pending_application_abort(
        target.database_identity(),
        &record,
        authorities.as_slice(),
        acknowledged_receipt,
    )?;
    let candidate = aborted_generated_row_local_candidate(
        &abort.current,
        abort.entity_tag,
        abort.constraint_id,
    )?;
    let accepted_head =
        accepted_head_after_candidates(authorities.as_slice(), std::slice::from_ref(&candidate))?;
    let receipt = SchemaChangeReceipt::new(
        record.receipt().database_identity(),
        record.receipt().submission_key().clone(),
        record.receipt().proposal_digest(),
        record.receipt().prior_head().clone(),
        SchemaChangeOutcome::Aborted { accepted_head },
    )?;
    let terminal = SchemaApplicationRecord::new(receipt.clone(), Vec::new())?;
    let operation = SchemaApplicationRecordOp::replace(&record, &terminal)?;
    if abort.remove_validation_job {
        publish_generated_row_local_abort_with_application_record(
            abort.authority.path,
            abort.authority.handle,
            abort.current.revision(),
            &candidate,
            abort.entity_tag,
            abort.constraint_id,
            operation,
        )?;
    } else {
        publish_accepted_schema_candidates_with_application_record(
            vec![AcceptedSchemaPublication::new(
                abort.authority.path,
                abort.authority.handle,
                abort.current.revision(),
                &candidate,
            )],
            operation,
        )?;
    }
    Ok(SchemaChangeProgress::new(
        receipt,
        SchemaChangeProgressStatus::Aborted,
    ))
}

fn prepare_pending_application_abort(
    database_identity: TargetDatabaseIdentity,
    record: &SchemaApplicationRecord,
    authorities: &[StoreApplicationAuthority],
    acknowledged_receipt: Option<u64>,
) -> Result<PendingApplicationAbort, InternalError> {
    let [activation] = record.activations() else {
        return Err(InternalError::store_corruption());
    };
    let authority = authorities
        .iter()
        .copied()
        .find(|authority| derive_store_identity(database_identity, authority) == activation.store())
        .ok_or_else(InternalError::store_corruption)?;
    let entity_tag = EntityTag::new(activation.entity_tag());
    let constraint_id = ConstraintId::new(activation.constraint_id())
        .ok_or_else(InternalError::store_corruption)?;
    let current = authority
        .handle
        .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
        .ok_or_else(InternalError::store_corruption)?;
    if current.store_path() != authority.path {
        return Err(InternalError::store_corruption());
    }
    let pending = current
        .entity_snapshots()
        .get(&entity_tag)
        .and_then(|snapshot| snapshot.constraint_catalog().activation(constraint_id))
        .filter(|pending| {
            pending.origin() == ConstraintOrigin::Generated
                && matches!(
                    pending.kind(),
                    ConstraintActivationKind::Check { .. }
                        | ConstraintActivationKind::TargetedRule { .. }
                )
        })
        .ok_or_else(InternalError::store_corruption)?;
    let remove_validation_job = pending_generated_row_local_job_retirement(
        authority,
        entity_tag,
        constraint_id,
        pending.state(),
        acknowledged_receipt,
    )?;
    Ok(PendingApplicationAbort {
        authority,
        current,
        entity_tag,
        constraint_id,
        remove_validation_job,
    })
}

fn pending_generated_row_local_job_retirement(
    authority: StoreApplicationAuthority,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    state: ConstraintActivationState,
    acknowledged_receipt: Option<u64>,
) -> Result<bool, InternalError> {
    let job = authority
        .handle
        .with_schema(|store| store.constraint_validation_job(entity_tag, constraint_id))?;
    match state {
        ConstraintActivationState::EnforcingNewWrites => {
            if acknowledged_receipt.is_some() || job.is_some() {
                return Err(InternalError::schema_application_conflict());
            }
            Ok(false)
        }
        ConstraintActivationState::Validating => {
            let mut job = job.ok_or_else(InternalError::store_corruption)?;
            if !job.acknowledge_receipt(acknowledged_receipt) {
                return Err(InternalError::schema_application_conflict());
            }
            Ok(true)
        }
    }
}

fn aborted_generated_row_local_candidate(
    current: &AcceptedSchemaRevisionBundle,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
) -> Result<CandidateSchemaRevision, InternalError> {
    let snapshot = current
        .entity_snapshots()
        .get(&entity_tag)
        .cloned()
        .ok_or_else(InternalError::store_corruption)?;
    let _activation = snapshot
        .constraint_catalog()
        .activation(constraint_id)
        .filter(|activation| {
            activation.origin() == ConstraintOrigin::Generated
                && matches!(
                    activation.kind(),
                    ConstraintActivationKind::Check { .. }
                        | ConstraintActivationKind::TargetedRule { .. }
                )
        })
        .ok_or_else(InternalError::store_corruption)?;
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_aborted_activation(constraint_id)
        .map_err(|_| InternalError::store_invariant())?;
    let accepted_identity_remains = catalog
        .constraints()
        .iter()
        .any(|constraint| constraint.id() == constraint_id);
    let mut source_bindings = current.source_bindings().clone();
    if !accepted_identity_remains {
        source_bindings.remove_constraint_identity(entity_tag, constraint_id)?;
    }
    let revision = current
        .revision()
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    CandidateSchemaRevision::from_entity_snapshot(
        current,
        revision,
        entity_tag,
        snapshot.with_constraint_catalog(catalog),
        source_bindings,
    )
}

/// Apply one exact source-keyed schema proposal through catalog-native
/// accepted candidates and the durable application-receipt boundary.
pub(in crate::db) fn apply_schema<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
) -> Result<SchemaChangeReceipt, InternalError> {
    apply_schema_with_contract::<C, true>(db, proposal)
}

/// Apply one canonical generated proposal whose sealed facade proves that it
/// cannot request explicit removals.
pub(in crate::db) fn apply_generated_schema<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
) -> Result<SchemaChangeReceipt, InternalError> {
    if !proposal.removals().is_empty() {
        return Err(InternalError::store_invariant());
    }
    apply_schema_with_contract::<C, false>(db, proposal)
}

fn apply_schema_with_contract<C: CanisterKind, const ALLOW_REMOVALS: bool>(
    db: &Db<C>,
    proposal: &SchemaProposal,
) -> Result<SchemaChangeReceipt, InternalError> {
    ensure_recovery_admitted(db)?;
    ensure_schema_migration_ready_for_schema_changes()?;
    let proposal_digest = proposal
        .digest()
        .map_err(|_| InternalError::store_unsupported())?;
    if let Some(receipt) = exact_schema_application_receipt(proposal, proposal_digest)? {
        return Ok(receipt);
    }

    let target = schema_application_target(db)?;
    if target.database_identity() != proposal.target_database()
        || target.accepted_head() != proposal.expected_head()
    {
        return Err(InternalError::schema_application_conflict());
    }

    preflight_ordinary_source_application(db, proposal, &target)?;

    let authorities = application_authorities(db);
    let LoweredApplication {
        current_bundles,
        candidates,
        pending,
    } = lower_application_candidates::<ALLOW_REMOVALS>(&target, proposal, authorities.as_slice())?;
    require_pending_application_source_ownership(
        proposal,
        &authorities,
        &current_bundles,
        &candidates,
    )?;
    validate_database_identity_state_capacity(
        authorities.as_slice(),
        candidates.as_slice(),
        database_incarnation_id()?,
    )?;
    // Derive the actual publication head once. Pending activation alone needs
    // a separate future head for its receipt; lineage records the admitted head.
    let lineage_head =
        accepted_head_after_candidates(authorities.as_slice(), candidates.as_slice())?;
    let outcome_head = if let Some(pending) = pending.as_ref() {
        let final_candidates =
            final_candidates_for_pending_row_local_constraint(&candidates, pending)?;
        accepted_head_after_candidates(authorities.as_slice(), final_candidates.as_slice())?
    } else if candidates.is_empty() {
        target.accepted_head().clone()
    } else {
        lineage_head.clone()
    };
    let outcome = if pending.is_some() {
        let job_id = derive_schema_change_job_id(
            target.database_identity(),
            proposal.submission_key(),
            proposal_digest,
            target.accepted_head(),
        )?;
        SchemaChangeOutcome::Pending {
            job: SchemaChangeJob::new(job_id),
            candidate_head: outcome_head,
        }
    } else if candidates.is_empty() {
        SchemaChangeOutcome::NoOp {
            accepted_head: outcome_head,
        }
    } else {
        SchemaChangeOutcome::Applied {
            accepted_head: outcome_head,
        }
    };
    let receipt = SchemaChangeReceipt::new(
        target.database_identity(),
        proposal.submission_key().clone(),
        proposal_digest,
        target.accepted_head().clone(),
        outcome,
    )?;
    let activations = match pending {
        Some(pending) => {
            let authority = authorities
                .iter()
                .find(|authority| authority.path == pending.proof.store_path)
                .ok_or_else(InternalError::store_invariant)?;
            vec![SchemaChangeActivation::new(
                derive_store_identity(target.database_identity(), authority),
                pending.proof.entity_tag.value(),
                pending.proof.constraint_id.get(),
            )?]
        }
        None => Vec::new(),
    };
    let record = SchemaApplicationRecord::new(receipt.clone(), activations)?;
    // This includes lineage and publication preparation, not candidate lowering.
    let _publication_span = ExecutionMetricsSpan::new(ExecutionMetricsPhase::Publication);
    let database_control = attach_ordinary_lineage_publication(
        proposal,
        target.accepted_head(),
        &lineage_head,
        authorities.as_slice(),
        &current_bundles,
        candidates.as_slice(),
        record,
    )?;
    let publications =
        application_publications(authorities.as_slice(), &current_bundles, &candidates)?;
    publish_accepted_schema_candidates_with_database_control(publications, database_control)?;
    Ok(receipt)
}

fn preflight_ordinary_source_application<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    target: &SchemaApplicationTarget,
) -> Result<(), InternalError> {
    if proposal.migration().is_none()
        || matches!(target.accepted_head(), ExpectedAcceptedHead::Empty)
    {
        return Ok(());
    }
    #[cfg(feature = "migration")]
    {
        if current_proposal_lineage_is_applied(db, proposal)? {
            return Ok(());
        }
        preflight_unpublished_schema_migration(target, proposal, db)?;
        // A valid adjacent plan awaits explicit migration commands; it is not
        // a terminal schema rejection. Reuse the active-migration gate so the
        // startup driver does not persist an Unsupported failure for this head.
        Err(InternalError::schema_migration(
            SchemaMigrationCode::MigrationInProgress,
        ))
    }
    #[cfg(not(feature = "migration"))]
    {
        let _ = db;
        Err(InternalError::store_unsupported())
    }
}

/// Execute one explicit source-migration operation against the exact deployed
/// generated proposal. Metadata adoption and bounded physical work share the
/// existing durable migration runner and accepted-head checks.
#[cfg(feature = "migration")]
pub(in crate::db) fn migrate_schema<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    command: SchemaMigrationCommand,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    ensure_recovery_admitted(db)?;
    // Commands and proposals must name the same predecessor, including retries.
    // Live freshness is checked at admission; lineage retains publication provenance.
    let expected_head = match &command {
        SchemaMigrationCommand::Adopt { expected_head, .. }
        | SchemaMigrationCommand::Advance { expected_head, .. }
        | SchemaMigrationCommand::Abort { expected_head, .. } => expected_head,
    };
    if proposal.expected_head() != expected_head {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::StaleAcceptedHead,
        ));
    }
    match command {
        SchemaMigrationCommand::Adopt {
            expected_database,
            expected_head,
        } => adopt_entity_source_lineage(db, proposal, expected_database, &expected_head),
        SchemaMigrationCommand::Advance {
            expected_database,
            expected_head,
            expected_plan,
        } => advance_metadata_schema_migration(
            db,
            proposal,
            expected_database,
            &expected_head,
            expected_plan,
        ),
        SchemaMigrationCommand::Abort {
            expected_database,
            expected_head,
            expected_plan,
        } => {
            let plan = proposal.migration().ok_or_else(|| {
                InternalError::schema_migration(SchemaMigrationCode::MissingMigration)
            })?;
            if proposal.target_database() != expected_database || plan.digest() != expected_plan {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::PlanChanged,
                ));
            }
            if let Some(record) = exact_active_migration_record(
                proposal,
                expected_database,
                &expected_head,
                expected_plan,
            )? {
                let target = schema_application_target(db)?;
                validate_active_migration_target(&record, &target)?;
                if record.phase() == PersistedSchemaMigrationPhase::Applied
                    || record.phase() == PersistedSchemaMigrationPhase::Aborted
                {
                    return active_migration_status(proposal, &target, &record);
                }
                if !record.phase().abortable() {
                    return Err(InternalError::schema_migration(
                        SchemaMigrationCode::AbortTooLate,
                    ));
                }
                let planned = recompile_active_physical_migration(db, proposal, &record)?;
                let authorities = application_authorities(db);
                let mut store_identities = BTreeMap::new();
                for authority in &authorities {
                    store_identities.insert(
                        authority.path,
                        derive_store_identity(record.database_identity(), authority),
                    );
                }
                let (progress, exhausted) = cleanup_migration_staging_page(
                    db,
                    &planned,
                    record.progress(),
                    &store_identities,
                )?;
                let phase = if exhausted {
                    PersistedSchemaMigrationPhase::Aborted
                } else {
                    record.phase()
                };
                let advanced = record.transition(phase, progress)?;
                let operation = SchemaMigrationRecordOp::replace(&record, &advanced)?;
                publish_accepted_schema_candidates_with_database_control(
                    Vec::new(),
                    vec![DatabaseControlOp::SchemaMigration(operation)],
                )?;
                return active_migration_status(proposal, &target, &advanced);
            }
            let target = exact_migration_target(db, expected_database, &expected_head)?;
            let status = schema_migration_status_for_target(db, proposal, &target)?;
            if status.phase() == SchemaMigrationPhase::Applied {
                Ok(status)
            } else {
                // No active nonterminal migration exists at this boundary, so
                // the request fails closed.
                Err(InternalError::schema_migration(
                    SchemaMigrationCode::MissingMigration,
                ))
            }
        }
    }
}

/// Return one bounded deployed-source migration status page.
#[cfg(feature = "migration")]
pub(in crate::db) fn schema_migration_status<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    request: &SchemaMigrationStatusRequest,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    ensure_recovery_admitted(db)?;
    if !request.validate() || request.cursor().is_some() {
        return Err(InternalError::cursor_invalid_continuation());
    }
    let target = schema_application_target(db)?;
    schema_migration_status_for_target(db, proposal, &target)
}

/// Keep generated schema application pending while an exact migration owns
/// publication. Prepared row admission is separate from generated readiness.
#[cfg(feature = "migration")]
pub(in crate::db) fn ensure_generated_schema_application_admitted<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
) -> Result<(), InternalError> {
    ensure_recovery_admitted(db)?;
    let Some(record) = load_schema_migration_record()? else {
        return Ok(());
    };
    if matches!(
        record.phase(),
        PersistedSchemaMigrationPhase::Applied | PersistedSchemaMigrationPhase::Aborted
    ) {
        return Ok(());
    }
    validate_active_migration_deployment(proposal, &record)?;
    let target = schema_application_target(db)?;
    validate_active_migration_target(&record, &target)?;
    // Every validated nonterminal phase waits for explicit controller commands.
    // Returning success for Prepared would make startup retry without progress.
    Err(InternalError::schema_migration(
        SchemaMigrationCode::MigrationInProgress,
    ))
}

#[cfg(feature = "migration")]
fn adopt_entity_source_lineage<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    expected_database: TargetDatabaseIdentity,
    expected_head: &ExpectedAcceptedHead,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    if proposal.migration().is_some() || proposal.target_database() != expected_database {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PlanChanged,
        ));
    }
    let proposal_digest = proposal
        .digest()
        .map_err(|_| InternalError::store_unsupported())?;
    let submission_key = migration_submission_key(None)?;
    if let Some(record) = load_exact_migration_record(
        expected_database,
        &submission_key,
        proposal_digest,
        expected_head,
    )? {
        let replay_target = exact_migration_replay_target(db, expected_database, &record)?;
        return schema_migration_status_for_target(db, proposal, &replay_target);
    }
    let target = exact_migration_target(db, expected_database, expected_head)?;

    let authorities = application_authorities(db);
    let current_bundles = load_current_application_bundles(authorities.as_slice())?;
    let stores = existing_proposal_stores(
        target.database_identity(),
        authorities.as_slice(),
        current_bundles.as_slice(),
    );
    let stored_before = load_entity_source_lineage_catalog()?;
    let before = stored_before.clone().unwrap_or_default();
    let planned = plan_entity_source_adoption(proposal, stores.as_slice(), &before)
        .map_err(schema_migration_planning_error)?;
    let after = lineage_after_planned(&before, planned.as_slice(), expected_head)?;
    let receipt = SchemaChangeReceipt::new(
        expected_database,
        submission_key,
        proposal_digest,
        expected_head.clone(),
        SchemaChangeOutcome::NoOp {
            accepted_head: expected_head.clone(),
        },
    )?;
    let record = SchemaApplicationRecord::new(receipt, Vec::new())?;
    let operation = SchemaApplicationRecordOp::insert(&record)?;
    let lineage = EntitySourceLineageCatalogOp::replace(stored_before.as_ref(), &after)?;
    publish_accepted_schema_candidates_with_database_control(
        Vec::new(),
        vec![
            DatabaseControlOp::SchemaApplication(operation),
            DatabaseControlOp::EntitySourceLineage(lineage),
        ],
    )?;
    schema_migration_status_for_target(db, proposal, &target)
}

#[cfg(feature = "migration")]
fn advance_metadata_schema_migration<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    expected_database: TargetDatabaseIdentity,
    expected_head: &ExpectedAcceptedHead,
    expected_plan: SchemaMigrationPlanDigest,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    let plan = proposal
        .migration()
        .ok_or_else(|| InternalError::schema_migration(SchemaMigrationCode::MissingMigration))?;
    if proposal.target_database() != expected_database || plan.digest() != expected_plan {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PlanChanged,
        ));
    }
    let proposal_digest = proposal
        .digest()
        .map_err(|_| InternalError::store_unsupported())?;
    if let Some(record) =
        exact_active_migration_record(proposal, expected_database, expected_head, expected_plan)?
    {
        let target = schema_application_target(db)?;
        validate_active_migration_target(&record, &target)?;
        return advance_active_schema_migration(db, proposal, &target, &record);
    }

    let submission_key = migration_submission_key(Some(expected_plan))?;
    if let Some(record) = load_exact_migration_record(
        expected_database,
        &submission_key,
        proposal_digest,
        expected_head,
    )? {
        let replay_target = exact_migration_replay_target(db, expected_database, &record)?;
        return schema_migration_status_for_target(db, proposal, &replay_target);
    }
    exact_migration_target(db, expected_database, expected_head)?;

    let authorities = application_authorities(db);
    let current_bundles = load_current_application_bundles(authorities.as_slice())?;
    let stores = existing_proposal_stores(
        expected_database,
        authorities.as_slice(),
        current_bundles.as_slice(),
    );
    let before = load_entity_source_lineage_catalog()?
        .ok_or_else(|| InternalError::schema_migration(SchemaMigrationCode::Unadopted))?;
    let planned = plan_schema_migration(proposal, stores.as_slice(), &before)
        .map_err(schema_migration_planning_error)?;
    let mut candidates = planned.candidates().to_vec();
    let pending = if planned.requires_physical_validation() {
        // The ordinary online preflight intentionally rejects non-empty field
        // removal and direct index-generation replacement. The offline
        // migration validator owns those same historical proofs against its
        // unpublished candidate instead.
        preflight_new_entity_domains(
            authorities.as_slice(),
            current_bundles.as_slice(),
            &candidates,
        )?;
        None
    } else {
        preflight_existing_application(
            authorities.as_slice(),
            current_bundles.as_slice(),
            &mut candidates,
        )?
    };
    if pending.is_some() {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::MigrationInProgress,
        ));
    }
    if candidates.is_empty() || planned.lineage().is_empty() {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::EmptyEntityVersionBump,
        ));
    }
    validate_database_identity_state_capacity(
        authorities.as_slice(),
        candidates.as_slice(),
        database_incarnation_id()?,
    )?;
    let accepted_head =
        accepted_head_after_candidates(authorities.as_slice(), candidates.as_slice())?;
    if planned.requires_physical_validation() {
        ensure_physical_migration_stores_are_journaled(db, &planned)?;
        let record = prepared_physical_schema_migration(
            proposal,
            &planned,
            candidates.as_slice(),
            expected_database,
            expected_head,
            &accepted_head,
            proposal_digest,
            expected_plan,
            stores.as_slice(),
        )?;
        // A terminal record remains the authority for its private journal
        // effects. Drain at most one existing convergence page per request
        // before changing that authority; the new plan stays Idle meanwhile.
        let prior = load_schema_migration_record()?;
        if prior.is_some()
            && continue_recovery_with_failure_authority(db)
                .map_err(StartupRecoveryFailure::into_error)?
                == RecoveryProgress::Pending
        {
            let target = schema_application_target(db)?;
            return schema_migration_status_for_target(db, proposal, &target);
        }
        let operation = match prior.as_ref() {
            Some(prior) => SchemaMigrationRecordOp::replace(prior, &record)?,
            None => SchemaMigrationRecordOp::insert(&record)?,
        };
        publish_accepted_schema_candidates_with_database_control(
            Vec::new(),
            vec![DatabaseControlOp::SchemaMigration(operation)],
        )?;
        let target = schema_application_target(db)?;
        validate_active_migration_target(&record, &target)?;
        return active_migration_status(proposal, &target, &record);
    }
    let after = lineage_after_planned(&before, planned.lineage(), &accepted_head)?;
    let receipt = SchemaChangeReceipt::new(
        expected_database,
        submission_key,
        proposal_digest,
        expected_head.clone(),
        SchemaChangeOutcome::Applied { accepted_head },
    )?;
    let record = SchemaApplicationRecord::new(receipt, Vec::new())?;
    let operation = SchemaApplicationRecordOp::insert(&record)?;
    let lineage = EntitySourceLineageCatalogOp::replace(Some(&before), &after)?;
    let publications = application_publications(
        authorities.as_slice(),
        current_bundles.as_slice(),
        candidates.as_slice(),
    )?;
    publish_accepted_schema_candidates_with_database_control(
        publications,
        vec![
            DatabaseControlOp::SchemaApplication(operation),
            DatabaseControlOp::EntitySourceLineage(lineage),
        ],
    )?;
    let applied_target = schema_application_target(db)?;
    schema_migration_status_for_target(db, proposal, &applied_target)
}

#[cfg(feature = "migration")]
fn ensure_physical_migration_stores_are_journaled<C: CanisterKind>(
    db: &Db<C>,
    planned: &crate::db::schema::migration_planner::PlannedSchemaMigration,
) -> Result<(), InternalError> {
    for program in planned.programs() {
        let store = db.store_handle(program.store_path())?;
        if store.storage_capabilities().recovery()
            != StoreRecoveryCapability::StableBasePlusJournalReplay
        {
            return Err(InternalError::schema_migration(
                SchemaMigrationCode::PhysicalRunnerMissing,
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "migration")]
#[expect(
    clippy::too_many_arguments,
    reason = "migration preparation binds every immutable deployment and candidate identity"
)]
fn prepared_physical_schema_migration(
    proposal: &SchemaProposal,
    planned: &crate::db::schema::migration_planner::PlannedSchemaMigration,
    candidates: &[CandidateSchemaRevision],
    database_identity: TargetDatabaseIdentity,
    accepted_before: &ExpectedAcceptedHead,
    candidate_head: &ExpectedAcceptedHead,
    submission_digest: SchemaProposalDigest,
    plan_digest: SchemaMigrationPlanDigest,
    stores: &[ExistingProposalStore<'_>],
) -> Result<SchemaMigrationRecord, InternalError> {
    let plan = proposal
        .migration()
        .ok_or_else(|| InternalError::schema_migration(SchemaMigrationCode::MissingMigration))?;
    let transitions = plan
        .transitions()
        .iter()
        .map(|transition| {
            PersistedSchemaMigrationTransition::try_new(
                transition.entity().clone(),
                transition.from().get(),
                transition
                    .from()
                    .get()
                    .checked_add(1)
                    .ok_or_else(InternalError::store_invariant)?,
            )
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    let entities = planned
        .lineage()
        .iter()
        .map(|entity| {
            PersistedSchemaMigrationEntity::try_new(
                entity.store(),
                entity.entity(),
                entity.digest(),
            )
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    let mut staged_indexes = Vec::new();
    for candidate in candidates {
        let store = stores
            .iter()
            .find(|store| store.path == candidate.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        for (entity, snapshot) in candidate.bundle().entity_snapshots() {
            // Fresh entities have empty initial index generations, not
            // historical domains to stage, rebuild or clean up on abort.
            let Some(before) = store.bundle.entity_snapshots.get(entity) else {
                continue;
            };
            for index in snapshot
                .indexes()
                .iter()
                .filter(|index| {
                    before
                        .indexes()
                        .iter()
                        .find(|old| old.schema_id() == index.schema_id())
                        .is_none_or(|old| old.physical_generation() != index.physical_generation())
                })
                .chain(snapshot.candidate_indexes())
            {
                staged_indexes.push(PersistedSchemaMigrationIndex::try_new(
                    store.identity,
                    *entity,
                    u64::from(index.schema_id().get()),
                    index.physical_generation(),
                )?);
            }
        }
    }
    icydb_schema::compact_sort_unstable_by(&mut staged_indexes, Ord::cmp);
    staged_indexes.dedup();
    SchemaMigrationRecord::prepared(
        database_identity,
        accepted_before.clone(),
        candidate_head.clone(),
        submission_digest,
        plan_digest,
        transitions,
        entities,
        staged_indexes,
    )
}

#[cfg(feature = "migration")]
fn advance_active_schema_migration<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    target: &SchemaApplicationTarget,
    record: &SchemaMigrationRecord,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    match record.phase() {
        PersistedSchemaMigrationPhase::Prepared => {
            let validating = record.transition(
                PersistedSchemaMigrationPhase::Validating,
                record.progress().clone(),
            )?;
            publish_migration_record_replacement(record, &validating)?;
            active_migration_status(proposal, target, &validating)
        }
        PersistedSchemaMigrationPhase::Validating => {
            let planned = recompile_active_physical_migration(db, proposal, record)?;
            let page =
                validate_migration_page(db, &planned, record.progress(), record.plan_digest())?;
            let (progress, staged_entries, exhausted) = page.into_parts();
            let phase = if progress.findings().is_empty() {
                if exhausted {
                    PersistedSchemaMigrationPhase::ReadyToRewrite
                } else {
                    PersistedSchemaMigrationPhase::Validating
                }
            } else {
                PersistedSchemaMigrationPhase::Rejected
            };
            if progress.findings().is_empty() {
                // Candidate generations are planner-invisible. Staging them
                // before the cursor marker makes retry idempotent and ensures
                // durable progress never names absent physical proof.
                stage_migration_index_entries(staged_entries)?;
            }
            let advanced = record.transition(phase, progress)?;
            publish_migration_record_replacement(record, &advanced)?;
            active_migration_status(proposal, target, &advanced)
        }
        PersistedSchemaMigrationPhase::ReadyToRewrite => {
            let progress = record.progress().begin_row_phase()?;
            let rewriting =
                record.transition(PersistedSchemaMigrationPhase::RewritingRows, progress)?;
            publish_migration_record_replacement(record, &rewriting)?;
            active_migration_status(proposal, target, &rewriting)
        }
        PersistedSchemaMigrationPhase::RewritingRows => {
            let planned = recompile_active_physical_migration(db, proposal, record)?;
            let page =
                rewrite_migration_page(db, &planned, record.progress(), record.plan_digest())?;
            let (progress, effects, exhausted) = page.into_parts();
            if progress.rows_rewritten() > progress.rows_validated()
                || (exhausted && progress.rows_rewritten() != progress.rows_validated())
            {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::ProgressCorrupt,
                ));
            }
            // Commit completion accounting and the fresh validation cursor with
            // the last page's effects so recovery cannot expose a partial handoff.
            let (phase, progress) = if exhausted {
                let rebuilt = migration_derived_domain_count(db, &planned)?;
                (
                    PersistedSchemaMigrationPhase::FinalValidation,
                    progress
                        .begin_row_phase()?
                        .with_index_progress(None, rebuilt)?,
                )
            } else {
                (PersistedSchemaMigrationPhase::RewritingRows, progress)
            };
            let advanced = record.transition(phase, progress)?;
            let operation = SchemaMigrationRecordOp::replace(record, &advanced)?;
            publish_migration_rewrite_page(effects, &operation)?;
            active_migration_status(proposal, target, &advanced)
        }
        PersistedSchemaMigrationPhase::FinalValidation => {
            let planned = recompile_active_physical_migration(db, proposal, record)?;
            let page = final_validate_migration_page(db, &planned, record.progress())?;
            let (progress, exhausted) = page.into_parts();
            let phase = if exhausted {
                PersistedSchemaMigrationPhase::Publishing
            } else {
                PersistedSchemaMigrationPhase::FinalValidation
            };
            let advanced = record.transition(phase, progress)?;
            publish_migration_record_replacement(record, &advanced)?;
            active_migration_status(proposal, target, &advanced)
        }
        PersistedSchemaMigrationPhase::Publishing => {
            publish_completed_physical_migration(db, proposal, record)
        }
        PersistedSchemaMigrationPhase::Rejected
        | PersistedSchemaMigrationPhase::Applied
        | PersistedSchemaMigrationPhase::Aborted => {
            active_migration_status(proposal, target, record)
        }
    }
}

#[cfg(feature = "migration")]
fn recompile_active_physical_migration<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    record: &SchemaMigrationRecord,
) -> Result<crate::db::schema::migration_planner::PlannedSchemaMigration, InternalError> {
    let authorities = application_authorities(db);
    let current_bundles = load_current_application_bundles(authorities.as_slice())?;
    let stores = existing_proposal_stores(
        record.database_identity(),
        authorities.as_slice(),
        current_bundles.as_slice(),
    );
    let lineage = load_entity_source_lineage_catalog()?
        .ok_or_else(|| InternalError::schema_migration(SchemaMigrationCode::Unadopted))?;
    let planned = plan_schema_migration(proposal, stores.as_slice(), &lineage)
        .map_err(schema_migration_planning_error)?;
    if !planned.requires_physical_validation() {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PlanChanged,
        ));
    }
    let candidate_head =
        accepted_head_after_candidates(authorities.as_slice(), planned.candidates())?;
    if &candidate_head != record.candidate_head() {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::CandidateMismatch,
        ));
    }
    Ok(planned)
}

#[cfg(feature = "migration")]
fn publish_completed_physical_migration<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    record: &SchemaMigrationRecord,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    let target = schema_application_target(db)?;
    validate_active_migration_target(record, &target)?;
    let planned = recompile_active_physical_migration(db, proposal, record)?;
    let authorities = application_authorities(db);
    let current_bundles = load_current_application_bundles(authorities.as_slice())?;
    let candidate_head =
        accepted_head_after_candidates(authorities.as_slice(), planned.candidates())?;
    if &candidate_head != record.candidate_head() {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PublicationRaceLost,
        ));
    }
    let lineage_before = load_entity_source_lineage_catalog()?
        .ok_or_else(|| InternalError::schema_migration(SchemaMigrationCode::Unadopted))?;
    let lineage_after =
        lineage_after_planned(&lineage_before, planned.lineage(), record.candidate_head())?;
    let receipt = SchemaChangeReceipt::new(
        record.database_identity(),
        migration_submission_key(Some(record.plan_digest()))?,
        record.submission_digest(),
        record.accepted_before().clone(),
        SchemaChangeOutcome::Applied {
            accepted_head: record.candidate_head().clone(),
        },
    )?;
    let application = SchemaApplicationRecord::new(receipt, Vec::new())?;
    let application = SchemaApplicationRecordOp::insert(&application)?;
    let lineage = EntitySourceLineageCatalogOp::replace(Some(&lineage_before), &lineage_after)?;
    let applied = record.transition(
        PersistedSchemaMigrationPhase::Applied,
        record.progress().clone(),
    )?;
    let migration = SchemaMigrationRecordOp::replace(record, &applied)?;
    let publications = application_publications(
        authorities.as_slice(),
        current_bundles.as_slice(),
        planned.candidates(),
    )?;
    publish_accepted_schema_candidates_with_database_control(
        publications,
        vec![
            DatabaseControlOp::SchemaApplication(application),
            DatabaseControlOp::EntitySourceLineage(lineage),
            DatabaseControlOp::SchemaMigration(migration),
        ],
    )?;
    db.mark_all_registered_index_stores_ready()?;
    let applied_target = schema_application_target(db)?;
    active_migration_status(proposal, &applied_target, &applied)
}

#[cfg(feature = "migration")]
fn publish_migration_record_replacement(
    before: &SchemaMigrationRecord,
    after: &SchemaMigrationRecord,
) -> Result<(), InternalError> {
    let operation = SchemaMigrationRecordOp::replace(before, after)?;
    publish_accepted_schema_candidates_with_database_control(
        Vec::new(),
        vec![DatabaseControlOp::SchemaMigration(operation)],
    )
}

// Exact replay returns before lowering. A new source application cannot
// supersede a pending entity or its shared type catalogs before the original
// activation has published its captured lineage and terminal receipt.
fn require_pending_application_source_ownership(
    proposal: &SchemaProposal,
    authorities: &[StoreApplicationAuthority],
    current: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<(), InternalError> {
    let pending = with_schema_application_store(|store| store.pending_activations())?;
    for activation in pending {
        let authority = authorities
            .iter()
            .find(|authority| {
                derive_store_identity(proposal.target_database(), authority) == activation.store()
            })
            .ok_or_else(InternalError::store_unsupported)?;
        let before = current
            .iter()
            .flatten()
            .find(|bundle| bundle.store_path() == authority.path)
            .ok_or_else(InternalError::store_corruption)?;
        let tag = EntityTag::new(activation.entity_tag());
        let before_entity = before
            .entity_snapshots()
            .get(&tag)
            .ok_or_else(InternalError::store_corruption)?;
        if proposal.assignments().iter().any(|assignment| {
            assignment.store() == activation.store()
                && before.source_bindings().entity(assignment.entity()) == Some(tag)
        }) {
            return Err(InternalError::schema_application_conflict());
        }
        if let Some(candidate) = candidates
            .iter()
            .find(|candidate| candidate.store_path() == authority.path)
        {
            let after = candidate.bundle();
            if Some(before_entity) != after.entity_snapshots().get(&tag)
                || before.enum_catalog() != after.enum_catalog()
                || before.composite_catalog() != after.composite_catalog()
            {
                return Err(InternalError::schema_application_conflict());
            }
        }
    }
    Ok(())
}

fn attach_ordinary_lineage_publication(
    proposal: &SchemaProposal,
    prior_head: &ExpectedAcceptedHead,
    accepted_head: &ExpectedAcceptedHead,
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
    record: SchemaApplicationRecord,
) -> Result<Vec<DatabaseControlOp>, InternalError> {
    let stored_before = load_entity_source_lineage_catalog()?;
    #[cfg(feature = "migration")]
    let planned = plan_new_entity_source_lineage(
        proposal,
        |source| {
            current_bundles
                .iter()
                .flatten()
                .any(|bundle| bundle.source_bindings().entity(source).is_some())
        },
        candidates,
    )
    .map_err(schema_migration_planning_error)?;
    #[cfg(feature = "migration")]
    if !planned.is_empty() && !matches!(prior_head, ExpectedAcceptedHead::Empty) {
        // Creation may initialise its own lineage, never implicitly adopt an
        // existing entity from a feature-less database or a partial adoption.
        let before = stored_before
            .as_ref()
            .ok_or_else(|| InternalError::schema_migration(SchemaMigrationCode::Unadopted))?;
        for bundle in current_bundles.iter().flatten() {
            let authority = authorities
                .iter()
                .find(|authority| authority.path == bundle.store_path())
                .ok_or_else(InternalError::store_invariant)?;
            let store = derive_store_identity(proposal.target_database(), authority);
            for tag in bundle.entity_snapshots().keys() {
                if !before.get(store, *tag).is_some_and(|entry| {
                    matches!(
                        entry.state(),
                        AcceptedEntitySourceLineageState::Adopted { .. }
                    )
                }) {
                    return Err(InternalError::schema_migration(
                        SchemaMigrationCode::Unadopted,
                    ));
                }
            }
        }
    }
    #[cfg(feature = "migration")]
    let needs_lineage = !planned.is_empty() || stored_before.is_some();
    // Disabling authoring must not strand adopted lineage from a prior binary.
    #[cfg(not(feature = "migration"))]
    let needs_lineage = stored_before.is_some();
    if !needs_lineage {
        return Ok(vec![DatabaseControlOp::SchemaApplication(
            SchemaApplicationRecordOp::insert(&record)?,
        )]);
    }
    let before = stored_before.clone().unwrap_or_default();
    #[cfg(feature = "migration")]
    let after = lineage_after_planned(&before, planned.as_slice(), accepted_head)?;
    #[cfg(not(feature = "migration"))]
    let after = {
        let _ = (prior_head, authorities, candidates);
        before.clone()
    };
    // Only this entity waits for its deferred validation. Other completed
    // source edits and the retained binding publish in this same marker.
    let (after, pending_lineage) = prepare_ordinary_source_lineage(
        after,
        proposal,
        current_bundles,
        accepted_head,
        record.activations().first(),
    )?;
    let record = record.with_pending_source_lineage(pending_lineage)?;
    let mut operations = vec![DatabaseControlOp::SchemaApplication(
        SchemaApplicationRecordOp::insert(&record)?,
    )];
    if before == after {
        return Ok(operations);
    }
    operations.push(DatabaseControlOp::EntitySourceLineage(
        EntitySourceLineageCatalogOp::replace(stored_before.as_ref(), &after)?,
    ));
    Ok(operations)
}

#[cfg(feature = "migration")]
fn exact_migration_target<C: CanisterKind>(
    db: &Db<C>,
    expected_database: TargetDatabaseIdentity,
    expected_head: &ExpectedAcceptedHead,
) -> Result<SchemaApplicationTarget, InternalError> {
    let target = schema_application_target(db)?;
    if target.database_identity() != expected_database || target.accepted_head() != expected_head {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::StaleAcceptedHead,
        ));
    }
    Ok(target)
}

#[cfg(feature = "migration")]
fn exact_migration_replay_target<C: CanisterKind>(
    db: &Db<C>,
    expected_database: TargetDatabaseIdentity,
    record: &SchemaApplicationRecord,
) -> Result<SchemaApplicationTarget, InternalError> {
    let target = schema_application_target(db)?;
    if target.database_identity() != expected_database
        || target.accepted_head() != migration_record_accepted_head(record)?
    {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PlanChanged,
        ));
    }
    Ok(target)
}

#[cfg(feature = "migration")]
fn schema_migration_status_for_target<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    target: &SchemaApplicationTarget,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    if let Some(record) = load_schema_migration_record()?
        && (!record.phase().terminal()
            || proposal
                .digest()
                .map_err(|_| InternalError::store_unsupported())?
                == record.submission_digest())
    {
        validate_active_migration_deployment(proposal, &record)?;
        validate_active_migration_target(&record, target)?;
        return active_migration_status(proposal, target, &record);
    }
    let lineage = load_entity_source_lineage_catalog()?.unwrap_or_default();
    let plan_digest = proposal
        .migration()
        .map(icydb_schema::SchemaMigrationPlan::digest);
    let transitions = migration_transitions(proposal)?;
    let submission_key = migration_submission_key(plan_digest)?;
    let terminal = load_migration_record_for_status(target.database_identity(), &submission_key)?
        .map(|record| public_migration_receipt(&record, plan_digest))
        .transpose()?;
    let unadopted = lineage.entries().is_empty()
        || lineage
            .entries()
            .values()
            .any(|entry| matches!(entry.state(), AcceptedEntitySourceLineageState::Unadopted));
    let applied = current_proposal_lineage_is_applied(db, proposal)?;
    let phase = if unadopted {
        SchemaMigrationPhase::Unadopted
    } else if proposal.migration().is_none() {
        SchemaMigrationPhase::Adopted
    } else if applied {
        SchemaMigrationPhase::Applied
    } else {
        SchemaMigrationPhase::Idle
    };
    let terminal = terminal.filter(|receipt| {
        receipt.accepted_head() == target.accepted_head()
            && receipt.plan_digest() == plan_digest
            && matches!(
                phase,
                SchemaMigrationPhase::Adopted | SchemaMigrationPhase::Applied
            )
    });
    Ok(SchemaMigrationStatusPage::new(
        target.database_identity(),
        target.accepted_head().clone(),
        plan_digest,
        phase,
        transitions,
        0,
        0,
        0,
        Vec::new(),
        None,
        terminal,
    ))
}

#[cfg(feature = "migration")]
fn exact_active_migration_record(
    proposal: &SchemaProposal,
    expected_database: TargetDatabaseIdentity,
    expected_head: &ExpectedAcceptedHead,
    expected_plan: SchemaMigrationPlanDigest,
) -> Result<Option<SchemaMigrationRecord>, InternalError> {
    let Some(record) = load_schema_migration_record()? else {
        return Ok(None);
    };
    // Terminal retries retain their exact result, but a new submission must
    // reach ordinary target/planner admission and the journal-safe handoff.
    if record.phase().terminal()
        && (record.accepted_before() != expected_head
            || record.plan_digest() != expected_plan
            || proposal
                .digest()
                .map_err(|_| InternalError::store_unsupported())?
                != record.submission_digest())
    {
        return Ok(None);
    }
    if record.database_identity() != expected_database
        || record.accepted_before() != expected_head
        || record.plan_digest() != expected_plan
    {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PlanChanged,
        ));
    }
    validate_active_migration_deployment(proposal, &record)?;
    Ok(Some(record))
}

#[cfg(feature = "migration")]
fn validate_active_migration_deployment(
    proposal: &SchemaProposal,
    record: &SchemaMigrationRecord,
) -> Result<(), InternalError> {
    let plan = proposal
        .migration()
        .ok_or_else(|| InternalError::schema_migration(SchemaMigrationCode::PlanChanged))?;
    let proposal_digest = proposal
        .digest()
        .map_err(|_| InternalError::store_unsupported())?;
    if proposal.target_database() != record.database_identity()
        || plan.digest() != record.plan_digest()
        || proposal_digest != record.submission_digest()
    {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PlanChanged,
        ));
    }
    Ok(())
}

#[cfg(feature = "migration")]
fn validate_active_migration_target(
    record: &SchemaMigrationRecord,
    target: &SchemaApplicationTarget,
) -> Result<(), InternalError> {
    let expected_head = if record.phase() == PersistedSchemaMigrationPhase::Applied {
        record.candidate_head()
    } else {
        record.accepted_before()
    };
    if target.database_identity() != record.database_identity()
        || target.accepted_head() != expected_head
    {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PlanChanged,
        ));
    }
    Ok(())
}

#[cfg(feature = "migration")]
fn active_migration_status(
    proposal: &SchemaProposal,
    target: &SchemaApplicationTarget,
    record: &SchemaMigrationRecord,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    validate_active_migration_deployment(proposal, record)?;
    validate_active_migration_target(record, target)?;
    let transitions = record
        .transitions()
        .iter()
        .map(|transition| {
            SchemaMigrationEntityTransition::new(
                transition.entity().clone(),
                Some(transition.predecessor_version()),
                transition.target_version(),
            )
        })
        .collect();
    let findings = record
        .progress()
        .findings()
        .iter()
        .map(|finding| {
            let kind = match finding.kind() {
                PersistedSchemaMigrationFindingKind::Transform => {
                    SchemaMigrationFindingKind::Transform
                }
                PersistedSchemaMigrationFindingKind::UniqueIndex => {
                    SchemaMigrationFindingKind::UniqueIndex
                }
                PersistedSchemaMigrationFindingKind::Relation => {
                    SchemaMigrationFindingKind::Relation
                }
                PersistedSchemaMigrationFindingKind::Constraint => {
                    SchemaMigrationFindingKind::Constraint
                }
                PersistedSchemaMigrationFindingKind::ResourceLimit => {
                    SchemaMigrationFindingKind::ResourceLimit
                }
            };
            SchemaMigrationFinding::new(
                kind,
                finding.entity().value(),
                finding.primary_key().to_vec(),
                finding.source_field().map(crate::db::schema::FieldId::get),
                finding.target_field().map(crate::db::schema::FieldId::get),
                finding.transform_reason().map(|reason| match reason {
                    PersistedSchemaMigrationTransformReason::Overflow => {
                        SchemaMigrationTransformReason::Overflow
                    }
                    PersistedSchemaMigrationTransformReason::NegativeToUnsigned => {
                        SchemaMigrationTransformReason::NegativeToUnsigned
                    }
                    PersistedSchemaMigrationTransformReason::PrecisionLoss => {
                        SchemaMigrationTransformReason::PrecisionLoss
                    }
                    PersistedSchemaMigrationTransformReason::NullSource => {
                        SchemaMigrationTransformReason::NullSource
                    }
                    PersistedSchemaMigrationTransformReason::ValueContract => {
                        SchemaMigrationTransformReason::ValueContract
                    }
                }),
            )
        })
        .collect();
    let phase = match record.phase() {
        PersistedSchemaMigrationPhase::Prepared => SchemaMigrationPhase::Prepared,
        PersistedSchemaMigrationPhase::Validating => SchemaMigrationPhase::Validating,
        PersistedSchemaMigrationPhase::ReadyToRewrite => SchemaMigrationPhase::ReadyToRewrite,
        PersistedSchemaMigrationPhase::RewritingRows => SchemaMigrationPhase::RewritingRows,
        PersistedSchemaMigrationPhase::FinalValidation => SchemaMigrationPhase::FinalValidation,
        PersistedSchemaMigrationPhase::Publishing => SchemaMigrationPhase::Publishing,
        PersistedSchemaMigrationPhase::Applied => SchemaMigrationPhase::Applied,
        PersistedSchemaMigrationPhase::Rejected => SchemaMigrationPhase::Rejected,
        PersistedSchemaMigrationPhase::Aborted => SchemaMigrationPhase::Aborted,
    };
    let terminal_receipt = (record.phase() == PersistedSchemaMigrationPhase::Applied).then(|| {
        SchemaMigrationReceipt::new(
            record.database_identity(),
            Some(record.plan_digest()),
            record.accepted_before().clone(),
            record.candidate_head().clone(),
        )
    });
    Ok(SchemaMigrationStatusPage::new(
        record.database_identity(),
        target.accepted_head().clone(),
        Some(record.plan_digest()),
        phase,
        transitions,
        record.progress().rows_validated(),
        record.progress().rows_rewritten(),
        record.progress().indexes_rebuilt(),
        findings,
        None,
        terminal_receipt,
    ))
}

#[cfg(feature = "migration")]
fn load_current_application_bundles(
    authorities: &[StoreApplicationAuthority],
) -> Result<Vec<Option<AcceptedSchemaRevisionBundle>>, InternalError> {
    authorities
        .iter()
        .map(|authority| {
            authority
                .handle
                .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)
        })
        .collect()
}

#[cfg(feature = "migration")]
fn existing_proposal_stores<'a>(
    database_identity: TargetDatabaseIdentity,
    authorities: &[StoreApplicationAuthority],
    bundles: &'a [Option<AcceptedSchemaRevisionBundle>],
) -> Vec<ExistingProposalStore<'a>> {
    authorities
        .iter()
        .zip(bundles)
        .filter_map(|(authority, bundle)| {
            bundle.as_ref().map(|bundle| ExistingProposalStore {
                path: authority.path,
                identity: derive_store_identity(database_identity, authority),
                bundle: bundle.catalog(),
            })
        })
        .collect()
}

#[cfg(feature = "migration")]
fn lineage_after_planned(
    before: &AcceptedEntitySourceLineageCatalog,
    planned: &[PlannedEntitySourceLineage],
    accepted_head: &ExpectedAcceptedHead,
) -> Result<AcceptedEntitySourceLineageCatalog, InternalError> {
    let mut entries = BTreeMap::new();
    for (key, entry) in before.entries() {
        let next = match entry.state() {
            AcceptedEntitySourceLineageState::Unadopted => {
                AcceptedEntitySourceLineage::unadopted(accepted_head.clone())?
            }
            AcceptedEntitySourceLineageState::Adopted {
                version,
                source_digest,
            } => AcceptedEntitySourceLineage::adopted(
                accepted_head.clone(),
                *version,
                *source_digest,
            )?,
        };
        entries.insert(*key, next);
    }
    for next in planned {
        entries.insert(
            (next.store(), next.entity()),
            AcceptedEntitySourceLineage::adopted(
                accepted_head.clone(),
                next.version(),
                next.digest(),
            )?,
        );
    }
    AcceptedEntitySourceLineageCatalog::try_new(entries)
}

#[cfg(feature = "migration")]
fn schema_migration_planning_error(error: SchemaMigrationPlanningError) -> InternalError {
    let reason = match error {
        SchemaMigrationPlanningError::Preparation(error) => return error,
        SchemaMigrationPlanningError::Unadopted => SchemaMigrationCode::Unadopted,
        SchemaMigrationPlanningError::MissingMigration => SchemaMigrationCode::MissingMigration,
        SchemaMigrationPlanningError::VersionGap => SchemaMigrationCode::VersionGap,
        SchemaMigrationPlanningError::Downgrade => SchemaMigrationCode::Downgrade,
        SchemaMigrationPlanningError::EmptyEntityVersionBump => {
            SchemaMigrationCode::EmptyEntityVersionBump
        }
        SchemaMigrationPlanningError::UnknownFromObject => SchemaMigrationCode::UnknownFromObject,
        SchemaMigrationPlanningError::UnknownToObject => SchemaMigrationCode::UnknownToObject,
        SchemaMigrationPlanningError::KindMismatch => SchemaMigrationCode::KindMismatch,
        SchemaMigrationPlanningError::IdentityConflict => SchemaMigrationCode::IdentityConflict,
        SchemaMigrationPlanningError::UnexplainedSchemaDifference => {
            SchemaMigrationCode::UnexplainedSchemaDifference
        }
        SchemaMigrationPlanningError::UnsupportedTransform => {
            SchemaMigrationCode::UnsupportedTransform
        }
        SchemaMigrationPlanningError::RekeyedCatalogInvalid
        | SchemaMigrationPlanningError::CandidateMismatch => SchemaMigrationCode::CandidateMismatch,
        SchemaMigrationPlanningError::CorruptLineage => SchemaMigrationCode::ProgressCorrupt,
    };
    InternalError::schema_migration(reason)
}

#[cfg(feature = "migration")]
fn migration_submission_key(
    plan_digest: Option<SchemaMigrationPlanDigest>,
) -> Result<SchemaSubmissionKey, InternalError> {
    let mut hasher = new_hash_sha256_prefixed(SCHEMA_MIGRATION_SUBMISSION_PROFILE);
    match plan_digest {
        None => write_hash_tag_u8(&mut hasher, 0),
        Some(digest) => {
            write_hash_tag_u8(&mut hasher, 1);
            hasher.update(digest.to_bytes());
        }
    }
    let digest = finalize_hash_sha256(hasher);
    let mut encoded = String::with_capacity(80);
    encoded.push_str("migration/");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").map_err(|_| InternalError::store_invariant())?;
    }
    SchemaSubmissionKey::try_new(encoded).map_err(|_| InternalError::store_invariant())
}

#[cfg(feature = "migration")]
fn load_migration_record_for_status(
    database_identity: TargetDatabaseIdentity,
    submission_key: &SchemaSubmissionKey,
) -> Result<Option<SchemaApplicationRecord>, InternalError> {
    let record =
        with_schema_application_store(|store| store.load(database_identity, submission_key))?;
    if record
        .as_ref()
        .is_some_and(|record| record.receipt().database_identity() != database_identity)
    {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::ProgressCorrupt,
        ));
    }
    Ok(record)
}

#[cfg(feature = "migration")]
fn load_exact_migration_record(
    database_identity: TargetDatabaseIdentity,
    submission_key: &SchemaSubmissionKey,
    proposal_digest: SchemaProposalDigest,
    prior_head: &ExpectedAcceptedHead,
) -> Result<Option<SchemaApplicationRecord>, InternalError> {
    let Some(record) =
        with_schema_application_store(|store| store.load(database_identity, submission_key))?
    else {
        return Ok(None);
    };
    if !record.receipt().is_exact_submission(
        database_identity,
        submission_key,
        proposal_digest,
        prior_head,
    ) {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::PlanChanged,
        ));
    }
    Ok(Some(record))
}

#[cfg(feature = "migration")]
fn public_migration_receipt(
    record: &SchemaApplicationRecord,
    plan_digest: Option<SchemaMigrationPlanDigest>,
) -> Result<SchemaMigrationReceipt, InternalError> {
    let accepted_head = migration_record_accepted_head(record)?.clone();
    Ok(SchemaMigrationReceipt::new(
        record.receipt().database_identity(),
        plan_digest,
        record.receipt().prior_head().clone(),
        accepted_head,
    ))
}

#[cfg(feature = "migration")]
fn migration_record_accepted_head(
    record: &SchemaApplicationRecord,
) -> Result<&ExpectedAcceptedHead, InternalError> {
    match record.receipt().outcome() {
        SchemaChangeOutcome::NoOp { accepted_head }
        | SchemaChangeOutcome::Applied { accepted_head } => Ok(accepted_head),
        SchemaChangeOutcome::Pending { .. } | SchemaChangeOutcome::Aborted { .. } => Err(
            InternalError::schema_migration(SchemaMigrationCode::ProgressCorrupt),
        ),
    }
}

#[cfg(feature = "migration")]
fn migration_transitions(
    proposal: &SchemaProposal,
) -> Result<Vec<SchemaMigrationEntityTransition>, InternalError> {
    if let Some(plan) = proposal.migration() {
        return plan
            .transitions()
            .iter()
            .map(|transition| {
                let target = proposal_entity(proposal, transition.entity())?;
                Ok(SchemaMigrationEntityTransition::new(
                    transition.entity().clone(),
                    Some(transition.from().get()),
                    target.version().get(),
                ))
            })
            .collect();
    }
    proposal
        .fragments()
        .iter()
        .flat_map(icydb_schema::SchemaFragment::entities)
        .map(|entity| {
            Ok(SchemaMigrationEntityTransition::new(
                entity.source_key().clone(),
                None,
                entity.version().get(),
            ))
        })
        .collect()
}

#[cfg(feature = "migration")]
fn proposal_entity<'a>(
    proposal: &'a SchemaProposal,
    source: &EntitySourceKey,
) -> Result<&'a icydb_schema::EntityFragment, InternalError> {
    proposal
        .fragments()
        .iter()
        .flat_map(icydb_schema::SchemaFragment::entities)
        .find(|entity| entity.source_key() == source)
        .ok_or_else(InternalError::store_invariant)
}

#[cfg(feature = "migration")]
fn current_proposal_lineage_is_applied<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
) -> Result<bool, InternalError> {
    let Some(lineage) = load_entity_source_lineage_catalog()? else {
        return Ok(false);
    };
    // Lineage retains historical entities after removal. Completion belongs
    // to this proposal's entities, each checked against current authority below.
    let entities = proposal
        .fragments()
        .iter()
        .flat_map(icydb_schema::SchemaFragment::entities);
    let target = proposal.target_database();
    let authorities = application_authorities(db);
    for entity in entities {
        let source = entity.source_key();
        let digest = proposal
            .entity_source_digest(source)
            .map_err(|_| InternalError::store_invariant())?;
        let mut matched = false;
        for authority in &authorities {
            let store_identity = derive_store_identity(target, authority);
            let entity_tag = authority
                .handle
                .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
                .and_then(|bundle| bundle.source_bindings().entity(source));
            let Some(entity_tag) = entity_tag else {
                continue;
            };
            let Some(entry) = lineage.get(store_identity, entity_tag) else {
                return Ok(false);
            };
            matched = matches!(
                entry.state(),
                AcceptedEntitySourceLineageState::Adopted { version, source_digest }
                    if version.get() == entity.version().get() && *source_digest == digest
            );
            break;
        }
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(feature = "migration")]
fn preflight_unpublished_schema_migration<C: CanisterKind>(
    target: &SchemaApplicationTarget,
    proposal: &SchemaProposal,
    db: &Db<C>,
) -> Result<(), InternalError> {
    let authorities = application_authorities(db);
    let current_bundles = authorities
        .iter()
        .map(|authority| {
            authority
                .handle
                .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    let stores = authorities
        .iter()
        .zip(&current_bundles)
        .filter_map(|(authority, bundle)| {
            bundle.as_ref().map(|bundle| ExistingProposalStore {
                path: authority.path,
                identity: derive_store_identity(target.database_identity(), authority),
                bundle: bundle.catalog(),
            })
        })
        .collect::<Vec<_>>();
    let lineage = load_entity_source_lineage_catalog()?.unwrap_or_default();
    let planned = plan_schema_migration(proposal, stores.as_slice(), &lineage)
        .map_err(schema_migration_planning_error)?;
    if planned.candidates().is_empty() || planned.lineage().is_empty() {
        return Err(InternalError::store_invariant());
    }
    for next in planned.lineage() {
        let Some(current) = lineage.get(next.store(), next.entity()) else {
            // Fresh entity lineage starts at version 1. An accepted entity
            // with missing lineage still requires adoption, never creation.
            if next.version().get() != 1
                || stores.iter().any(|store| {
                    store.identity == next.store()
                        && store.bundle.entity_snapshots.contains_key(&next.entity())
                })
            {
                return Err(InternalError::store_invariant());
            }
            continue;
        };
        let AcceptedEntitySourceLineageState::Adopted {
            version,
            source_digest,
        } = current.state()
        else {
            return Err(InternalError::store_invariant());
        };
        let expected_version = version
            .get()
            .checked_add(1)
            .ok_or_else(InternalError::store_invariant)?;
        if next.version().get() != expected_version || next.digest() == *source_digest {
            return Err(InternalError::store_invariant());
        }
    }
    Ok(())
}

fn lower_application_candidates<const ALLOW_REMOVALS: bool>(
    target: &SchemaApplicationTarget,
    proposal: &SchemaProposal,
    authorities: &[StoreApplicationAuthority],
) -> Result<LoweredApplication, InternalError> {
    let _span = ExecutionMetricsSpan::new(ExecutionMetricsPhase::Lowering);
    let current_bundles = authorities
        .iter()
        .map(|authority| {
            authority
                .handle
                .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    let initial_application = matches!(target.accepted_head(), ExpectedAcceptedHead::Empty);
    let mut candidates = match target.accepted_head() {
        ExpectedAcceptedHead::Empty => {
            let stores = authorities
                .iter()
                .map(|authority| ProposalStoreTarget {
                    path: authority.path,
                    identity: derive_store_identity(target.database_identity(), authority),
                })
                .collect::<Vec<_>>();
            let candidates = lower_initial_schema_proposal(proposal, stores.as_slice())?;
            #[cfg(feature = "migration")]
            {
                let planned = plan_new_entity_source_lineage(proposal, |_| false, &candidates)
                    .map_err(schema_migration_planning_error)?;
                if planned.len()
                    != proposal
                        .fragments()
                        .iter()
                        .map(|fragment| fragment.entities().len())
                        .sum::<usize>()
                {
                    return Err(InternalError::store_invariant());
                }
            }
            candidates
        }
        ExpectedAcceptedHead::Exact { .. }
            if proposal.fragments().is_empty() && proposal.removals().is_empty() =>
        {
            Vec::new()
        }
        ExpectedAcceptedHead::Exact { .. } => {
            let stores = authorities
                .iter()
                .zip(&current_bundles)
                .filter_map(|(authority, bundle)| {
                    bundle.as_ref().map(|bundle| ExistingProposalStore {
                        path: authority.path,
                        identity: derive_store_identity(target.database_identity(), authority),
                        bundle: bundle.catalog(),
                    })
                })
                .collect::<Vec<_>>();
            if ALLOW_REMOVALS {
                lower_existing_schema_proposal(proposal, stores.as_slice())?
            } else {
                lower_generated_existing_schema_proposal(proposal, stores.as_slice())?
            }
        }
    };
    let pending = if initial_application {
        preflight_initial_application(authorities, &candidates)?;
        None
    } else {
        preflight_existing_application(authorities, &current_bundles, &mut candidates)?
    };
    Ok(LoweredApplication {
        current_bundles,
        candidates,
        pending,
    })
}

fn validate_database_identity_state_capacity(
    authorities: &[StoreApplicationAuthority],
    candidates: &[CandidateSchemaRevision],
    incarnation: crate::db::integrity::DatabaseIncarnationId,
) -> Result<(), InternalError> {
    let mut total = 0usize;
    for authority in authorities {
        let count = match candidates
            .iter()
            .find(|candidate| candidate.store_path() == authority.path)
        {
            Some(candidate) => authority.handle.with_schema(|store| {
                store.projected_identity_state_count(incarnation, candidate)
            })?,
            None => authority
                .handle
                .with_schema(|store| store.identity_state_inventory_for_integrity(incarnation))?
                .len(),
        };
        total = include_identity_state_count(total, count)?;
    }
    Ok(())
}

fn include_identity_state_count(total: usize, count: usize) -> Result<usize, InternalError> {
    let total = total
        .checked_add(count)
        .ok_or_else(InternalError::identity_state_capacity_exhausted)?;
    if total > MAX_IDENTITY_STATE_RECORDS_PER_DATABASE {
        return Err(InternalError::identity_state_capacity_exhausted());
    }
    Ok(total)
}

fn preflight_initial_application(
    authorities: &[StoreApplicationAuthority],
    candidates: &[crate::db::schema::CandidateSchemaRevision],
) -> Result<(), InternalError> {
    for candidate in candidates {
        let authority = authorities
            .iter()
            .find(|authority| authority.path == candidate.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        if !authority.handle.with_data(DataStore::is_empty)?
            || authority.handle.index_state() != IndexState::Ready
            || !authority.handle.with_index(IndexStore::is_empty)?
        {
            return Err(InternalError::store_unsupported());
        }
    }
    Ok(())
}

/// Complete generated row-local additions only after a bounded exact proof.
///
/// Empty domains use effective row-key presence. At most one non-empty
/// activation may consume the canonical exact scan budget. A journaled
/// proof that exceeds that page becomes one durable pending application;
/// volatile or additional non-empty proofs reject before publication.
fn preflight_existing_application(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<crate::db::schema::AcceptedSchemaRevisionBundle>],
    candidates: &mut [CandidateSchemaRevision],
) -> Result<Option<PendingGeneratedRowLocalConstraint>, InternalError> {
    preflight_new_entity_domains(authorities, current_bundles, candidates)?;
    require_empty_physical_entity_removal(authorities, current_bundles, candidates)?;
    require_empty_physical_field_removals(authorities, current_bundles, candidates)?;
    require_empty_physical_index_removals(authorities, current_bundles, candidates)?;
    require_empty_physical_relation_removals(authorities, current_bundles, candidates)?;
    let proofs = generated_row_local_constraint_proofs(authorities, current_bundles, candidates)?;
    if proofs.iter().filter(|proof| proof.has_rows).count() > 1 {
        return Err(InternalError::store_unsupported());
    }

    let mut pending = None;
    for candidate_index in 0..candidates.len() {
        let candidate = candidates
            .get(candidate_index)
            .cloned()
            .ok_or_else(InternalError::store_invariant)?;
        let candidate_proofs = proofs
            .iter()
            .filter(|proof| proof.candidate_index == candidate_index)
            .collect::<Vec<_>>();
        if candidate_proofs.is_empty() {
            continue;
        }

        let mut snapshots = candidate.bundle().entity_snapshots().clone();
        for proof in candidate_proofs {
            let mut promote = true;
            if proof.has_rows {
                match validate_unpublished_row_local_candidate_bounded(
                    proof.store,
                    proof.store_path,
                    proof.entity_tag,
                    proof.entity_path.as_str(),
                    &candidate,
                    proof.constraint_id,
                )? {
                    UnpublishedRowLocalValidation::Complete { .. } => {}
                    UnpublishedRowLocalValidation::Incomplete(_) => {
                        if proof.store.storage_capabilities().recovery()
                            != StoreRecoveryCapability::StableBasePlusJournalReplay
                            || pending.is_some()
                        {
                            return Err(InternalError::store_unsupported());
                        }
                        pending = Some(PendingGeneratedRowLocalConstraint {
                            proof: (*proof).clone(),
                        });
                        promote = false;
                    }
                }
            }
            if !promote {
                continue;
            }
            let snapshot = snapshots
                .get(&proof.entity_tag)
                .cloned()
                .ok_or_else(InternalError::store_invariant)?;
            let catalog = snapshot
                .constraint_catalog()
                .clone()
                .with_directly_validated_activation(proof.constraint_id)
                .map_err(|_| InternalError::store_invariant())?;
            snapshots.insert(proof.entity_tag, snapshot.with_constraint_catalog(catalog));
        }
        let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
            candidate.revision(),
            candidate.bundle().store_path(),
            candidate.bundle().enum_catalog().clone(),
            candidate.bundle().composite_catalog().clone(),
            candidate.bundle().source_bindings().clone(),
            snapshots,
        )?;
        candidates[candidate_index] = CandidateSchemaRevision::new(bundle)?;
    }
    Ok(pending)
}

/// New accepted tags must name empty physical domains before any publication.
/// Use candidate catalogs only to resolve target store placement; current rows,
/// user indexes and target-owned reverse indexes supply the emptiness proof.
fn preflight_new_entity_domains(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<(), InternalError> {
    let mut after = current_bundles
        .iter()
        .map(Option::as_ref)
        .collect::<Vec<_>>();
    for candidate in candidates {
        let position = authorities
            .iter()
            .position(|authority| authority.path == candidate.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        let bundle = after
            .get_mut(position)
            .ok_or_else(InternalError::store_invariant)?;
        *bundle = Some(candidate.bundle());
    }
    for candidate in candidates {
        let (position, authority) = authorities
            .iter()
            .enumerate()
            .find(|(_, authority)| authority.path == candidate.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        let current = current_bundles
            .get(position)
            .and_then(Option::as_ref)
            .ok_or_else(InternalError::store_invariant)?;
        for (tag, snapshot) in candidate.bundle().entity_snapshots() {
            if current.entity_snapshots().contains_key(tag) {
                continue;
            }
            require_exact_empty_entity(authority.handle, *tag)?;
            authority
                .handle
                .with_index(|store| prove_empty_user_index_domain(store, *tag))
                .map_err(StagedUserIndexDomainError::into_internal_error)?;
            for relation in snapshot.relations() {
                let target = accepted_entity_store_for_path(
                    authorities,
                    after.iter().copied(),
                    relation.target_path(),
                )?;
                target.with_index(|store| {
                    prove_empty_reverse_relation_domain(store, *tag, snapshot, relation)
                })?;
            }
        }
    }
    Ok(())
}

/// Prove one exact generated entity removal has no retained logical or
/// physical authority.
///
/// The source row domain, every user-index generation, and every outgoing
/// reverse-relation generation must be empty. The accepted-after topology must
/// also contain no retained relation targeting the removed entity.
fn require_empty_physical_entity_removal(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<(), InternalError> {
    let mut removed_entity = None;
    for candidate in candidates {
        let (position, source_authority) = authorities
            .iter()
            .enumerate()
            .find(|(_, authority)| authority.path == candidate.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        let current = current_bundles
            .get(position)
            .and_then(Option::as_ref)
            .ok_or_else(InternalError::store_invariant)?;
        let removed = current
            .entity_snapshots()
            .iter()
            .filter(|(entity_tag, _)| {
                !candidate
                    .bundle()
                    .entity_snapshots()
                    .contains_key(entity_tag)
            })
            .collect::<Vec<_>>();
        if removed.is_empty() {
            continue;
        }
        let [(entity_tag, snapshot)] = removed.as_slice() else {
            return Err(InternalError::store_unsupported());
        };
        let entity_tag = **entity_tag;
        let snapshot = *snapshot;
        if removed_entity.is_some()
            || current.entity_snapshots().len()
                != candidate
                    .bundle()
                    .entity_snapshots()
                    .len()
                    .saturating_add(1)
        {
            return Err(InternalError::store_unsupported());
        }
        require_exact_empty_entity(source_authority.handle, entity_tag)?;
        source_authority
            .handle
            .with_index(|store| prove_empty_user_index_domain(store, entity_tag))
            .map_err(StagedUserIndexDomainError::into_internal_error)?;
        for relation in snapshot.relations() {
            let target_store = accepted_entity_store_for_path(
                authorities,
                current_bundles.iter().map(Option::as_ref),
                relation.target_path(),
            )?;
            target_store.with_index(|store| {
                prove_empty_reverse_relation_domain(store, entity_tag, snapshot, relation)
            })?;
        }
        removed_entity = Some(snapshot.entity_path());
    }

    let Some(removed_path) = removed_entity else {
        return Ok(());
    };
    for (position, authority) in authorities.iter().enumerate() {
        let after = candidates
            .iter()
            .find(|candidate| candidate.store_path() == authority.path)
            .map(CandidateSchemaRevision::bundle)
            .or_else(|| current_bundles.get(position).and_then(Option::as_ref));
        let Some(after) = after else {
            continue;
        };
        if after
            .entity_snapshots()
            .values()
            .flat_map(crate::db::schema::PersistedSchemaSnapshot::relations)
            .any(|relation| relation.target_path() == removed_path)
        {
            return Err(InternalError::store_unsupported());
        }
    }
    Ok(())
}

/// Prove that every removed relation has neither source rows nor surviving
/// entries in its exact target-owned reverse physical generation.
fn require_empty_physical_relation_removals(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<(), InternalError> {
    for candidate in candidates {
        let (position, source_authority) = authorities
            .iter()
            .enumerate()
            .find(|(_, authority)| authority.path == candidate.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        let current = current_bundles
            .get(position)
            .and_then(Option::as_ref)
            .ok_or_else(InternalError::store_invariant)?;
        for (entity_tag, after) in candidate.bundle().entity_snapshots() {
            let Some(before) = current.entity_snapshots().get(entity_tag) else {
                continue;
            };
            let removed = before
                .relations()
                .iter()
                .filter(|relation| {
                    !after
                        .relations()
                        .iter()
                        .any(|candidate| candidate.id() == relation.id())
                })
                .collect::<Vec<_>>();
            if removed.is_empty() {
                continue;
            }
            let added = after.relations().iter().any(|relation| {
                !before
                    .relations()
                    .iter()
                    .any(|accepted| accepted.id() == relation.id())
            });
            let [removed] = removed.as_slice() else {
                return Err(InternalError::store_unsupported());
            };
            if added || before.relations().len() != after.relations().len().saturating_add(1) {
                return Err(InternalError::store_unsupported());
            }
            require_exact_empty_entity(source_authority.handle, *entity_tag)?;
            let target_store = accepted_entity_store_for_path(
                authorities,
                current_bundles.iter().map(Option::as_ref),
                removed.target_path(),
            )?;
            target_store.with_index(|store| {
                prove_empty_reverse_relation_domain(store, *entity_tag, before, removed)
            })?;
        }
    }
    Ok(())
}

fn accepted_entity_store_for_path<'a>(
    authorities: &[StoreApplicationAuthority],
    current_bundles: impl Iterator<Item = Option<&'a AcceptedSchemaRevisionBundle>>,
    entity_path: &str,
) -> Result<StoreHandle, InternalError> {
    let mut resolved = None;
    for (position, bundle) in current_bundles.enumerate() {
        let Some(bundle) = bundle else {
            continue;
        };
        if !bundle
            .entity_snapshots()
            .values()
            .any(|snapshot| snapshot.entity_path() == entity_path)
        {
            continue;
        }
        if resolved.is_some() {
            return Err(InternalError::store_invariant());
        }
        resolved = authorities.get(position).map(|authority| authority.handle);
    }
    resolved.ok_or_else(InternalError::store_unsupported)
}

/// Prove that every dense index-removal candidate has neither authoritative
/// rows nor stale physical user-index state. The staged replacement is empty
/// by construction and is discarded before schema-only publication.
fn require_empty_physical_index_removals(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<(), InternalError> {
    validate_existing_candidate_entities(
        authorities,
        current_bundles,
        candidates,
        |store, entity_tag, before, after| {
            if before.indexes().len() == after.indexes().len() {
                return Ok(());
            }
            if before.indexes().len() != after.indexes().len().saturating_add(1) {
                return Err(InternalError::store_unsupported());
            }
            require_exact_empty_entity(store, entity_tag)?;
            store
                .with_index(|index| prove_empty_user_index_domain(index, entity_tag))
                .map_err(StagedUserIndexDomainError::into_internal_error)
        },
    )
}

/// Prove that every dense field-removal candidate has no historical row to
/// rewrite by inspecting the effective row-key prefix.
fn require_empty_physical_field_removals(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<(), InternalError> {
    validate_existing_candidate_entities(
        authorities,
        current_bundles,
        candidates,
        |store, entity_tag, before, after| {
            if before.row_layout() == after.row_layout() {
                return Ok(());
            }
            if before.fields().len() != after.fields().len().saturating_add(1) {
                return Err(InternalError::store_unsupported());
            }
            require_exact_empty_entity(store, entity_tag)
        },
    )
}

// Traverse candidates in their existing order and pair only retained entities.
// Resolve each store before invoking its kind-specific physical safety proof;
// missing authority/bundles remain invariants, while new entities are skipped.
fn validate_existing_candidate_entities(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
    mut validate: impl FnMut(
        StoreHandle,
        EntityTag,
        &PersistedSchemaSnapshot,
        &PersistedSchemaSnapshot,
    ) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    for candidate in candidates {
        let (position, authority) = authorities
            .iter()
            .enumerate()
            .find(|(_, authority)| authority.path == candidate.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        let current = current_bundles
            .get(position)
            .and_then(Option::as_ref)
            .ok_or_else(InternalError::store_invariant)?;
        for (entity_tag, after) in candidate.bundle().entity_snapshots() {
            let Some(before) = current.entity_snapshots().get(entity_tag) else {
                continue;
            };
            validate(authority.handle, *entity_tag, before, after)?;
        }
    }
    Ok(())
}

// Logical emptiness comes from the effective row owner, not optional counts.
fn require_exact_empty_entity(
    store: StoreHandle,
    entity_tag: EntityTag,
) -> Result<(), InternalError> {
    if store.with_data(|data| entity_has_rows(data, entity_tag))? {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

// Seek this entity's effective prefix and stop at its first visible key. The
// row store owns overlay/tombstone merging; a presence proof reads no payloads.
fn entity_has_rows(data: &DataStore, entity: EntityTag) -> Result<bool, InternalError> {
    let range = RawDataStoreKeyRange::entity_prefix(entity);
    let lower = Bound::Included(RawDataStoreKey::store_range_lower_key(&range));
    let upper = range
        .upper_exclusive()
        .map(RawDataStoreKey::from_store_range_bound)
        .map_or(Bound::Unbounded, Bound::Excluded);
    let mut has_rows = false;
    data.visit_key_range((lower, upper), |_| {
        has_rows = true;
        Ok::<_, InternalError>(StoreVisit::Stop)
    })?;

    Ok(has_rows)
}

fn generated_row_local_constraint_proofs(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<Vec<DirectGeneratedRowLocalProof>, InternalError> {
    let mut proofs = Vec::new();
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        let (position, authority) = authorities
            .iter()
            .enumerate()
            .find(|(_, authority)| authority.path == candidate.store_path())
            .ok_or_else(InternalError::store_invariant)?;
        let current = current_bundles
            .get(position)
            .and_then(Option::as_ref)
            .ok_or_else(InternalError::store_invariant)?;
        for (entity_tag, after) in candidate.bundle().entity_snapshots() {
            let Some(before) = current.entity_snapshots().get(entity_tag) else {
                continue;
            };
            let added = added_generated_row_local_activations(before, after);
            if added.is_empty() {
                continue;
            }
            let has_rows = authority
                .handle
                .with_data(|data| entity_has_rows(data, *entity_tag))?;
            for constraint_id in added {
                proofs.push(DirectGeneratedRowLocalProof {
                    candidate_index,
                    store: authority.handle,
                    store_path: authority.path,
                    entity_tag: *entity_tag,
                    entity_path: after.entity_path().to_string(),
                    constraint_id,
                    has_rows,
                });
            }
        }
    }
    Ok(proofs)
}

fn added_generated_row_local_activations(
    before: &crate::db::schema::PersistedSchemaSnapshot,
    after: &crate::db::schema::PersistedSchemaSnapshot,
) -> Vec<ConstraintId> {
    after
        .constraint_activations()
        .iter()
        .filter(|candidate| {
            candidate.origin() == ConstraintOrigin::Generated
                && matches!(
                    candidate.kind(),
                    ConstraintActivationKind::Check { .. }
                        | ConstraintActivationKind::TargetedRule { .. }
                )
                && !before
                    .constraint_activations()
                    .iter()
                    .any(|accepted| accepted.id() == candidate.id())
        })
        .map(crate::db::schema::ConstraintActivationSnapshot::id)
        .collect()
}

fn final_candidates_for_pending_row_local_constraint(
    candidates: &[CandidateSchemaRevision],
    pending: &PendingGeneratedRowLocalConstraint,
) -> Result<Vec<CandidateSchemaRevision>, InternalError> {
    let mut final_candidates = candidates.to_vec();
    let candidate = final_candidates
        .get(pending.proof.candidate_index)
        .cloned()
        .ok_or_else(InternalError::store_invariant)?;
    if candidate.store_path() != pending.proof.store_path {
        return Err(InternalError::store_invariant());
    }
    let mut snapshots = candidate.bundle().entity_snapshots().clone();
    let snapshot = snapshots
        .get(&pending.proof.entity_tag)
        .cloned()
        .ok_or_else(InternalError::store_invariant)?;
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_directly_validated_activation(pending.proof.constraint_id)
        .map_err(|_| InternalError::store_invariant())?;
    snapshots.insert(
        pending.proof.entity_tag,
        snapshot.with_constraint_catalog(catalog),
    );
    let final_revision = candidate
        .revision()
        .checked_next()
        .and_then(AcceptedSchemaRevision::checked_next)
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
        final_revision,
        candidate.bundle().store_path(),
        candidate.bundle().enum_catalog().clone(),
        candidate.bundle().composite_catalog().clone(),
        candidate.bundle().source_bindings().clone(),
        snapshots,
    )?;
    final_candidates[pending.proof.candidate_index] = CandidateSchemaRevision::new(bundle)?;
    Ok(final_candidates)
}

fn schema_change_progress_status(
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    progress: ConstraintValidationProgress,
) -> Result<SchemaChangeProgressStatus, InternalError> {
    match progress {
        ConstraintValidationProgress::Started => Ok(SchemaChangeProgressStatus::Started),
        ConstraintValidationProgress::Advanced {
            phase,
            rows_scanned,
        } => Ok(SchemaChangeProgressStatus::Advanced {
            phase: schema_change_validation_phase(phase),
            rows_scanned,
        }),
        ConstraintValidationProgress::Findings {
            receipt,
            phase,
            rows_scanned,
        } => {
            let activation = snapshot
                .constraint_catalog()
                .activation(constraint_id)
                .ok_or_else(InternalError::store_corruption)?;
            let fingerprint =
                crate::db::schema::accepted_schema_cache_fingerprint_for_persisted_snapshot(
                    snapshot,
                )?;
            let findings = receipt
                .findings()
                .iter()
                .map(|finding| {
                    constraint_validation_finding_output(
                        fingerprint,
                        entity_tag,
                        activation,
                        finding,
                    )
                })
                .collect::<Result<Vec<_>, InternalError>>()?;
            Ok(SchemaChangeProgressStatus::Findings {
                phase: schema_change_validation_phase(phase),
                rows_scanned,
                page_sequence: receipt.page_sequence(),
                findings,
            })
        }
        ConstraintValidationProgress::Restarted { rows_scanned } => {
            Ok(SchemaChangeProgressStatus::Restarted { rows_scanned })
        }
        ConstraintValidationProgress::Promoted { .. } => Ok(SchemaChangeProgressStatus::Applied),
    }
}

const fn schema_change_validation_phase(
    phase: ConstraintValidationPhase,
) -> SchemaChangeValidationPhase {
    match phase {
        ConstraintValidationPhase::Forward => SchemaChangeValidationPhase::Forward,
        ConstraintValidationPhase::Verify => SchemaChangeValidationPhase::Verify,
    }
}

fn finalize_schema_application<C: CanisterKind>(
    db: &Db<C>,
    record: &SchemaApplicationRecord,
    status: SchemaChangeProgressStatus,
) -> Result<SchemaChangeProgress, InternalError> {
    // Continuation has proved promotion of this record's exact activation.
    // Unrelated accepted publications may have changed the predicted database
    // head; record current authority rather than stranding a validated job.
    let accepted_head = schema_application_target(db)?.accepted_head().clone();
    let receipt = SchemaChangeReceipt::new(
        record.receipt().database_identity(),
        record.receipt().submission_key().clone(),
        record.receipt().proposal_digest(),
        record.receipt().prior_head().clone(),
        SchemaChangeOutcome::Applied {
            accepted_head: accepted_head.clone(),
        },
    )?;
    let terminal = SchemaApplicationRecord::new(receipt.clone(), Vec::new())?;
    let operation = SchemaApplicationRecordOp::replace(record, &terminal)?;
    let mut operations = vec![DatabaseControlOp::SchemaApplication(operation)];
    if let Some(pending) = record.pending_source_lineage() {
        let [activation] = record.activations() else {
            return Err(InternalError::store_corruption());
        };
        let before =
            load_entity_source_lineage_catalog()?.ok_or_else(InternalError::store_corruption)?;
        let after = complete_pending_source_lineage(&before, activation, pending, &accepted_head)?;
        if before != after {
            operations.push(DatabaseControlOp::EntitySourceLineage(
                EntitySourceLineageCatalogOp::replace(Some(&before), &after)?,
            ));
        }
    }
    publish_accepted_schema_candidates_with_database_control(Vec::new(), operations)?;
    Ok(SchemaChangeProgress::new(receipt, status))
}

fn application_publications<'a>(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<crate::db::schema::AcceptedSchemaRevisionBundle>],
    candidates: &'a [crate::db::schema::CandidateSchemaRevision],
) -> Result<Vec<AcceptedSchemaPublication<'a>>, InternalError> {
    candidates
        .iter()
        .map(|candidate| {
            let (position, authority) = authorities
                .iter()
                .enumerate()
                .find(|(_, authority)| authority.path == candidate.store_path())
                .ok_or_else(InternalError::store_invariant)?;
            let expected_revision = current_bundles[position].as_ref().map_or(
                AcceptedSchemaRevision::NONE,
                crate::db::schema::AcceptedSchemaRevisionBundle::revision,
            );
            Ok(AcceptedSchemaPublication::new(
                authority.path,
                authority.handle,
                expected_revision,
                candidate,
            ))
        })
        .collect()
}

fn application_authorities<C: CanisterKind>(db: &Db<C>) -> Vec<StoreApplicationAuthority> {
    let mut authorities = db.with_store_registry(|registry| {
        registry
            .iter()
            .map(|(path, handle)| StoreApplicationAuthority { path, handle })
            .collect::<Vec<_>>()
    });
    icydb_schema::compact_sort_unstable_by(&mut authorities, |left, right| {
        left.path.cmp(right.path)
    });
    authorities
}

fn accepted_head_after_candidates(
    authorities: &[StoreApplicationAuthority],
    candidates: &[crate::db::schema::CandidateSchemaRevision],
) -> Result<ExpectedAcceptedHead, InternalError> {
    let heads = authorities
        .iter()
        .map(|authority| {
            let candidate = candidates
                .iter()
                .find(|candidate| candidate.store_path() == authority.path);
            let head = match candidate {
                Some(candidate) => Some(AcceptedStoreHead {
                    revision: candidate.revision().get(),
                    fingerprint: candidate.root().fingerprint().as_bytes(),
                }),
                None => authority
                    .handle
                    .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_root)?
                    .map(|selection| AcceptedStoreHead {
                        revision: selection.root().revision().get(),
                        fingerprint: selection.root().fingerprint().as_bytes(),
                    }),
            };
            Ok((authority.path, head))
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    Ok(derive_accepted_head(heads.as_slice()))
}

// Unrelated store additions/removals must not re-key every lineage, job or
// receipt. The incarnation already changes on explicit database recreation.
fn derive_database_identity(incarnation: [u8; 16]) -> TargetDatabaseIdentity {
    let mut hasher = new_hash_sha256_prefixed(DATABASE_TARGET_FINGERPRINT_PROFILE);
    hasher.update(incarnation);
    TargetDatabaseIdentity::from_bytes(finalize_hash_sha256(hasher))
}

fn derive_store_identity(
    database_identity: TargetDatabaseIdentity,
    store: &StoreApplicationAuthority,
) -> TargetStoreIdentity {
    let mut hasher = new_hash_sha256_prefixed(STORE_TARGET_FINGERPRINT_PROFILE);
    hasher.update(database_identity.to_bytes());
    write_store_authority(&mut hasher, store);
    TargetStoreIdentity::from_bytes(finalize_hash_sha256(hasher))
}

fn derive_accepted_head(stores: &[(&str, Option<AcceptedStoreHead>)]) -> ExpectedAcceptedHead {
    let Some(revision) = stores
        .iter()
        .filter_map(|(_, head)| head.map(|head| head.revision))
        .max()
    else {
        return ExpectedAcceptedHead::Empty;
    };

    let mut hasher = new_hash_sha256_prefixed(ACCEPTED_DATABASE_HEAD_FINGERPRINT_PROFILE);
    write_hash_len_u32(&mut hasher, stores.len());
    for (path, head) in stores {
        write_hash_str_u32(&mut hasher, path);
        match head {
            None => write_hash_tag_u8(&mut hasher, 0),
            Some(head) => {
                write_hash_tag_u8(&mut hasher, 1);
                write_hash_u64(&mut hasher, head.revision);
                hasher.update(head.fingerprint);
            }
        }
    }

    ExpectedAcceptedHead::Exact {
        revision,
        fingerprint: ExpectedSchemaFingerprint::from_bytes(finalize_hash_sha256(hasher)),
    }
}

/// Resolve the exact generated receipt using the current database incarnation.
///
/// Generated `CanisterKind` wiring supplies its immutable submission; callers
/// must select its commit allocation before invoking this helper.
#[doc(hidden)]
pub fn generated_schema_application_record_key(
    submission_key: &str,
) -> Result<[u8; 32], InternalError> {
    let submission_key = SchemaSubmissionKey::try_new(submission_key.to_owned())
        .map_err(|_| InternalError::store_invariant())?;
    let identity = generated_database_identity(database_incarnation_id()?);
    ApplicationRecordKey::new(identity, &submission_key).map(ApplicationRecordKey::to_bytes)
}

pub(in crate::db) fn generated_schema_reconciled(
    registry: &'static std::thread::LocalKey<crate::db::registry::StoreRegistry>,
    incarnation: DatabaseIncarnationId,
    submission_key: &str,
) -> Result<(bool, ExpectedAcceptedHead), InternalError> {
    let submission_key = SchemaSubmissionKey::try_new(submission_key.to_string())
        .map_err(|_| InternalError::store_invariant())?;
    let (database_identity, accepted_head) = generated_schema_authority(registry, incarnation)?;
    let reconciled = generated_submission_is_reconciled(database_identity, &submission_key)?;
    Ok((reconciled, accepted_head))
}

pub(in crate::db) fn generated_schema_is_reconciled(
    incarnation: DatabaseIncarnationId,
    submission_key: &str,
) -> Result<bool, InternalError> {
    let submission_key = SchemaSubmissionKey::try_new(submission_key.to_string())
        .map_err(|_| InternalError::store_invariant())?;
    let database_identity = generated_database_identity(incarnation);
    generated_submission_is_reconciled(database_identity, &submission_key)
}

fn generated_submission_is_reconciled(
    database_identity: TargetDatabaseIdentity,
    submission_key: &SchemaSubmissionKey,
) -> Result<bool, InternalError> {
    Ok(
        load_schema_application_record_read_only(database_identity, submission_key)?.is_some_and(
            |record| match record.receipt().outcome() {
                // The generated submission key is derived from the complete
                // fragment, migration plan, attempt and entity/store bindings.
                // A terminal receipt proves
                // that exact source was applied in this database incarnation.
                // Compatible SQL DDL may subsequently advance the accepted head
                // while intentionally preserving generated-owned identities and
                // semantics; exact submission replay returns the original receipt
                // and cannot rebind it to that later DDL-owned head.
                SchemaChangeOutcome::NoOp { .. } | SchemaChangeOutcome::Applied { .. } => true,
                SchemaChangeOutcome::Pending { .. } | SchemaChangeOutcome::Aborted { .. } => false,
            },
        ),
    )
}

pub(in crate::db) fn generated_schema_authority(
    registry: &'static std::thread::LocalKey<crate::db::registry::StoreRegistry>,
    incarnation: DatabaseIncarnationId,
) -> Result<(TargetDatabaseIdentity, ExpectedAcceptedHead), InternalError> {
    let database_identity = generated_database_identity(incarnation);
    let mut stores = store_application_authorities(registry);
    icydb_schema::compact_sort_unstable_by(&mut stores, |left, right| left.path.cmp(right.path));
    let heads = stores
        .iter()
        .map(|store| {
            let head = store
                .handle
                .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_root)?
                .map(|selection| AcceptedStoreHead {
                    revision: selection.root().revision().get(),
                    fingerprint: selection.root().fingerprint().as_bytes(),
                });
            Ok((store.path, head))
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    let accepted_head = derive_accepted_head(heads.as_slice());
    Ok((database_identity, accepted_head))
}

fn generated_database_identity(incarnation: DatabaseIncarnationId) -> TargetDatabaseIdentity {
    let cached = GENERATED_DATABASE_IDENTITY
        .with(Cell::get)
        .and_then(|entry| (entry.incarnation == incarnation).then_some(entry.identity));
    if let Some(identity) = cached {
        return identity;
    }
    let identity = derive_database_identity(incarnation.to_bytes());
    GENERATED_DATABASE_IDENTITY.set(Some(GeneratedDatabaseIdentityCacheEntry {
        incarnation,
        identity,
    }));
    identity
}

fn store_application_authorities(
    registry: &'static std::thread::LocalKey<crate::db::registry::StoreRegistry>,
) -> Vec<StoreApplicationAuthority> {
    registry.with(|registry| {
        registry
            .iter()
            .map(|(path, handle)| StoreApplicationAuthority { path, handle })
            .collect()
    })
}

fn write_store_authority(hasher: &mut sha2::Sha256, store: &StoreApplicationAuthority) {
    write_hash_str_u32(hasher, store.path);
    write_storage_capabilities(hasher, store.handle);
    for allocation in [
        store.handle.data_allocation(),
        store.handle.index_allocation(),
        store.handle.schema_allocation(),
        store.handle.journal_allocation(),
    ] {
        write_allocation_identity(hasher, allocation);
    }
}

fn write_storage_capabilities(hasher: &mut sha2::Sha256, store: StoreHandle) {
    let capabilities = store.storage_capabilities();
    write_hash_tag_u8(
        hasher,
        match capabilities.storage_mode() {
            StoreRuntimeStorageMode::Heap => 0,
            StoreRuntimeStorageMode::Journaled => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.allocation_identity() {
            StoreAllocationIdentityCapability::Present => 0,
            StoreAllocationIdentityCapability::Absent => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.durability() {
            StoreDurability::Durable => 0,
            StoreDurability::Volatile => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.recovery() {
            StoreRecoveryCapability::StableBasePlusJournalReplay => 0,
            StoreRecoveryCapability::None => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.commit_participation() {
            StoreCommitParticipation::Durable => 0,
            StoreCommitParticipation::LiveOnly => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.schema_metadata() {
            StoreSchemaMetadataCapability::LiveRebuiltMetadata => 0,
            StoreSchemaMetadataCapability::CanonicalStableHistoryPlusJournalTail => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.relation_source() {
            StoreRelationSourceCapability::DurableSource => 0,
            StoreRelationSourceCapability::LiveSource => 1,
        },
    );
    write_hash_tag_u8(
        hasher,
        match capabilities.relation_target() {
            StoreRelationTargetCapability::DurableTarget => 0,
            StoreRelationTargetCapability::VolatileTarget => 1,
        },
    );
}

fn write_allocation_identity(
    hasher: &mut sha2::Sha256,
    allocation: Option<StoreAllocationIdentity>,
) {
    match allocation {
        None => write_hash_tag_u8(hasher, 0),
        Some(allocation) => {
            write_hash_tag_u8(hasher, 1);
            hasher.update(allocation.memory_id().to_le_bytes());
            write_hash_str_u32(hasher, allocation.stable_key());
        }
    }
}

#[cfg(test)]
mod tests;
