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
            ensure_recovered, publish_accepted_schema_candidates_with_application_record,
            publish_accepted_schema_candidates_with_database_control,
            publish_generated_row_local_abort_with_application_record,
        },
        data::DataStore,
        index::{IndexState, IndexStore},
        registry::{
            StoreAllocationIdentity, StoreAllocationIdentityCapability, StoreCommitParticipation,
            StoreDurability, StoreHandle, StoreRecoveryCapability, StoreRelationSourceCapability,
            StoreRelationTargetCapability, StoreRuntimeStorageMode, StoreSchemaMetadataCapability,
        },
        relation::prove_empty_reverse_relation_domain,
        schema::ensure_schema_migration_ready_for_ordinary_operations,
        schema::{
            AcceptedSchemaRevision, AcceptedSchemaRevisionBundle, CandidateSchemaRevision,
            ConstraintActivationKind, ConstraintActivationState, ConstraintId, ConstraintOrigin,
            ConstraintValidationPhase, ConstraintValidationProgress, ExistingProposalStore,
            MAX_IDENTITY_STATE_RECORDS_PER_DATABASE, ProposalStoreTarget, SchemaApplicationRecord,
            SchemaApplicationRecordOp, SchemaChangeActivation, SchemaChangeJob, SchemaChangeJobId,
            SchemaChangeOutcome, SchemaChangeProgress, SchemaChangeProgressStatus,
            SchemaChangeReceipt, SchemaChangeValidationPhase, StagedUserIndexDomainError,
            UnpublishedRowLocalValidation, advance_accepted_row_local_constraint_activation,
            constraint_validation_finding_diagnostic, derive_schema_change_job_id,
            lower_existing_schema_proposal, lower_initial_schema_proposal,
            prove_empty_user_index_domain, validate_unpublished_row_local_candidate_bounded,
            with_schema_application_store,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};
use candid::CandidType;
use icydb_schema::{
    ExpectedAcceptedHead, ExpectedSchemaFingerprint, SchemaProposal, SchemaProposalDigest,
    SchemaSubmissionKey, TargetDatabaseIdentity, TargetStoreIdentity,
};
use serde::Deserialize;
use sha2::Digest;
#[cfg(feature = "migration")]
use std::collections::BTreeMap;

#[cfg(feature = "migration")]
use crate::db::schema::{
    PersistedSchemaMigrationEntity, PersistedSchemaMigrationFindingKind,
    PersistedSchemaMigrationIndex, PersistedSchemaMigrationPhase,
    PersistedSchemaMigrationTransition, SchemaMigrationCommand, SchemaMigrationEntityTransition,
    SchemaMigrationFinding, SchemaMigrationFindingKind, SchemaMigrationPhase,
    SchemaMigrationReceipt, SchemaMigrationRecord, SchemaMigrationRecordOp,
    SchemaMigrationStatusPage, SchemaMigrationStatusRequest,
    live_schema_checkpoint::{load_entity_source_lineage_catalog, load_schema_migration_record},
    migration_execution::{
        cleanup_migration_staging_page, final_validate_migration_page,
        migration_derived_domain_count, publish_migration_rewrite_page, rewrite_migration_page,
    },
    migration_lineage::{
        AcceptedEntitySourceLineage, AcceptedEntitySourceLineageCatalog,
        AcceptedEntitySourceLineageState, EntitySourceLineageCatalogOp,
    },
    migration_planner::{
        PlannedEntitySourceLineage, SchemaMigrationPlanningError, plan_entity_source_adoption,
        plan_initial_entity_source_lineage, plan_schema_migration,
    },
    migration_validation::{stage_migration_index_entries, validate_migration_page},
};

#[cfg(feature = "migration")]
use icydb_diagnostic_code::SchemaMigrationCode;
#[cfg(feature = "migration")]
use icydb_schema::{EntitySourceKey, SchemaMigrationPlanDigest};

const DATABASE_TARGET_FINGERPRINT_PROFILE: &[u8] = b"icydb.schema-target.database.v1";
const STORE_TARGET_FINGERPRINT_PROFILE: &[u8] = b"icydb.schema-target.store.v1";
const ACCEPTED_DATABASE_HEAD_FINGERPRINT_PROFILE: &[u8] = b"icydb.accepted-schema.database-head.v1";
#[cfg(feature = "migration")]
const SCHEMA_MIGRATION_SUBMISSION_PROFILE: &[u8] = b"icydb.schema-migration.submission.v1";

///
/// SchemaApplicationStore
///
/// One registered store path paired with the opaque routing token accepted by
/// the current database incarnation.
///

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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

#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
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
    historical_rows: u64,
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
    ensure_recovered(db)?;
    let incarnation = database_incarnation_id()?;
    let mut stores = db.with_store_registry(|registry| {
        registry
            .iter()
            .map(|(path, handle)| StoreApplicationAuthority { path, handle })
            .collect::<Vec<_>>()
    });
    stores.sort_unstable_by(|left, right| left.path.cmp(right.path));

    let database_identity = derive_database_identity(incarnation.to_bytes(), stores.as_slice());
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
    ensure_recovered(db)?;
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
/// 0.211 validation step.
pub(in crate::db) fn continue_schema_application<C: CanisterKind>(
    db: &Db<C>,
    job_id: SchemaChangeJobId,
    acknowledged_receipt: Option<u64>,
) -> Result<SchemaChangeProgress, InternalError> {
    ensure_recovered(db)?;
    ensure_schema_migration_ready_for_ordinary_operations()?;
    let record = with_schema_application_store(|store| store.load_job(job_id))?
        .ok_or_else(InternalError::schema_application_conflict)?;
    let target = schema_application_target(db)?;
    if target.database_identity() != record.receipt().database_identity() {
        return Err(InternalError::schema_application_conflict());
    }
    let candidate_head = match record.receipt().outcome() {
        SchemaChangeOutcome::Pending {
            job,
            candidate_head,
        } if job.id() == job_id => candidate_head,
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
    };
    let [activation] = record.activations() else {
        return Err(InternalError::store_corruption());
    };
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
        return finalize_schema_application(
            db,
            &record,
            candidate_head,
            SchemaChangeProgressStatus::Applied,
        );
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
    let status = schema_change_progress_status(snapshot, constraint_id, progress)?;
    if status == SchemaChangeProgressStatus::Applied {
        finalize_schema_application(db, &record, candidate_head, status)
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
    ensure_recovered(db)?;
    ensure_schema_migration_ready_for_ordinary_operations()?;
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
    let mut snapshots = current.entity_snapshots().clone();
    snapshots.insert(entity_tag, snapshot.with_constraint_catalog(catalog));
    let mut source_bindings = current.source_bindings().clone();
    if !accepted_identity_remains {
        source_bindings.remove_constraint_identity(entity_tag, constraint_id)?;
    }
    let revision = current
        .revision()
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
        revision,
        current.store_path(),
        current.enum_catalog().clone(),
        current.composite_catalog().clone(),
        source_bindings,
        snapshots,
    )?;
    CandidateSchemaRevision::new(bundle)
}

/// Apply one exact source-keyed schema proposal through catalog-native
/// accepted candidates and the durable application-receipt boundary.
pub(in crate::db) fn apply_schema<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
) -> Result<SchemaChangeReceipt, InternalError> {
    ensure_recovered(db)?;
    ensure_schema_migration_ready_for_ordinary_operations()?;
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
    } = lower_application_candidates(&target, proposal, authorities.as_slice())?;
    validate_database_identity_state_capacity(
        authorities.as_slice(),
        candidates.as_slice(),
        database_incarnation_id()?,
    )?;
    let accepted_head = if let Some(pending) = pending.as_ref() {
        let final_candidates =
            final_candidates_for_pending_row_local_constraint(&candidates, pending)?;
        accepted_head_after_candidates(authorities.as_slice(), final_candidates.as_slice())?
    } else if candidates.is_empty() {
        target.accepted_head().clone()
    } else {
        accepted_head_after_candidates(authorities.as_slice(), candidates.as_slice())?
    };
    #[cfg(feature = "migration")]
    let outcome_head = accepted_head.clone();
    #[cfg(not(feature = "migration"))]
    let outcome_head = accepted_head;
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
    let operation = SchemaApplicationRecordOp::insert(&record)?;
    #[cfg(feature = "migration")]
    let database_control = attach_ordinary_lineage_publication(
        proposal,
        target.accepted_head(),
        &accepted_head,
        candidates.as_slice(),
        operation,
    )?;
    #[cfg(not(feature = "migration"))]
    let database_control = vec![DatabaseControlOp::SchemaApplication(operation)];
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
    if current_proposal_lineage_is_applied(db, proposal, target.accepted_head())? {
        return Ok(());
    }
    #[cfg(feature = "migration")]
    preflight_unpublished_schema_migration(target, proposal, db)?;
    #[cfg(not(feature = "migration"))]
    let _ = db;
    Err(InternalError::store_unsupported())
}

/// Execute one explicit source-migration operation against the exact deployed
/// generated proposal. Metadata-only adoption and advance complete in one
/// marker; physical work remains rejected until the durable runner exists.
#[cfg(feature = "migration")]
pub(in crate::db) fn migrate_schema<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    command: SchemaMigrationCommand,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    ensure_recovered(db)?;
    match command {
        SchemaMigrationCommand::Adopt {
            expected_database,
            expected_head,
        } => adopt_entity_source_lineage(db, proposal, expected_database, &expected_head),
        SchemaMigrationCommand::Advance {
            expected_database,
            expected_head,
            expected_plan,
            acknowledged_finding_page,
        } => advance_metadata_schema_migration(
            db,
            proposal,
            expected_database,
            &expected_head,
            expected_plan,
            acknowledged_finding_page,
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
                let store_identities = authorities
                    .iter()
                    .map(|authority| {
                        (
                            authority.path,
                            derive_store_identity(record.database_identity(), authority),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
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
                // Patch 4 has no nonterminal job to abort. Patch 5 introduces
                // the bounded pre-rewrite abort state machine.
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
    ensure_recovered(db)?;
    if !request.validate() || request.cursor().is_some() {
        return Err(InternalError::cursor_invalid_continuation());
    }
    let target = schema_application_target(db)?;
    schema_migration_status_for_target(db, proposal, &target)
}

/// Admit generated ordinary endpoint startup while an exact prepared
/// migration deliberately leaves predecessor authority live. Every later
/// phase remains owned by the database-wide gate.
#[cfg(feature = "migration")]
pub(in crate::db) fn defer_generated_schema_application_for_prepared_migration<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
) -> Result<bool, InternalError> {
    ensure_recovered(db)?;
    let Some(record) = load_schema_migration_record()? else {
        return Ok(false);
    };
    if matches!(
        record.phase(),
        PersistedSchemaMigrationPhase::Applied | PersistedSchemaMigrationPhase::Aborted
    ) {
        return Ok(false);
    }
    validate_active_migration_deployment(proposal, &record)?;
    let target = schema_application_target(db)?;
    validate_active_migration_target(&record, &target)?;
    match record.phase() {
        PersistedSchemaMigrationPhase::Prepared => Ok(true),
        PersistedSchemaMigrationPhase::Validating
        | PersistedSchemaMigrationPhase::ReadyToRewrite
        | PersistedSchemaMigrationPhase::RewritingRows
        | PersistedSchemaMigrationPhase::RebuildingIndexes
        | PersistedSchemaMigrationPhase::FinalValidation
        | PersistedSchemaMigrationPhase::Publishing
        | PersistedSchemaMigrationPhase::Rejected => Err(InternalError::schema_migration(
            SchemaMigrationCode::MigrationInProgress,
        )),
        PersistedSchemaMigrationPhase::Applied | PersistedSchemaMigrationPhase::Aborted => {
            Ok(false)
        }
    }
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
#[expect(
    clippy::too_many_lines,
    reason = "one migration entry point keeps preparation and exact replay ordering visible"
)]
fn advance_metadata_schema_migration<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    expected_database: TargetDatabaseIdentity,
    expected_head: &ExpectedAcceptedHead,
    expected_plan: SchemaMigrationPlanDigest,
    acknowledged_finding_page: Option<u64>,
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
        return advance_active_schema_migration(
            db,
            proposal,
            &target,
            &record,
            acknowledged_finding_page,
        );
    }
    if acknowledged_finding_page.is_some() {
        return Err(InternalError::schema_migration(
            SchemaMigrationCode::CandidateMismatch,
        ));
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
        let operation = SchemaMigrationRecordOp::insert(&record)?;
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
            let before = store.bundle.entity_snapshots().get(entity);
            for index in snapshot
                .indexes()
                .iter()
                .filter(|index| {
                    before
                        .and_then(|before| {
                            before
                                .indexes()
                                .iter()
                                .find(|old| old.schema_id() == index.schema_id())
                        })
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
    staged_indexes.sort_unstable();
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
#[expect(
    clippy::too_many_lines,
    reason = "the closed phase match keeps every durable migration transition and publication boundary exhaustive"
)]
fn advance_active_schema_migration<C: CanisterKind>(
    db: &Db<C>,
    proposal: &SchemaProposal,
    target: &SchemaApplicationTarget,
    record: &SchemaMigrationRecord,
    acknowledged_finding_page: Option<u64>,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    match record.phase() {
        PersistedSchemaMigrationPhase::Prepared => {
            if acknowledged_finding_page.is_some() {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
            let validating = record.transition(
                PersistedSchemaMigrationPhase::Validating,
                record.progress().clone(),
            )?;
            publish_migration_record_replacement(record, &validating)?;
            active_migration_status(proposal, target, &validating)
        }
        PersistedSchemaMigrationPhase::Validating => {
            if acknowledged_finding_page.is_some() {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
            let planned = recompile_active_physical_migration(db, proposal, record)?;
            let page = validate_migration_page(db, &planned, record.progress())?;
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
        PersistedSchemaMigrationPhase::Rejected => {
            if acknowledged_finding_page.is_some()
                && acknowledged_finding_page != record.progress().finding_page()
            {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
            active_migration_status(proposal, target, record)
        }
        PersistedSchemaMigrationPhase::ReadyToRewrite => {
            if acknowledged_finding_page.is_some() {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
            let progress = record.progress().begin_row_phase()?;
            let rewriting =
                record.transition(PersistedSchemaMigrationPhase::RewritingRows, progress)?;
            publish_migration_record_replacement(record, &rewriting)?;
            active_migration_status(proposal, target, &rewriting)
        }
        PersistedSchemaMigrationPhase::RewritingRows => {
            if acknowledged_finding_page.is_some() {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
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
            let phase = if exhausted {
                PersistedSchemaMigrationPhase::RebuildingIndexes
            } else {
                PersistedSchemaMigrationPhase::RewritingRows
            };
            let advanced = record.transition(phase, progress)?;
            let operation = SchemaMigrationRecordOp::replace(record, &advanced)?;
            publish_migration_rewrite_page(effects, operation)?;
            active_migration_status(proposal, target, &advanced)
        }
        PersistedSchemaMigrationPhase::RebuildingIndexes => {
            if acknowledged_finding_page.is_some() {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
            let planned = recompile_active_physical_migration(db, proposal, record)?;
            let rebuilt = migration_derived_domain_count(db, &planned)?;
            let progress = record
                .progress()
                .begin_row_phase()?
                .with_index_progress(None, rebuilt)?;
            let validating =
                record.transition(PersistedSchemaMigrationPhase::FinalValidation, progress)?;
            publish_migration_record_replacement(record, &validating)?;
            active_migration_status(proposal, target, &validating)
        }
        PersistedSchemaMigrationPhase::FinalValidation => {
            if acknowledged_finding_page.is_some() {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
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
            if acknowledged_finding_page.is_some() {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
            publish_completed_physical_migration(db, proposal, record)
        }
        PersistedSchemaMigrationPhase::Applied | PersistedSchemaMigrationPhase::Aborted => {
            if acknowledged_finding_page.is_some() {
                return Err(InternalError::schema_migration(
                    SchemaMigrationCode::CandidateMismatch,
                ));
            }
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
    db.mark_all_registered_index_stores_ready();
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

#[cfg(feature = "migration")]
fn attach_ordinary_lineage_publication(
    proposal: &SchemaProposal,
    prior_head: &ExpectedAcceptedHead,
    accepted_head: &ExpectedAcceptedHead,
    candidates: &[CandidateSchemaRevision],
    operation: SchemaApplicationRecordOp,
) -> Result<Vec<DatabaseControlOp>, InternalError> {
    let mut operations = vec![DatabaseControlOp::SchemaApplication(operation)];
    let stored_before = load_entity_source_lineage_catalog()?;
    let planned = if matches!(prior_head, ExpectedAcceptedHead::Empty) {
        plan_initial_entity_source_lineage(proposal, candidates)
            .map_err(schema_migration_planning_error)?
    } else {
        Vec::new()
    };
    if planned.is_empty() && (stored_before.is_none() || prior_head == accepted_head) {
        return Ok(operations);
    }
    let before = stored_before.clone().unwrap_or_default();
    let after = lineage_after_planned(&before, planned.as_slice(), accepted_head)?;
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
    if let Some(record) = load_schema_migration_record()? {
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
    let applied = current_proposal_lineage_is_applied(db, proposal, target.accepted_head())?;
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
            };
            SchemaMigrationFinding::new(
                kind,
                finding.entity().value(),
                finding.primary_key().to_vec(),
            )
        })
        .collect();
    let phase = match record.phase() {
        PersistedSchemaMigrationPhase::Prepared => SchemaMigrationPhase::Prepared,
        PersistedSchemaMigrationPhase::Validating => SchemaMigrationPhase::Validating,
        PersistedSchemaMigrationPhase::ReadyToRewrite => SchemaMigrationPhase::ReadyToRewrite,
        PersistedSchemaMigrationPhase::RewritingRows => SchemaMigrationPhase::RewritingRows,
        PersistedSchemaMigrationPhase::RebuildingIndexes => SchemaMigrationPhase::RebuildingIndexes,
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
                bundle,
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
        SchemaMigrationPlanningError::Unadopted => SchemaMigrationCode::Unadopted,
        SchemaMigrationPlanningError::MissingMigration => SchemaMigrationCode::MissingMigration,
        SchemaMigrationPlanningError::VersionGap => SchemaMigrationCode::VersionGap,
        SchemaMigrationPlanningError::Downgrade => SchemaMigrationCode::Downgrade,
        SchemaMigrationPlanningError::EmptyEntityVersionBump => {
            SchemaMigrationCode::EmptyEntityVersionBump
        }
        SchemaMigrationPlanningError::StaleAcceptedHead => SchemaMigrationCode::StaleAcceptedHead,
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
    accepted_head: &ExpectedAcceptedHead,
) -> Result<bool, InternalError> {
    let Some(lineage) = load_entity_source_lineage_catalog()? else {
        return Ok(false);
    };
    let entities = proposal
        .fragments()
        .iter()
        .flat_map(icydb_schema::SchemaFragment::entities)
        .collect::<Vec<_>>();
    if entities.len() != lineage.entries().len() {
        return Ok(false);
    }
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
            matched = entry.accepted_head() == accepted_head
                && matches!(
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
                bundle,
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
        let current = lineage
            .get(next.store(), next.entity())
            .ok_or_else(InternalError::store_invariant)?;
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

fn lower_application_candidates(
    target: &SchemaApplicationTarget,
    proposal: &SchemaProposal,
    authorities: &[StoreApplicationAuthority],
) -> Result<LoweredApplication, InternalError> {
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
                let planned = plan_initial_entity_source_lineage(proposal, &candidates)
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
                        bundle,
                    })
                })
                .collect::<Vec<_>>();
            lower_existing_schema_proposal(proposal, stores.as_slice())?
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
        if authority.handle.with_data(DataStore::len) != 0
            || authority.handle.index_state() != IndexState::Ready
            || !authority.handle.with_index(IndexStore::is_empty)
        {
            return Err(InternalError::store_unsupported());
        }
    }
    Ok(())
}

/// Complete generated row-local additions only after a bounded exact proof.
///
/// Empty domains use maintained exact cardinality. At most one non-empty
/// activation may consume the canonical 0.211 exact scan budget. A journaled
/// proof that exceeds that page becomes one durable pending application;
/// volatile or additional non-empty proofs reject before publication.
fn preflight_existing_application(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<crate::db::schema::AcceptedSchemaRevisionBundle>],
    candidates: &mut [CandidateSchemaRevision],
) -> Result<Option<PendingGeneratedRowLocalConstraint>, InternalError> {
    require_empty_physical_entity_removal(authorities, current_bundles, candidates)?;
    require_empty_physical_field_removals(authorities, current_bundles, candidates)?;
    require_empty_physical_index_removals(authorities, current_bundles, candidates)?;
    require_empty_physical_relation_removals(authorities, current_bundles, candidates)?;
    let proofs = generated_row_local_constraint_proofs(authorities, current_bundles, candidates)?;
    if proofs
        .iter()
        .filter(|proof| proof.historical_rows != 0)
        .count()
        > 1
    {
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
            if proof.historical_rows != 0 {
                match validate_unpublished_row_local_candidate_bounded(
                    proof.store,
                    proof.store_path,
                    proof.entity_tag,
                    proof.entity_path.as_str(),
                    &candidate,
                    proof.constraint_id,
                )? {
                    UnpublishedRowLocalValidation::Complete { .. } => {}
                    UnpublishedRowLocalValidation::Incomplete => {
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
                current_bundles,
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
            let before = current
                .entity_snapshots()
                .get(entity_tag)
                .ok_or_else(InternalError::store_invariant)?;
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
                current_bundles,
                removed.target_path(),
            )?;
            target_store.with_index(|store| {
                prove_empty_reverse_relation_domain(store, *entity_tag, before, removed)
            })?;
        }
    }
    Ok(())
}

fn accepted_entity_store_for_path(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    entity_path: &str,
) -> Result<StoreHandle, InternalError> {
    let mut resolved = None;
    for (position, bundle) in current_bundles.iter().enumerate() {
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
            let before = current
                .entity_snapshots()
                .get(entity_tag)
                .ok_or_else(InternalError::store_invariant)?;
            if before.indexes().len() == after.indexes().len() {
                continue;
            }
            if before.indexes().len() != after.indexes().len().saturating_add(1) {
                return Err(InternalError::store_unsupported());
            }
            require_exact_empty_entity(authority.handle, *entity_tag)?;
            authority
                .handle
                .with_index(|store| prove_empty_user_index_domain(store, *entity_tag))
                .map_err(StagedUserIndexDomainError::into_internal_error)?;
        }
    }
    Ok(())
}

/// Prove that every dense field-removal candidate has no historical row to
/// rewrite. Missing or corrupt maintained cardinality fails closed.
fn require_empty_physical_field_removals(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
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
            let before = current
                .entity_snapshots()
                .get(entity_tag)
                .ok_or_else(InternalError::store_invariant)?;
            if before.row_layout() == after.row_layout() {
                continue;
            }
            if before.fields().len() != after.fields().len().saturating_add(1) {
                return Err(InternalError::store_unsupported());
            }
            require_exact_empty_entity(authority.handle, *entity_tag)?;
        }
    }
    Ok(())
}

// Prove exact logical emptiness from the maintained cardinality authority.
// Missing cardinality is corrupt state, not an empty domain or an unsupported
// user transition.
fn require_exact_empty_entity(
    store: StoreHandle,
    entity_tag: EntityTag,
) -> Result<(), InternalError> {
    require_exact_empty_entity_count(store.with_data(|data| data.exact_entity_count(entity_tag)))
}

fn require_exact_empty_entity_count(count: Option<u64>) -> Result<(), InternalError> {
    let count = count.ok_or_else(InternalError::store_corruption)?;
    if count != 0 {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
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
            let before = current
                .entity_snapshots()
                .get(entity_tag)
                .ok_or_else(InternalError::store_invariant)?;
            for constraint_id in added_generated_row_local_activations(before, after) {
                let historical_rows = authority
                    .handle
                    .with_data(|store| store.exact_entity_count(*entity_tag))
                    .ok_or_else(InternalError::store_corruption)?;
                proofs.push(DirectGeneratedRowLocalProof {
                    candidate_index,
                    store: authority.handle,
                    store_path: authority.path,
                    entity_tag: *entity_tag,
                    entity_path: after.entity_path().to_string(),
                    constraint_id,
                    historical_rows,
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
            let findings = receipt
                .findings()
                .iter()
                .map(|finding| {
                    let primary_key = finding
                        .primary_key()
                        .encoded_primary_key_bytes()
                        .ok_or_else(InternalError::store_invariant)?;
                    constraint_validation_finding_diagnostic(
                        snapshot,
                        activation,
                        snapshot.entity_path(),
                        primary_key,
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
    candidate_head: &ExpectedAcceptedHead,
    status: SchemaChangeProgressStatus,
) -> Result<SchemaChangeProgress, InternalError> {
    if schema_application_target(db)?.accepted_head() != candidate_head {
        return Err(InternalError::schema_application_conflict());
    }
    let receipt = SchemaChangeReceipt::new(
        record.receipt().database_identity(),
        record.receipt().submission_key().clone(),
        record.receipt().proposal_digest(),
        record.receipt().prior_head().clone(),
        SchemaChangeOutcome::Applied {
            accepted_head: candidate_head.clone(),
        },
    )?;
    let terminal = SchemaApplicationRecord::new(receipt.clone(), Vec::new())?;
    let operation = SchemaApplicationRecordOp::replace(record, &terminal)?;
    publish_accepted_schema_candidates_with_application_record(Vec::new(), operation)?;
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
    authorities.sort_unstable_by(|left, right| left.path.cmp(right.path));
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

fn derive_database_identity(
    incarnation: [u8; 16],
    stores: &[StoreApplicationAuthority],
) -> TargetDatabaseIdentity {
    let mut hasher = new_hash_sha256_prefixed(DATABASE_TARGET_FINGERPRINT_PROFILE);
    hasher.update(incarnation);
    write_hash_len_u32(&mut hasher, stores.len());
    for store in stores {
        write_store_authority(&mut hasher, store);
    }
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
            write_hash_tag_u8(hasher, allocation.memory_id());
            write_hash_str_u32(hasher, allocation.stable_key());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcceptedSchemaPublication, AcceptedStoreHead, DirectGeneratedRowLocalProof,
        PendingGeneratedRowLocalConstraint, abort_schema_application,
        aborted_generated_row_local_candidate, accepted_head_after_candidates,
        application_authorities, apply_schema, continue_schema_application, derive_accepted_head,
        derive_schema_change_job_id, ensure_recovered,
        final_candidates_for_pending_row_local_constraint, include_identity_state_count,
        lower_existing_schema_proposal, lower_initial_schema_proposal,
        publish_accepted_schema_candidates_with_application_record,
        require_exact_empty_entity_count, schema_application_target,
    };
    use crate::{
        db::{
            Db,
            commit::forget_recovered_domain_for_tests,
            data::DataStore,
            index::IndexStore,
            journal::JournalTailStore,
            registry::{
                StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
                StoreRuntimeStorageCapabilities,
            },
            schema::{
                AcceptedConstraintKind, AcceptedRuleOperation, AcceptedSchemaRevisionBundle,
                CandidateSchemaRevision, ConstraintOrigin, ConstraintValidationJob,
                ExistingProposalStore, ProposalStoreTarget, SchemaApplicationRecord,
                SchemaApplicationRecordOp, SchemaChangeActivation, SchemaChangeJob,
                SchemaChangeOutcome, SchemaChangeProgressStatus, SchemaStore,
            },
        },
        error::{ErrorClass, ErrorOrigin},
        testing::test_memory,
        traits::{CanisterKind, Path},
    };
    use icydb_schema::{
        ConstraintFragment, ConstraintSourceKey, DeclaredEntityVersion, EntityFragment,
        EntitySourceKey, EntityStoreAssignment, ExpectedAcceptedHead, ExpectedSchemaFingerprint,
        FieldFragment, FieldInsertPolicy, FieldSourceKey, FieldType, NamedTypeFragment,
        RuleSourceKey, ScalarLiteral, ScalarType, SchemaCapability, SchemaFragment, SchemaName,
        SchemaProposal, SchemaSubmissionKey, SourceCheckExpr, SourceCheckInstruction,
        SourceRuleOperation, TargetDatabaseIdentity, TargetStoreIdentity, TargetedRuleFragment,
        TypeSourceKey,
    };
    use std::cell::RefCell;

    #[cfg(feature = "migration")]
    use crate::{
        db::{
            DbSession, DynamicMutation, DynamicStructuralPatch, DynamicWriteCell,
            schema::SchemaChangeReceipt,
        },
        value::InputValue,
    };
    #[cfg(feature = "migration")]
    use icydb_schema::{
        EntityMigration, IndexFragment, IndexKeyFragment, RelationDeleteAction, RelationFragment,
        SchemaMigrationPlan, SchemaMigrationTransform, SchemaProposalDigest,
    };

    fn version_one() -> DeclaredEntityVersion {
        DeclaredEntityVersion::try_new(1).expect("fixture version should admit")
    }

    const ABORT_STORE_PATH: &str = "schema_application_tests::AbortStore";
    const EVOLUTION_STORE_PATH: &str = "schema_application_tests::EvolutionStore";
    #[cfg(feature = "migration")]
    const MIGRATION_STORE_PATH: &str = "schema_application_tests::MigrationStore";
    #[cfg(feature = "migration")]
    const MIGRATION_EXECUTION_STORE_PATH: &str =
        "schema_application_tests::MigrationExecutionStore";
    #[cfg(feature = "migration")]
    const MIGRATION_FINDING_STORE_PATH: &str = "schema_application_tests::MigrationFindingStore";

    #[test]
    fn database_identity_state_capacity_combines_store_inventories_exactly() {
        let below = include_identity_state_count(0, 65_535)
            .expect("the first store inventory should remain below the database cap");
        let exact = include_identity_state_count(below, 1)
            .expect("the combined database boundary should admit");
        assert_eq!(exact, 65_536);

        let error = include_identity_state_count(exact, 1)
            .expect_err("the next owner in another store must reject");
        assert_eq!(error.class(), ErrorClass::Unsupported);
        assert_eq!(error.origin(), ErrorOrigin::Identity);
    }

    #[test]
    fn exact_empty_entity_proof_distinguishes_corruption_from_non_empty_input() {
        let corrupt = require_exact_empty_entity_count(None)
            .expect_err("uninspectable cardinality must fail closed");
        assert_eq!(corrupt.class(), ErrorClass::Corruption);

        let non_empty = require_exact_empty_entity_count(Some(1))
            .expect_err("non-empty cardinality must reject removal");
        assert_eq!(non_empty.class(), ErrorClass::Unsupported);
        assert!(require_exact_empty_entity_count(Some(0)).is_ok());
    }

    thread_local! {
        static ABORT_DATA: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(180)));
        static ABORT_INDEX: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(181)));
        static ABORT_SCHEMA: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(182)));
        static ABORT_JOURNAL: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(183)));
        static ABORT_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_journaled_store(
                ABORT_STORE_PATH,
                &ABORT_DATA,
                &ABORT_INDEX,
                &ABORT_SCHEMA,
                &ABORT_JOURNAL,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(180, "icydb.test.application-abort.data.v1"),
                    StoreAllocationIdentity::new(181, "icydb.test.application-abort.index.v1"),
                    StoreAllocationIdentity::new(182, "icydb.test.application-abort.schema.v1"),
                    StoreAllocationIdentity::new(183, "icydb.test.application-abort.journal.v1"),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ).expect("abort journaled store should register");
            registry
        };
    }

    #[cfg(feature = "migration")]
    thread_local! {
        static MIGRATION_EXECUTION_DATA: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(210)));
        static MIGRATION_EXECUTION_INDEX: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(211)));
        static MIGRATION_EXECUTION_SCHEMA: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(212)));
        static MIGRATION_EXECUTION_JOURNAL: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(213)));
        static MIGRATION_EXECUTION_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_journaled_store(
                MIGRATION_EXECUTION_STORE_PATH,
                &MIGRATION_EXECUTION_DATA,
                &MIGRATION_EXECUTION_INDEX,
                &MIGRATION_EXECUTION_SCHEMA,
                &MIGRATION_EXECUTION_JOURNAL,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(210, "icydb.test.migration-execution.data.v1"),
                    StoreAllocationIdentity::new(211, "icydb.test.migration-execution.index.v1"),
                    StoreAllocationIdentity::new(212, "icydb.test.migration-execution.schema.v1"),
                    StoreAllocationIdentity::new(213, "icydb.test.migration-execution.journal.v1"),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ).expect("migration execution store should register");
            registry
        };
    }

    #[cfg(feature = "migration")]
    thread_local! {
        static MIGRATION_DATA: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(200)));
        static MIGRATION_INDEX: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(201)));
        static MIGRATION_SCHEMA: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(202)));
        static MIGRATION_JOURNAL: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(203)));
        static MIGRATION_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_journaled_store(
                MIGRATION_STORE_PATH,
                &MIGRATION_DATA,
                &MIGRATION_INDEX,
                &MIGRATION_SCHEMA,
                &MIGRATION_JOURNAL,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(200, "icydb.test.migration-validation.data.v1"),
                    StoreAllocationIdentity::new(201, "icydb.test.migration-validation.index.v1"),
                    StoreAllocationIdentity::new(202, "icydb.test.migration-validation.schema.v1"),
                    StoreAllocationIdentity::new(203, "icydb.test.migration-validation.journal.v1"),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ).expect("migration validation store should register");
            registry
        };
    }

    #[cfg(feature = "migration")]
    thread_local! {
        static MIGRATION_FINDING_DATA: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(206)));
        static MIGRATION_FINDING_INDEX: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(207)));
        static MIGRATION_FINDING_SCHEMA: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(208)));
        static MIGRATION_FINDING_JOURNAL: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(209)));
        static MIGRATION_FINDING_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_journaled_store(
                MIGRATION_FINDING_STORE_PATH,
                &MIGRATION_FINDING_DATA,
                &MIGRATION_FINDING_INDEX,
                &MIGRATION_FINDING_SCHEMA,
                &MIGRATION_FINDING_JOURNAL,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(206, "icydb.test.migration-finding.data.v1"),
                    StoreAllocationIdentity::new(207, "icydb.test.migration-finding.index.v1"),
                    StoreAllocationIdentity::new(208, "icydb.test.migration-finding.schema.v1"),
                    StoreAllocationIdentity::new(209, "icydb.test.migration-finding.journal.v1"),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ).expect("migration finding store should register");
            registry
        };
    }

    thread_local! {
        static EVOLUTION_DATA: RefCell<DataStore> =
            RefCell::new(DataStore::init_journaled(test_memory(192)));
        static EVOLUTION_INDEX: RefCell<IndexStore> =
            RefCell::new(IndexStore::init_journaled(test_memory(193)));
        static EVOLUTION_SCHEMA: RefCell<SchemaStore> =
            RefCell::new(SchemaStore::init_journaled(test_memory(194)));
        static EVOLUTION_JOURNAL: RefCell<JournalTailStore> =
            RefCell::new(JournalTailStore::init(test_memory(195)));
        static EVOLUTION_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_journaled_store(
                EVOLUTION_STORE_PATH,
                &EVOLUTION_DATA,
                &EVOLUTION_INDEX,
                &EVOLUTION_SCHEMA,
                &EVOLUTION_JOURNAL,
                StoreAllocationIdentities::new_journaled(
                    StoreAllocationIdentity::new(192, "icydb.test.rule-evolution.data.v1"),
                    StoreAllocationIdentity::new(193, "icydb.test.rule-evolution.index.v1"),
                    StoreAllocationIdentity::new(194, "icydb.test.rule-evolution.schema.v1"),
                    StoreAllocationIdentity::new(195, "icydb.test.rule-evolution.journal.v1"),
                ),
                StoreRuntimeStorageCapabilities::journaled(),
            ).expect("rule-evolution journaled store should register");
            registry
        };
    }

    struct AbortCanister;

    impl Path for AbortCanister {
        const PATH: &'static str = "schema_application_tests::AbortCanister";
    }

    impl CanisterKind for AbortCanister {
        const COMMIT_MEMORY_ID: u8 = 184;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.application-abort.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 185;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.application-abort.integrity.v1";
    }

    struct EvolutionCanister;

    impl Path for EvolutionCanister {
        const PATH: &'static str = "schema_application_tests::EvolutionCanister";
    }

    impl CanisterKind for EvolutionCanister {
        const COMMIT_MEMORY_ID: u8 = 196;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.rule-evolution.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 197;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.rule-evolution.integrity.v1";
    }

    #[cfg(feature = "migration")]
    struct MigrationCanister;

    #[cfg(feature = "migration")]
    impl Path for MigrationCanister {
        const PATH: &'static str = "schema_application_tests::MigrationCanister";
    }

    #[cfg(feature = "migration")]
    impl CanisterKind for MigrationCanister {
        const COMMIT_MEMORY_ID: u8 = 204;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.migration-validation.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 205;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.migration-validation.integrity.v1";
    }

    #[cfg(feature = "migration")]
    struct MigrationExecutionCanister;

    #[cfg(feature = "migration")]
    impl Path for MigrationExecutionCanister {
        const PATH: &'static str = "schema_application_tests::MigrationExecutionCanister";
    }

    #[cfg(feature = "migration")]
    impl CanisterKind for MigrationExecutionCanister {
        const COMMIT_MEMORY_ID: u8 = 214;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.migration-execution.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 215;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.migration-execution.integrity.v1";
    }

    #[cfg(feature = "migration")]
    struct MigrationFindingCanister;

    #[cfg(feature = "migration")]
    impl Path for MigrationFindingCanister {
        const PATH: &'static str = "schema_application_tests::MigrationFindingCanister";
    }

    #[cfg(feature = "migration")]
    impl CanisterKind for MigrationFindingCanister {
        const COMMIT_MEMORY_ID: u8 = 210;
        const COMMIT_STABLE_KEY: &'static str = "icydb.test.migration-finding.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 211;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.test.migration-finding.integrity.v1";
    }

    fn name(value: &str) -> SchemaName {
        SchemaName::try_new(value).expect("test schema name should admit")
    }

    fn generated_check_proposal(
        expected_head: ExpectedAcceptedHead,
        submission_key: &str,
        include_check: bool,
        database: TargetDatabaseIdentity,
        store: TargetStoreIdentity,
    ) -> (SchemaProposal, EntitySourceKey, ConstraintSourceKey) {
        let entity_source = EntitySourceKey::try_new("Item").expect("entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("id source should admit");
        let score_source = FieldSourceKey::try_new("score").expect("score source should admit");
        let check_source =
            ConstraintSourceKey::try_new("score_non_negative").expect("check source should admit");
        let check = SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(score_source),
            SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
            SourceCheckInstruction::GreaterThanOrEqual,
        ])
        .expect("check expression should admit");
        let constraints = include_check
            .then(|| ConstraintFragment::check(name("score_non_negative"), check))
            .into_iter()
            .collect();
        let entity = EntityFragment::try_new(
            name("Item"),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("score"),
                    FieldType::Scalar(ScalarType::Int64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id_source],
            Vec::new(),
            Vec::new(),
            constraints,
        )
        .expect("entity should admit");
        let proposal = SchemaProposal::try_compose(
            vec![SchemaCapability::ACCEPTED_CHECKS],
            database,
            SchemaSubmissionKey::try_new(submission_key).expect("submission key should admit"),
            expected_head,
            vec![
                SchemaFragment::try_new(vec![entity], Vec::new())
                    .expect("schema fragment should admit"),
            ],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
            None,
        )
        .expect("schema proposal should compose");
        (proposal, entity_source, check_source)
    }

    #[cfg(feature = "migration")]
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum ValidationMigrationShape {
        Clean,
        AllFindingFamilies,
    }

    #[cfg(feature = "migration")]
    #[expect(
        clippy::too_many_lines,
        reason = "the fixture keeps both predecessor and candidate source contracts adjacent"
    )]
    fn validation_migration_proposal(
        shape: ValidationMigrationShape,
        current: bool,
        expected_head: ExpectedAcceptedHead,
        database: TargetDatabaseIdentity,
        store: TargetStoreIdentity,
    ) -> SchemaProposal {
        let entity_source = EntitySourceKey::try_new("MigratingItem")
            .expect("migration entity source should admit");
        let old_value =
            FieldSourceKey::try_new("old_value").expect("predecessor field source should admit");
        let current_value =
            FieldSourceKey::try_new("value").expect("candidate field source should admit");
        let target_entity = EntitySourceKey::try_new("MigrationTarget")
            .expect("migration target source should admit");
        let target_id = FieldSourceKey::try_new("id").expect("target id source should admit");
        let constraint = SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(current_value.clone()),
            SourceCheckInstruction::Literal(ScalarLiteral::Nat(8)),
            SourceCheckInstruction::LessThanOrEqual,
        ])
        .expect("candidate check should admit");
        let findings = shape == ValidationMigrationShape::AllFindingFamilies;
        let entity = EntityFragment::try_new(
            name("MigratingItem"),
            DeclaredEntityVersion::try_new(if current { 2 } else { 1 })
                .expect("migration version should admit"),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name(if current { "value" } else { "old_value" }),
                    FieldType::Scalar(if current {
                        ScalarType::Nat8
                    } else {
                        ScalarType::Int64
                    }),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![FieldSourceKey::try_new("id").expect("id source should admit")],
            current
                .then(|| {
                    IndexFragment::try_new(
                        name("value_unique"),
                        vec![IndexKeyFragment::Field(current_value.clone())],
                        true,
                        None,
                    )
                    .expect("candidate index should admit")
                })
                .into_iter()
                .collect(),
            (current && findings)
                .then(|| {
                    RelationFragment::try_new(
                        name("value_target"),
                        vec![current_value.clone()],
                        target_entity.clone(),
                        vec![target_id.clone()],
                        RelationDeleteAction::Restrict,
                    )
                    .expect("candidate relation should admit")
                })
                .into_iter()
                .collect(),
            (current && findings)
                .then(|| ConstraintFragment::check(name("value_at_most_eight"), constraint))
                .into_iter()
                .collect(),
        )
        .expect("migration entity should admit");
        let target = EntityFragment::try_new(
            name("MigrationTarget"),
            version_one(),
            vec![FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat8),
                false,
                FieldInsertPolicy::Required,
                None,
            )],
            vec![target_id],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("migration relation target should admit");
        let migration = current.then(|| {
            SchemaMigrationPlan::try_new(vec![
                EntityMigration::try_new(
                    entity_source.clone(),
                    DeclaredEntityVersion::try_new(1).expect("predecessor should admit"),
                    None,
                    Vec::new(),
                    vec![SchemaMigrationTransform::CheckedCast {
                        from: old_value.clone(),
                        to: current_value,
                        target: ScalarType::Nat8,
                    }],
                )
                .expect("migration transition should admit"),
            ])
            .expect("migration plan should admit")
        });
        let mut capabilities = Vec::new();
        if current && findings {
            capabilities.extend([
                SchemaCapability::ACCEPTED_CHECKS,
                SchemaCapability::SECONDARY_INDEXES,
                SchemaCapability::RESTRICTIVE_RELATIONS,
            ]);
        }
        if migration.is_some() {
            capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
        }
        let mut entities = vec![entity];
        let mut assignments = vec![EntityStoreAssignment::new(entity_source.clone(), store)];
        if findings {
            entities.push(target);
            assignments.push(EntityStoreAssignment::new(target_entity, store));
        }
        SchemaProposal::try_compose(
            capabilities,
            database,
            SchemaSubmissionKey::try_new(if current {
                "migration-validation-v2"
            } else {
                "migration-validation-v1"
            })
            .expect("submission should admit"),
            expected_head,
            vec![
                SchemaFragment::try_new(entities, Vec::new())
                    .expect("migration fragment should admit"),
            ],
            assignments,
            current
                .then_some(icydb_schema::SchemaRemoval::Field {
                    entity: entity_source,
                    field: old_value,
                })
                .into_iter()
                .collect(),
            migration,
        )
        .expect("migration proposal should compose")
    }

    fn targeted_rule_proposal(
        expected_head: ExpectedAcceptedHead,
        submission_key: &str,
        operation: SourceRuleOperation,
        database: TargetDatabaseIdentity,
        store: TargetStoreIdentity,
    ) -> (SchemaProposal, EntitySourceKey, ConstraintSourceKey) {
        let entity_source =
            EntitySourceKey::try_new("Measured").expect("entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("id source should admit");
        let value_source = FieldSourceKey::try_new("value").expect("value source should admit");
        let value_type = TypeSourceKey::try_new("Measure").expect("type source should admit");
        let rule_source = RuleSourceKey::try_new("limit").expect("rule source should admit");
        let constraint_source =
            ConstraintSourceKey::for_targeted_field_rule(&value_source, &value_type, &rule_source);
        let entity = EntityFragment::try_new(
            name("Measured"),
            version_one(),
            vec![
                FieldFragment::new(
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    name("value"),
                    FieldType::Named(value_type.clone()),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
            ],
            vec![id_source],
            Vec::new(),
            Vec::new(),
            vec![ConstraintFragment::targeted_rule(
                TargetedRuleFragment::new(value_source, value_type, name("limit"), operation),
            )],
        )
        .expect("targeted entity should admit");
        let proposal = SchemaProposal::try_compose(
            vec![SchemaCapability::ACCEPTED_CHECKS],
            database,
            SchemaSubmissionKey::try_new(submission_key).expect("submission key should admit"),
            expected_head,
            vec![
                SchemaFragment::try_new(
                    vec![entity],
                    vec![NamedTypeFragment::newtype(
                        name("Measure"),
                        FieldType::Scalar(ScalarType::Nat8),
                    )],
                )
                .expect("schema fragment should admit"),
            ],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
            None,
        )
        .expect("schema proposal should compose");
        (proposal, entity_source, constraint_source)
    }

    #[test]
    fn database_head_is_empty_only_when_every_store_root_is_absent() {
        assert_eq!(
            derive_accepted_head(&[("test::A", None), ("test::B", None)]),
            ExpectedAcceptedHead::Empty,
        );
    }

    #[test]
    fn database_head_covers_store_path_revision_fingerprint_and_absence() {
        let first = derive_accepted_head(&[
            (
                "test::A",
                Some(AcceptedStoreHead {
                    revision: 3,
                    fingerprint: [0x11; 32],
                }),
            ),
            ("test::B", None),
        ]);
        let changed_fingerprint = derive_accepted_head(&[
            (
                "test::A",
                Some(AcceptedStoreHead {
                    revision: 3,
                    fingerprint: [0x12; 32],
                }),
            ),
            ("test::B", None),
        ]);
        let changed_absence = derive_accepted_head(&[
            (
                "test::A",
                Some(AcceptedStoreHead {
                    revision: 3,
                    fingerprint: [0x11; 32],
                }),
            ),
            (
                "test::B",
                Some(AcceptedStoreHead {
                    revision: 1,
                    fingerprint: [0x22; 32],
                }),
            ),
        ]);

        assert_ne!(first, changed_fingerprint);
        assert_ne!(first, changed_absence);
        assert!(matches!(
            first,
            ExpectedAcceptedHead::Exact { revision: 3, .. }
        ));
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the end-to-end catalog assertion is clearer as one lifecycle test"
    )]
    fn generated_check_abort_retires_source_identity_and_allows_fresh_reproposal() {
        let database = TargetDatabaseIdentity::from_bytes([0x71; 32]);
        let store = TargetStoreIdentity::from_bytes([0x72; 32]);
        let (initial, entity_source, _) = generated_check_proposal(
            ExpectedAcceptedHead::Empty,
            "abort-initial",
            false,
            database,
            store,
        );
        let initial_candidate = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "abort::Store",
                identity: store,
            }],
        )
        .expect("initial proposal should lower")
        .pop()
        .expect("initial proposal should produce one candidate");
        let (with_check, _, check_source) = generated_check_proposal(
            ExpectedAcceptedHead::Exact {
                revision: 1,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x73; 32]),
            },
            "abort-add-check",
            true,
            database,
            store,
        );
        let pending_candidate = lower_existing_schema_proposal(
            &with_check,
            &[ExistingProposalStore {
                path: "abort::Store",
                identity: store,
                bundle: initial_candidate.bundle(),
            }],
        )
        .expect("generated check should lower")
        .pop()
        .expect("generated check should produce one candidate");
        let entity_tag = pending_candidate
            .bundle()
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should remain bound");
        let constraint_id = pending_candidate
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &check_source)
            .expect("generated check source should bind");
        let pending_snapshot = pending_candidate
            .bundle()
            .entity_snapshots()
            .get(&entity_tag)
            .expect("pending entity should exist");
        let activation = pending_snapshot
            .constraint_catalog()
            .activation(constraint_id)
            .expect("generated check should remain an activation");
        assert_eq!(activation.origin(), ConstraintOrigin::Generated);

        let aborted = aborted_generated_row_local_candidate(
            pending_candidate.bundle(),
            entity_tag,
            constraint_id,
        )
        .expect("generated check abort should build one catalog-native candidate");
        let aborted_snapshot = aborted
            .bundle()
            .entity_snapshots()
            .get(&entity_tag)
            .expect("aborted entity should remain");
        assert!(
            aborted_snapshot
                .constraint_catalog()
                .activation(constraint_id)
                .is_none(),
        );
        assert_eq!(aborted_snapshot.row_layout(), pending_snapshot.row_layout());
        assert!(
            aborted
                .bundle()
                .source_bindings_for_tests()
                .constraint(entity_tag, &check_source)
                .is_none(),
        );

        let reproposed = lower_existing_schema_proposal(
            &with_check,
            &[ExistingProposalStore {
                path: "abort::Store",
                identity: store,
                bundle: aborted.bundle(),
            }],
        )
        .expect("aborted generated check should be independently reproposable")
        .pop()
        .expect("reproposal should produce one candidate");
        let replacement_id = reproposed
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &check_source)
            .expect("reproposal should bind a fresh constraint identity");
        assert!(
            replacement_id > constraint_id,
            "aborted accepted IDs must remain retired",
        );
    }

    #[test]
    fn targeted_rule_edit_abort_keeps_prior_accepted_semantics_and_source_identity() {
        let database = TargetDatabaseIdentity::from_bytes([0x81; 32]);
        let store = TargetStoreIdentity::from_bytes([0x82; 32]);
        let (initial, entity_source, constraint_source) = targeted_rule_proposal(
            ExpectedAcceptedHead::Empty,
            "targeted-abort-initial",
            SourceRuleOperation::NumericRangeInclusive {
                min: ScalarLiteral::Nat(0),
                max: ScalarLiteral::Nat(10),
            },
            database,
            store,
        );
        let initial_candidate = lower_initial_schema_proposal(
            &initial,
            &[ProposalStoreTarget {
                path: "abort::TargetedStore",
                identity: store,
            }],
        )
        .expect("initial targeted proposal should lower")
        .pop()
        .expect("initial targeted proposal should produce one candidate");
        let initial_bundle = initial_candidate.bundle();
        let entity_tag = initial_bundle
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should bind");
        let constraint_id = initial_bundle
            .source_bindings_for_tests()
            .constraint(entity_tag, &constraint_source)
            .expect("targeted source should bind");
        let high_water = initial_bundle.entity_snapshots()[&entity_tag]
            .constraint_id_allocator()
            .high_water();
        let (edited, _, _) = targeted_rule_proposal(
            ExpectedAcceptedHead::Exact {
                revision: initial_bundle.revision().get(),
                fingerprint: ExpectedSchemaFingerprint::from_bytes([0x83; 32]),
            },
            "targeted-abort-edit",
            SourceRuleOperation::NumericMaximumInclusive {
                value: ScalarLiteral::Nat(8),
            },
            database,
            store,
        );
        let staged = lower_existing_schema_proposal(
            &edited,
            &[ExistingProposalStore {
                path: "abort::TargetedStore",
                identity: store,
                bundle: initial_bundle,
            }],
        )
        .expect("targeted semantic edit should stage")
        .pop()
        .expect("targeted semantic edit should produce one candidate");
        let aborted =
            aborted_generated_row_local_candidate(staged.bundle(), entity_tag, constraint_id)
                .expect("targeted semantic edit should abort through catalog authority");
        let snapshot = &aborted.bundle().entity_snapshots()[&entity_tag];

        assert!(
            snapshot
                .constraint_catalog()
                .activation(constraint_id)
                .is_none()
        );
        assert_eq!(snapshot.constraint_id_allocator().high_water(), high_water);
        assert_eq!(
            aborted
                .bundle()
                .source_bindings_for_tests()
                .constraint(entity_tag, &constraint_source),
            Some(constraint_id),
        );
        assert!(snapshot.constraints().iter().any(|constraint| {
            constraint.id() == constraint_id
                && matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::TargetedRule { operation, .. }
                        if matches!(
                            operation.as_ref(),
                            AcceptedRuleOperation::NumericRangeInclusive { .. }
                        )
                )
        }));
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the staged publication, recovery, and promotion assertions form one lifecycle"
    )]
    fn targeted_rule_edit_activation_recovers_and_promotes_without_source_model() {
        let db = Db::<EvolutionCanister>::new(&EVOLUTION_REGISTRY);
        let empty_target =
            schema_application_target(&db).expect("empty evolution target should issue");
        let store_identity = empty_target
            .stores()
            .first()
            .expect("evolution store should register")
            .identity();
        let (initial, entity_source, constraint_source) = targeted_rule_proposal(
            empty_target.accepted_head().clone(),
            "targeted-recovery-initial",
            SourceRuleOperation::NumericRangeInclusive {
                min: ScalarLiteral::Nat(0),
                max: ScalarLiteral::Nat(10),
            },
            empty_target.database_identity(),
            store_identity,
        );
        assert!(matches!(
            apply_schema(&db, &initial)
                .expect("initial targeted proposal should publish")
                .outcome(),
            SchemaChangeOutcome::Applied { .. },
        ));

        let direct_target =
            schema_application_target(&db).expect("direct evolution target should issue");
        let (direct_edit, _, _) = targeted_rule_proposal(
            direct_target.accepted_head().clone(),
            "targeted-direct-edit",
            SourceRuleOperation::NumericMaximumInclusive {
                value: ScalarLiteral::Nat(8),
            },
            direct_target.database_identity(),
            store_identity,
        );
        assert!(matches!(
            apply_schema(&db, &direct_edit)
                .expect("empty-domain semantic edit should publish directly")
                .outcome(),
            SchemaChangeOutcome::Applied { .. },
        ));
        let store = db
            .store_handle(EVOLUTION_STORE_PATH)
            .expect("evolution store should resolve");
        let direct = store
            .with_schema(SchemaStore::current_accepted_schema_bundle)
            .expect("directly edited bundle should remain readable")
            .expect("directly edited bundle should exist");
        let entity_tag = direct
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should remain bound");
        let constraint_id = direct
            .source_bindings_for_tests()
            .constraint(entity_tag, &constraint_source)
            .expect("direct edit should preserve constraint identity");
        assert!(
            direct.entity_snapshots()[&entity_tag]
                .constraint_catalog()
                .activation(constraint_id)
                .is_none()
        );
        assert!(
            direct.entity_snapshots()[&entity_tag]
                .constraints()
                .iter()
                .any(|constraint| {
                    constraint.id() == constraint_id
                        && matches!(
                            constraint.kind(),
                            AcceptedConstraintKind::TargetedRule { operation, .. }
                                if matches!(
                                    operation.as_ref(),
                                    AcceptedRuleOperation::NumericMaximumInclusive { .. }
                                )
                        )
                })
        );

        let target = schema_application_target(&db).expect("staged evolution target should issue");
        let (edited, _, _) = targeted_rule_proposal(
            target.accepted_head().clone(),
            "targeted-recovery-edit",
            SourceRuleOperation::MultipleOf {
                divisor: ScalarLiteral::Nat(2),
            },
            target.database_identity(),
            store_identity,
        );
        let current = store
            .with_schema(SchemaStore::current_accepted_schema_bundle)
            .expect("accepted evolution bundle should remain readable")
            .expect("directly edited evolution bundle should exist");
        let staged = lower_existing_schema_proposal(
            &edited,
            &[ExistingProposalStore {
                path: EVOLUTION_STORE_PATH,
                identity: store_identity,
                bundle: &current,
            }],
        )
        .expect("targeted edit should stage")
        .pop()
        .expect("targeted edit should produce one candidate");
        assert_eq!(
            staged
                .bundle()
                .source_bindings_for_tests()
                .constraint(entity_tag, &constraint_source),
            Some(constraint_id),
        );
        let proof = DirectGeneratedRowLocalProof {
            candidate_index: 0,
            store,
            store_path: EVOLUTION_STORE_PATH,
            entity_tag,
            entity_path: staged.bundle().entity_snapshots()[&entity_tag]
                .entity_path()
                .to_string(),
            constraint_id,
            historical_rows: 0,
        };
        let final_candidates = final_candidates_for_pending_row_local_constraint(
            std::slice::from_ref(&staged),
            &PendingGeneratedRowLocalConstraint { proof },
        )
        .expect("final semantic replacement should derive without source input");
        let authorities = application_authorities(&db);
        let candidate_head =
            accepted_head_after_candidates(authorities.as_slice(), &final_candidates)
                .expect("final candidate head should derive");
        let digest = edited.digest().expect("proposal digest should derive");
        let job_id = derive_schema_change_job_id(
            target.database_identity(),
            edited.submission_key(),
            digest,
            target.accepted_head(),
        )
        .expect("job identity should derive");
        let receipt = crate::db::schema::SchemaChangeReceipt::new(
            target.database_identity(),
            edited.submission_key().clone(),
            digest,
            target.accepted_head().clone(),
            SchemaChangeOutcome::Pending {
                job: SchemaChangeJob::new(job_id),
                candidate_head,
            },
        )
        .expect("pending replacement receipt should admit");
        let record = SchemaApplicationRecord::new(
            receipt,
            vec![
                SchemaChangeActivation::new(
                    store_identity,
                    entity_tag.value(),
                    constraint_id.get(),
                )
                .expect("replacement activation should admit"),
            ],
        )
        .expect("pending replacement record should admit");
        let operation =
            SchemaApplicationRecordOp::insert(&record).expect("pending insert should prepare");
        publish_accepted_schema_candidates_with_application_record(
            vec![AcceptedSchemaPublication::new(
                EVOLUTION_STORE_PATH,
                store,
                current.revision(),
                &staged,
            )],
            operation,
        )
        .expect("staged replacement and record should publish atomically");

        forget_recovered_domain_for_tests(&db).expect("upgrade should reset recovery ownership");
        ensure_recovered(&db).expect("recovery should restore the staged accepted activation");

        let recovered = store
            .with_schema(SchemaStore::current_accepted_schema_bundle)
            .expect("recovered staged bundle should decode")
            .expect("recovered staged bundle should exist");
        let recovered_snapshot = recovered.entity_snapshots()[&entity_tag].clone();
        let validating_catalog = recovered_snapshot
            .constraint_catalog()
            .clone()
            .with_validation_started(constraint_id)
            .expect("recovered replacement should enter validation");
        let mut validating_snapshots = recovered.entity_snapshots().clone();
        validating_snapshots.insert(
            entity_tag,
            recovered_snapshot.with_constraint_catalog(validating_catalog),
        );
        let validating_bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
            recovered
                .revision()
                .checked_next()
                .expect("validation revision should remain available"),
            recovered.store_path(),
            recovered.enum_catalog().clone(),
            recovered.composite_catalog().clone(),
            recovered.source_bindings_for_tests().clone(),
            validating_snapshots,
        )
        .expect("validating replacement bundle should close");
        let validating_candidate = CandidateSchemaRevision::new(validating_bundle)
            .expect("validating replacement candidate should encode");
        let validating_activation = validating_candidate.bundle().entity_snapshots()[&entity_tag]
            .constraint_catalog()
            .activation(constraint_id)
            .expect("validating replacement activation should remain present");
        let validation_job = ConstraintValidationJob::start(
            entity_tag,
            validating_candidate.bundle().entity_snapshots()[&entity_tag]
                .entity_path()
                .to_string(),
            validating_activation,
            None,
        )
        .expect("validating replacement job should derive from accepted state");
        store
            .with_schema(|schema| {
                schema.validate_live_activation_transition(validating_candidate.bundle())?;
                schema.validate_constraint_validation_job_closure_with_change(
                    validating_candidate.bundle(),
                    Some(&validation_job),
                    None,
                )
            })
            .expect("validating replacement transition and job should close");

        let started = continue_schema_application(&db, job_id, None).unwrap_or_else(|error| {
            panic!(
                "recovered replacement should begin validation: {:?}/{:?}",
                error.class(),
                error.origin(),
            )
        });
        assert_eq!(started.status(), &SchemaChangeProgressStatus::Started);
        let mut applied = None;
        for _ in 0..8 {
            let progress = continue_schema_application(&db, job_id, None)
                .expect("replacement validation should advance from durable authority");
            if progress.status() == &SchemaChangeProgressStatus::Applied {
                applied = Some(progress);
                break;
            }
        }
        let applied = applied.expect("empty historical domain should promote within bounded steps");
        assert!(matches!(
            applied.receipt().outcome(),
            SchemaChangeOutcome::Applied { .. }
        ));
        let promoted = store
            .with_schema(SchemaStore::current_accepted_schema_bundle)
            .expect("promoted bundle should remain readable")
            .expect("promoted bundle should exist");
        let snapshot = &promoted.entity_snapshots()[&entity_tag];
        assert!(
            snapshot
                .constraint_catalog()
                .activation(constraint_id)
                .is_none()
        );
        assert_eq!(
            promoted
                .source_bindings_for_tests()
                .constraint(entity_tag, &constraint_source),
            Some(constraint_id),
        );
        assert!(snapshot.constraints().iter().any(|constraint| {
            constraint.id() == constraint_id
                && matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::TargetedRule { operation, .. }
                        if matches!(
                            operation.as_ref(),
                            AcceptedRuleOperation::MultipleOf { .. }
                        )
                )
        }));
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the journaled abort, replay, and recovery assertions form one scenario"
    )]
    fn pending_generated_check_abort_is_atomic_terminal_and_replayable() {
        let db = Db::<AbortCanister>::new(&ABORT_REGISTRY);
        let empty_target =
            schema_application_target(&db).expect("empty application target should issue");
        let store_identity = empty_target
            .stores()
            .first()
            .expect("abort store should be registered")
            .identity();
        let (initial, entity_source, _) = generated_check_proposal(
            empty_target.accepted_head().clone(),
            "abort-runtime-initial",
            false,
            empty_target.database_identity(),
            store_identity,
        );
        assert!(matches!(
            apply_schema(&db, &initial)
                .expect("initial application should publish")
                .outcome(),
            SchemaChangeOutcome::Applied { .. },
        ));

        let target =
            schema_application_target(&db).expect("existing application target should issue");
        let (with_check, _, check_source) = generated_check_proposal(
            target.accepted_head().clone(),
            "abort-runtime-pending",
            true,
            target.database_identity(),
            store_identity,
        );
        let store = db
            .store_handle(ABORT_STORE_PATH)
            .expect("abort store should resolve");
        let current = store
            .with_schema(SchemaStore::current_accepted_schema_bundle)
            .expect("accepted bundle should remain readable")
            .expect("initial accepted bundle should exist");
        let pending_candidate = lower_existing_schema_proposal(
            &with_check,
            &[ExistingProposalStore {
                path: ABORT_STORE_PATH,
                identity: store_identity,
                bundle: &current,
            }],
        )
        .expect("pending generated check should lower")
        .pop()
        .expect("pending generated check should produce one candidate");
        let entity_tag = pending_candidate
            .bundle()
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should bind");
        let constraint_id = pending_candidate
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &check_source)
            .expect("generated check source should bind");
        let digest = with_check.digest().expect("proposal digest should derive");
        let job_id = derive_schema_change_job_id(
            target.database_identity(),
            with_check.submission_key(),
            digest,
            target.accepted_head(),
        )
        .expect("job identity should derive");
        let receipt = crate::db::schema::SchemaChangeReceipt::new(
            target.database_identity(),
            with_check.submission_key().clone(),
            digest,
            target.accepted_head().clone(),
            SchemaChangeOutcome::Pending {
                job: SchemaChangeJob::new(job_id),
                candidate_head: ExpectedAcceptedHead::Exact {
                    revision: pending_candidate.revision().get().saturating_add(2),
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([0x76; 32]),
                },
            },
        )
        .expect("pending receipt should admit");
        let record = SchemaApplicationRecord::new(
            receipt,
            vec![
                SchemaChangeActivation::new(
                    store_identity,
                    entity_tag.value(),
                    constraint_id.get(),
                )
                .expect("application activation should admit"),
            ],
        )
        .expect("pending application record should admit");
        let operation =
            SchemaApplicationRecordOp::insert(&record).expect("pending insert should prepare");
        publish_accepted_schema_candidates_with_application_record(
            vec![AcceptedSchemaPublication::new(
                ABORT_STORE_PATH,
                store,
                current.revision(),
                &pending_candidate,
            )],
            operation,
        )
        .expect("pending candidate and record should publish atomically");

        let started = continue_schema_application(&db, job_id, None)
            .expect("first continuation should durably start validation");
        assert_eq!(started.status(), &SchemaChangeProgressStatus::Started);
        let progress =
            abort_schema_application(&db, job_id, None).expect("pending application should abort");
        assert_eq!(progress.status(), &SchemaChangeProgressStatus::Aborted);
        assert!(matches!(
            progress.receipt().outcome(),
            SchemaChangeOutcome::Aborted { .. },
        ));
        let replay =
            abort_schema_application(&db, job_id, None).expect("terminal abort should replay");
        assert_eq!(replay, progress);
        assert_eq!(
            continue_schema_application(&db, job_id, None)
                .expect("continuation after abort should replay terminal state"),
            progress,
        );

        let aborted = store
            .with_schema(SchemaStore::current_accepted_schema_bundle)
            .expect("accepted bundle should remain readable")
            .expect("aborted accepted bundle should exist");
        assert!(
            aborted
                .entity_snapshots()
                .get(&entity_tag)
                .expect("entity should remain after abort")
                .constraint_catalog()
                .activation(constraint_id)
                .is_none(),
        );
        assert!(
            aborted
                .source_bindings_for_tests()
                .constraint(entity_tag, &check_source)
                .is_none(),
        );
        assert!(
            store
                .with_schema(|schema| {
                    schema.constraint_validation_job(entity_tag, constraint_id)
                })
                .expect("validation-job storage should remain readable")
                .is_none(),
        );

        ABORT_DATA.with(|store| {
            *store.borrow_mut() = DataStore::init_journaled(test_memory(180));
        });
        ABORT_INDEX.with(|store| {
            *store.borrow_mut() = IndexStore::init_journaled(test_memory(181));
        });
        ABORT_SCHEMA.with(|store| {
            *store.borrow_mut() = SchemaStore::init_journaled(test_memory(182));
        });
        ABORT_JOURNAL.with(|store| {
            *store.borrow_mut() = JournalTailStore::init(test_memory(183));
        });
        forget_recovered_domain_for_tests(&db).expect("upgrade should reset recovery ownership");
        ensure_recovered(&db).expect("recovery should retain the terminal abort");
        assert_eq!(
            abort_schema_application(&db, job_id, None)
                .expect("recovered terminal abort should replay"),
            progress,
        );
        assert!(
            store
                .with_schema(|schema| {
                    schema.constraint_validation_job(entity_tag, constraint_id)
                })
                .expect("recovered validation-job storage should remain readable")
                .is_none(),
        );
    }

    #[cfg(feature = "migration")]
    #[test]
    fn migration_planning_failures_retain_typed_public_classification() {
        use super::schema_migration_planning_error;
        use crate::db::schema::migration_planner::SchemaMigrationPlanningError;
        use icydb_diagnostic_code::{DiagnosticDetail, SchemaMigrationCode};

        for (error, reason) in [
            (
                SchemaMigrationPlanningError::Unadopted,
                SchemaMigrationCode::Unadopted,
            ),
            (
                SchemaMigrationPlanningError::MissingMigration,
                SchemaMigrationCode::MissingMigration,
            ),
            (
                SchemaMigrationPlanningError::VersionGap,
                SchemaMigrationCode::VersionGap,
            ),
            (
                SchemaMigrationPlanningError::Downgrade,
                SchemaMigrationCode::Downgrade,
            ),
            (
                SchemaMigrationPlanningError::EmptyEntityVersionBump,
                SchemaMigrationCode::EmptyEntityVersionBump,
            ),
            (
                SchemaMigrationPlanningError::StaleAcceptedHead,
                SchemaMigrationCode::StaleAcceptedHead,
            ),
            (
                SchemaMigrationPlanningError::UnknownFromObject,
                SchemaMigrationCode::UnknownFromObject,
            ),
            (
                SchemaMigrationPlanningError::UnknownToObject,
                SchemaMigrationCode::UnknownToObject,
            ),
            (
                SchemaMigrationPlanningError::KindMismatch,
                SchemaMigrationCode::KindMismatch,
            ),
            (
                SchemaMigrationPlanningError::IdentityConflict,
                SchemaMigrationCode::IdentityConflict,
            ),
            (
                SchemaMigrationPlanningError::UnexplainedSchemaDifference,
                SchemaMigrationCode::UnexplainedSchemaDifference,
            ),
            (
                SchemaMigrationPlanningError::UnsupportedTransform,
                SchemaMigrationCode::UnsupportedTransform,
            ),
            (
                SchemaMigrationPlanningError::RekeyedCatalogInvalid,
                SchemaMigrationCode::CandidateMismatch,
            ),
            (
                SchemaMigrationPlanningError::CandidateMismatch,
                SchemaMigrationCode::CandidateMismatch,
            ),
            (
                SchemaMigrationPlanningError::CorruptLineage,
                SchemaMigrationCode::ProgressCorrupt,
            ),
        ] {
            let diagnostic = schema_migration_planning_error(error).diagnostic();
            assert_eq!(
                diagnostic.detail(),
                Some(&DiagnosticDetail::SchemaMigration { reason }),
            );
            assert_eq!(diagnostic.code(), reason.diagnostic_code());
        }
    }

    #[cfg(feature = "migration")]
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the validation replay, staging, and unchanged-row assertions form one scenario"
    )]
    fn physical_migration_validation_is_bounded_staged_and_does_not_rewrite_rows() {
        use std::convert::Infallible;

        use super::{defer_generated_schema_application_for_prepared_migration, migrate_schema};
        use crate::db::{
            data::StoreVisit,
            index::{IndexEntryValue, IndexId, IndexKey, IndexKeyKind},
            key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
            schema::{SchemaMigrationCommand, SchemaMigrationPhase},
        };
        use crate::types::EntityTag;

        let db = Db::<MigrationCanister>::new(&MIGRATION_REGISTRY);
        ensure_recovered(&db).expect("migration database should initialize");
        let initial_target = schema_application_target(&db).expect("initial target should issue");
        let store_identity = initial_target
            .stores()
            .first()
            .expect("migration store should exist")
            .identity();
        let initial = validation_migration_proposal(
            ValidationMigrationShape::Clean,
            false,
            initial_target.accepted_head().clone(),
            initial_target.database_identity(),
            store_identity,
        );
        apply_schema(&db, &initial).expect("initial schema should publish");

        let session = DbSession::<MigrationCanister>::new(&MIGRATION_REGISTRY);
        for (id, value) in [(1, 7), (2, 8)] {
            session
                .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                    entity: "MigratingItem".to_string(),
                    patch: DynamicStructuralPatch::new(vec![
                        (
                            "id".to_string(),
                            DynamicWriteCell::Value(InputValue::Nat64(id)),
                        ),
                        (
                            "old_value".to_string(),
                            DynamicWriteCell::Value(InputValue::Int64(value)),
                        ),
                    ]),
                })
                .expect("predecessor row should insert");
        }
        let store = db
            .store_handle(MIGRATION_STORE_PATH)
            .expect("migration store should resolve");
        let row_bytes = || {
            store.with_data(|data| {
                let mut rows = Vec::new();
                let result: Result<(), Infallible> = data.visit_entries(|key, row| {
                    rows.push((key.as_bytes().to_vec(), row.as_bytes().to_vec()));
                    Ok(StoreVisit::Continue)
                });
                result.expect("infallible row visit should complete");
                rows
            })
        };
        let before_rows = row_bytes();

        let target = schema_application_target(&db).expect("migration target should issue");
        let proposal = validation_migration_proposal(
            ValidationMigrationShape::Clean,
            true,
            target.accepted_head().clone(),
            target.database_identity(),
            store_identity,
        );
        let plan = proposal
            .migration()
            .expect("migration plan should exist")
            .digest();
        let command = || SchemaMigrationCommand::Advance {
            expected_database: target.database_identity(),
            expected_head: target.accepted_head().clone(),
            expected_plan: plan,
            acknowledged_finding_page: None,
        };
        assert_eq!(
            migrate_schema(&db, &proposal, command())
                .unwrap_or_else(|error| {
                    panic!(
                        "physical migration should prepare: {:?}",
                        error.diagnostic()
                    )
                })
                .phase(),
            SchemaMigrationPhase::Prepared,
        );
        assert_eq!(
            migrate_schema(&db, &proposal, command())
                .expect("physical migration should enter validation")
                .phase(),
            SchemaMigrationPhase::Validating,
        );
        let record = super::load_schema_migration_record()
            .expect("migration record should remain readable")
            .expect("validating migration record should exist");
        let planned = super::recompile_active_physical_migration(&db, &proposal, &record)
            .expect("the exact active plan should recompile");
        for _ in 0..2 {
            let page = super::validate_migration_page(&db, &planned, record.progress())
                .expect("the same validation page should remain replayable");
            let (progress, staged, exhausted) = page.into_parts();
            assert!(progress.findings().is_empty());
            assert!(exhausted);
            super::stage_migration_index_entries(staged)
                .expect("staging before a cursor marker should be idempotent");
        }
        assert_eq!(
            store.with_index(IndexStore::len),
            2,
            "replaying an uncheckpointed page must retain one exact staged key per row",
        );
        let ready =
            migrate_schema(&db, &proposal, command()).expect("bounded validation should complete");
        assert_eq!(ready.phase(), SchemaMigrationPhase::ReadyToRewrite);
        assert_eq!(ready.rows_validated(), 2);
        assert!(ready.findings().is_empty());
        assert_eq!(row_bytes(), before_rows, "validation must not rewrite rows");
        assert_eq!(
            store.with_index(IndexStore::len),
            2,
            "the isolated candidate unique generation should be durably staged",
        );
        store.with_index_mut(|index| {
            for ordinal in 0..513_u64 {
                let component = ordinal.to_be_bytes();
                let key = IndexKey::new_from_components_with_primary_key_value(
                    &IndexId::new(EntityTag::new(2), 0),
                    IndexKeyKind::User,
                    &[component],
                    &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(ordinal)),
                )
                .expect("unrelated abort-scan key should build")
                .to_raw()
                .expect("unrelated abort-scan key should encode");
                index.insert(key, IndexEntryValue::presence());
            }
        });
        let abort = || SchemaMigrationCommand::Abort {
            expected_database: target.database_identity(),
            expected_head: target.accepted_head().clone(),
            expected_plan: plan,
        };
        let cleaning = migrate_schema(&db, &proposal, abort())
            .expect("the first bounded abort cleanup page should publish");
        assert_eq!(cleaning.phase(), SchemaMigrationPhase::ReadyToRewrite);
        assert_eq!(store.with_index(IndexStore::len), 513);
        let aborted =
            migrate_schema(&db, &proposal, abort()).expect("pre-rewrite migration should abort");
        assert_eq!(aborted.phase(), SchemaMigrationPhase::Aborted);
        assert_eq!(
            store.with_index(IndexStore::len),
            513,
            "abort must remove only planner-invisible candidate generations",
        );
        assert_eq!(
            row_bytes(),
            before_rows,
            "abort must retain predecessor rows"
        );
        assert!(
            !defer_generated_schema_application_for_prepared_migration(&db, &proposal)
                .expect("terminal aborted record must not block generated startup"),
        );
    }

    #[cfg(feature = "migration")]
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "the interrupted rewrite, recovery, final proof, and publication form one scenario"
    )]
    fn physical_migration_rewrite_recovers_and_publishes_one_complete_candidate() {
        use super::{
            defer_generated_schema_application_for_prepared_migration, migrate_schema,
            schema_migration_status_for_target,
        };
        use crate::db::{
            data::{CanonicalSlotReader, DecodedDataStoreKey, StoreVisit, StructuralSlotReader},
            schema::{
                MigrationRewriteInterruption, SchemaMigrationCommand, SchemaMigrationPhase,
                ensure_schema_migration_ready_for_ordinary_operations,
                interrupt_next_migration_rewrite_at,
            },
        };
        use crate::error::InternalError;

        let db = Db::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY);
        ensure_recovered(&db).expect("migration execution database should initialize");
        let initial_target = schema_application_target(&db).expect("initial target should issue");
        let store_identity = initial_target
            .stores()
            .first()
            .expect("migration execution store should exist")
            .identity();
        let initial = validation_migration_proposal(
            ValidationMigrationShape::Clean,
            false,
            initial_target.accepted_head().clone(),
            initial_target.database_identity(),
            store_identity,
        );
        apply_schema(&db, &initial).expect("initial schema should publish");
        let session = DbSession::<MigrationExecutionCanister>::new(&MIGRATION_EXECUTION_REGISTRY);
        for (id, value) in [(1, 7), (2, 8), (3, 9)] {
            session
                .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                    entity: "MigratingItem".to_string(),
                    patch: DynamicStructuralPatch::new(vec![
                        (
                            "id".to_string(),
                            DynamicWriteCell::Value(InputValue::Nat64(id)),
                        ),
                        (
                            "old_value".to_string(),
                            DynamicWriteCell::Value(InputValue::Int64(value)),
                        ),
                    ]),
                })
                .expect("predecessor row should insert");
        }
        let target = schema_application_target(&db).expect("migration target should issue");
        let proposal = validation_migration_proposal(
            ValidationMigrationShape::Clean,
            true,
            target.accepted_head().clone(),
            target.database_identity(),
            store_identity,
        );
        let plan = proposal
            .migration()
            .expect("migration plan should exist")
            .digest();
        let command = || SchemaMigrationCommand::Advance {
            expected_database: target.database_identity(),
            expected_head: target.accepted_head().clone(),
            expected_plan: plan,
            acknowledged_finding_page: None,
        };
        for expected in [
            SchemaMigrationPhase::Prepared,
            SchemaMigrationPhase::Validating,
            SchemaMigrationPhase::ReadyToRewrite,
            SchemaMigrationPhase::RewritingRows,
        ] {
            assert_eq!(
                migrate_schema(&db, &proposal, command())
                    .expect("migration phase should advance")
                    .phase(),
                expected,
            );
        }

        for interruption in [
            MigrationRewriteInterruption::MarkerPersisted,
            MigrationRewriteInterruption::JournalPublished,
            MigrationRewriteInterruption::PhysicalApplied,
        ] {
            interrupt_next_migration_rewrite_at(interruption);
            migrate_schema(&db, &proposal, command())
                .expect_err("injected interruption should retain the rewrite marker");

            forget_recovered_domain_for_tests(&db)
                .expect("upgrade should reset recovery ownership");
            ensure_recovered(&db).unwrap_or_else(|error| {
                panic!(
                    "recovery should finish the exact marker-bound rewrite page: {:?}",
                    error.diagnostic(),
                )
            });
        }

        let rebuilding = schema_migration_status_for_target(
            &db,
            &proposal,
            &schema_application_target(&db).expect("recovered target should issue"),
        )
        .expect("recovered status should remain readable");
        assert_eq!(rebuilding.phase(), SchemaMigrationPhase::RebuildingIndexes);
        assert_eq!(rebuilding.rows_rewritten(), 3);
        assert_eq!(
            migrate_schema(&db, &proposal, command())
                .expect("derived generations should complete")
                .phase(),
            SchemaMigrationPhase::FinalValidation,
        );
        assert_eq!(
            migrate_schema(&db, &proposal, command())
                .expect("final validation should complete")
                .phase(),
            SchemaMigrationPhase::Publishing,
        );
        let applied = migrate_schema(&db, &proposal, command())
            .expect("candidate publication should complete atomically");
        assert_eq!(applied.phase(), SchemaMigrationPhase::Applied);
        assert_eq!(applied.rows_rewritten(), 3);
        assert_eq!(applied.indexes_rebuilt(), 1);
        assert_ne!(applied.accepted_head(), target.accepted_head());
        let terminal_target = schema_application_target(&db).expect("terminal target should issue");
        let terminal_proposal = validation_migration_proposal(
            ValidationMigrationShape::Clean,
            true,
            terminal_target.accepted_head().clone(),
            terminal_target.database_identity(),
            store_identity,
        );
        assert!(
            !defer_generated_schema_application_for_prepared_migration(&db, &terminal_proposal,)
                .expect("terminal record must not block generated startup"),
        );

        let store = db
            .store_handle(MIGRATION_EXECUTION_STORE_PATH)
            .expect("migration execution store should resolve");
        let runtime = db
            .accepted_runtime_entity_for_path("MigratingItem")
            .expect("published candidate entity should resolve");
        let selection = store
            .with_schema(|schema| {
                schema.current_accepted_catalog_selection(
                    runtime.entity_tag(),
                    runtime.entity_path(),
                    runtime.store_path(),
                )
            })
            .expect("candidate selection should remain readable")
            .expect("candidate selection should exist");
        let contract = crate::db::data::AcceptedStructuralRowAuthority::from_catalog_selection(
            runtime.entity_path(),
            &selection,
        )
        .expect("candidate row authority should compile")
        .into_row_contract();
        let mut values = Vec::new();
        store
            .with_data(|data| {
                data.visit_entries(|key, row| {
                    let decoded = DecodedDataStoreKey::try_from_raw(key)
                        .expect("rewritten key should decode");
                    let reader =
                        StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                            row, &contract,
                        )
                        .expect("rewritten row should use the candidate layout");
                    reader
                        .validate_primary_key(&decoded)
                        .expect("rewritten row and key should remain bound");
                    values.push(
                        reader
                            .required_value_by_contract(1)
                            .expect("candidate value slot should decode"),
                    );
                    Ok::<StoreVisit, InternalError>(StoreVisit::Continue)
                })
            })
            .expect("rewritten row scan should complete");
        assert_eq!(
            values,
            vec![
                crate::value::Value::Nat64(7),
                crate::value::Value::Nat64(8),
                crate::value::Value::Nat64(9),
            ],
        );
        assert_eq!(store.with_index(IndexStore::len), 3);
        let accepted = store
            .with_schema(SchemaStore::current_accepted_schema_bundle)
            .expect("published candidate bundle should remain readable")
            .expect("published candidate bundle should exist");
        let entity_source = EntitySourceKey::try_new("MigratingItem")
            .expect("migration entity source should admit");
        let entity_tag = accepted
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("candidate entity source should remain bound");
        let old_value =
            FieldSourceKey::try_new("old_value").expect("predecessor source should admit");
        let current_value =
            FieldSourceKey::try_new("value").expect("candidate source should admit");
        assert_eq!(
            accepted
                .source_bindings_for_tests()
                .field(entity_tag, &old_value),
            None,
        );
        assert!(
            accepted
                .source_bindings_for_tests()
                .field(entity_tag, &current_value)
                .is_some(),
        );
        ensure_schema_migration_ready_for_ordinary_operations()
            .expect("terminal publication must clear the database-wide gate");
    }

    #[cfg(feature = "migration")]
    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "all four finding families share one ordered historical scan fixture"
    )]
    fn physical_migration_validation_reports_every_typed_finding_family_without_writes() {
        use std::convert::Infallible;

        use super::migrate_schema;
        use crate::db::{
            data::StoreVisit,
            schema::{SchemaMigrationCommand, SchemaMigrationFindingKind, SchemaMigrationPhase},
        };

        let db = Db::<MigrationFindingCanister>::new(&MIGRATION_FINDING_REGISTRY);
        ensure_recovered(&db).expect("migration finding database should initialize");
        let initial_target = schema_application_target(&db).expect("initial target should issue");
        let store_identity = initial_target
            .stores()
            .first()
            .expect("migration finding store should exist")
            .identity();
        let initial = validation_migration_proposal(
            ValidationMigrationShape::AllFindingFamilies,
            false,
            initial_target.accepted_head().clone(),
            initial_target.database_identity(),
            store_identity,
        );
        apply_schema(&db, &initial).expect("initial finding schema should publish");

        let session = DbSession::<MigrationFindingCanister>::new(&MIGRATION_FINDING_REGISTRY);
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "MigrationTarget".to_string(),
                patch: DynamicStructuralPatch::new(vec![(
                    "id".to_string(),
                    DynamicWriteCell::Value(InputValue::Nat64(7)),
                )]),
            })
            .expect("relation target should insert");
        for (id, value) in [(1, 9), (2, 8), (3, 7), (4, 7), (5, 300)] {
            session
                .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                    entity: "MigratingItem".to_string(),
                    patch: DynamicStructuralPatch::new(vec![
                        (
                            "id".to_string(),
                            DynamicWriteCell::Value(InputValue::Nat64(id)),
                        ),
                        (
                            "old_value".to_string(),
                            DynamicWriteCell::Value(InputValue::Int64(value)),
                        ),
                    ]),
                })
                .expect("predecessor finding row should insert");
        }
        let store = db
            .store_handle(MIGRATION_FINDING_STORE_PATH)
            .expect("migration finding store should resolve");
        let row_bytes = || {
            store.with_data(|data| {
                let mut rows = Vec::new();
                let result: Result<(), Infallible> = data.visit_entries(|key, row| {
                    rows.push((key.as_bytes().to_vec(), row.as_bytes().to_vec()));
                    Ok(StoreVisit::Continue)
                });
                result.expect("infallible row visit should complete");
                rows
            })
        };
        let before_rows = row_bytes();

        let target = schema_application_target(&db).expect("migration target should issue");
        let proposal = validation_migration_proposal(
            ValidationMigrationShape::AllFindingFamilies,
            true,
            target.accepted_head().clone(),
            target.database_identity(),
            store_identity,
        );
        let plan = proposal
            .migration()
            .expect("migration plan should exist")
            .digest();
        let command = || SchemaMigrationCommand::Advance {
            expected_database: target.database_identity(),
            expected_head: target.accepted_head().clone(),
            expected_plan: plan,
            acknowledged_finding_page: None,
        };
        assert_eq!(
            migrate_schema(&db, &proposal, command())
                .expect("finding migration should prepare")
                .phase(),
            SchemaMigrationPhase::Prepared,
        );
        assert_eq!(
            migrate_schema(&db, &proposal, command())
                .expect("finding migration should enter validation")
                .phase(),
            SchemaMigrationPhase::Validating,
        );
        let rejected =
            migrate_schema(&db, &proposal, command()).expect("validation should report findings");
        assert_eq!(rejected.phase(), SchemaMigrationPhase::Rejected);
        assert_eq!(rejected.rows_validated(), 5);
        assert_eq!(
            rejected
                .findings()
                .iter()
                .map(crate::db::schema::SchemaMigrationFinding::kind)
                .collect::<Vec<_>>(),
            vec![
                SchemaMigrationFindingKind::Constraint,
                SchemaMigrationFindingKind::Relation,
                SchemaMigrationFindingKind::UniqueIndex,
                SchemaMigrationFindingKind::Transform,
            ],
        );
        assert_eq!(
            row_bytes(),
            before_rows,
            "rejected validation must not rewrite accepted rows"
        );
        assert_eq!(
            store.with_index(IndexStore::len),
            0,
            "a rejected page must not publish any staged generation"
        );
    }

    #[cfg(feature = "migration")]
    #[test]
    fn exact_migration_retry_binds_the_terminal_head_not_the_predecessor_head() {
        use super::exact_migration_replay_target;

        let db = Db::<EvolutionCanister>::new(&EVOLUTION_REGISTRY);
        ensure_recovered(&db).expect("test database should initialize");
        let initial_target = schema_application_target(&db).expect("initial target should issue");
        let (proposal, _, _) = generated_check_proposal(
            initial_target.accepted_head().clone(),
            "migration-retry-initial",
            false,
            initial_target.database_identity(),
            initial_target
                .stores()
                .first()
                .expect("test store should exist")
                .identity(),
        );
        apply_schema(&db, &proposal).expect("initial schema should publish");
        let current_target = schema_application_target(&db).expect("current target should issue");
        assert_ne!(
            current_target.accepted_head(),
            initial_target.accepted_head(),
        );

        let receipt = SchemaChangeReceipt::new(
            current_target.database_identity(),
            SchemaSubmissionKey::try_new("migration/retry")
                .expect("migration submission should admit"),
            SchemaProposalDigest::from_bytes([0x77; 32]),
            initial_target.accepted_head().clone(),
            SchemaChangeOutcome::Applied {
                accepted_head: current_target.accepted_head().clone(),
            },
        )
        .expect("terminal migration receipt should admit");
        let record = SchemaApplicationRecord::new(receipt, Vec::new())
            .expect("terminal migration record should admit");

        assert_eq!(
            exact_migration_replay_target(&db, current_target.database_identity(), &record,)
                .expect("exact retry should resolve the terminal target"),
            current_target,
        );
    }
}
