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
            AcceptedSchemaPublication, database_incarnation_id, ensure_recovered,
            publish_accepted_schema_candidates_with_application_record,
            publish_generated_check_abort_with_application_record,
        },
        data::DataStore,
        index::{IndexState, IndexStore},
        registry::{
            StoreAllocationIdentity, StoreAllocationIdentityCapability, StoreCommitParticipation,
            StoreDurability, StoreHandle, StoreRecoveryCapability, StoreRelationSourceCapability,
            StoreRelationTargetCapability, StoreRuntimeStorageMode, StoreSchemaMetadataCapability,
        },
        relation::prove_empty_reverse_relation_domain,
        schema::{
            AcceptedSchemaRevision, AcceptedSchemaRevisionBundle, CandidateSchemaRevision,
            ConstraintActivationKind, ConstraintActivationState, ConstraintId, ConstraintOrigin,
            ConstraintValidationPhase, ConstraintValidationProgress, ExistingProposalStore,
            ProposalStoreTarget, SchemaApplicationRecord, SchemaApplicationRecordOp,
            SchemaChangeActivation, SchemaChangeJob, SchemaChangeJobId, SchemaChangeOutcome,
            SchemaChangeProgress, SchemaChangeProgressStatus, SchemaChangeReceipt,
            SchemaChangeValidationPhase, StagedUserIndexDomainError, UnpublishedCheckValidation,
            accepted_constraint_field_paths, advance_accepted_check_constraint_activation,
            derive_schema_change_job_id, lower_existing_schema_proposal,
            lower_initial_schema_proposal, prove_empty_user_index_domain,
            validate_unpublished_check_candidate_bounded, with_schema_application_store,
        },
    },
    error::{ConstraintDiagnostic, ConstraintDiagnosticKind, InternalError},
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

const DATABASE_TARGET_FINGERPRINT_PROFILE: &[u8] = b"icydb.schema-target.database.v1";
const STORE_TARGET_FINGERPRINT_PROFILE: &[u8] = b"icydb.schema-target.store.v1";
const ACCEPTED_DATABASE_HEAD_FINGERPRINT_PROFILE: &[u8] = b"icydb.accepted-schema.database-head.v1";

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

/// Catalog authority resolved for one pending generated-check abort.
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

/// One new generated-check activation awaiting direct bounded proof.
#[derive(Clone)]
struct DirectGeneratedCheckProof {
    candidate_index: usize,
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: crate::types::EntityTag,
    entity_path: String,
    constraint_id: ConstraintId,
    historical_rows: u64,
}

/// One generated check whose direct proof requires durable continuation.
#[derive(Clone)]
struct PendingGeneratedCheck {
    proof: DirectGeneratedCheckProof,
}

/// Catalog-native application staging retained until marker publication.
struct LoweredApplication {
    current_bundles: Vec<Option<AcceptedSchemaRevisionBundle>>,
    candidates: Vec<CandidateSchemaRevision>,
    pending: Option<PendingGeneratedCheck>,
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
    stores.sort_by(|left, right| left.path.cmp(right.path));

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

    if snapshot
        .constraint_catalog()
        .constraints()
        .iter()
        .any(|constraint| {
            constraint.id() == constraint_id
                && constraint.origin() == ConstraintOrigin::Generated
                && matches!(
                    constraint.kind(),
                    crate::db::schema::AcceptedConstraintKind::Check { .. }
                )
        })
    {
        return finalize_schema_application(
            db,
            &record,
            candidate_head,
            SchemaChangeProgressStatus::Applied,
        );
    }

    let pending = snapshot
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_corruption)?;
    if pending.origin() != ConstraintOrigin::Generated
        || !matches!(pending.kind(), ConstraintActivationKind::Check { .. })
    {
        return Err(InternalError::store_corruption());
    }
    let entity_path = snapshot.entity_path().to_string();
    let progress = advance_accepted_check_constraint_activation(
        authority.handle,
        authority.path,
        entity_tag,
        entity_path.as_str(),
        constraint_id,
        acknowledged_receipt,
    )?;
    let status = schema_change_progress_status(snapshot, constraint_id, pending.name(), progress)?;
    if status == SchemaChangeProgressStatus::Applied {
        finalize_schema_application(db, &record, candidate_head, status)
    } else {
        Ok(SchemaChangeProgress::new(record.receipt().clone(), status))
    }
}

/// Abort one pending generated-check application.
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
    let candidate =
        aborted_generated_check_candidate(&abort.current, abort.entity_tag, abort.constraint_id)?;
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
        publish_generated_check_abort_with_application_record(
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
                && matches!(pending.kind(), ConstraintActivationKind::Check { .. })
        })
        .ok_or_else(InternalError::store_corruption)?;
    let remove_validation_job = pending_generated_check_job_retirement(
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

fn pending_generated_check_job_retirement(
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

fn aborted_generated_check_candidate(
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
                && matches!(activation.kind(), ConstraintActivationKind::Check { .. })
        })
        .ok_or_else(InternalError::store_corruption)?;
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_aborted_activation(constraint_id)
        .map_err(|_| InternalError::store_invariant())?;
    let mut snapshots = current.entity_snapshots().clone();
    snapshots.insert(entity_tag, snapshot.with_constraint_catalog(catalog));
    let mut source_bindings = current.source_bindings().clone();
    source_bindings.remove_constraint_identity(entity_tag, constraint_id)?;
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

    let authorities = application_authorities(db);
    let LoweredApplication {
        current_bundles,
        candidates,
        pending,
    } = lower_application_candidates(&target, proposal, authorities.as_slice())?;
    let accepted_head = if let Some(pending) = pending.as_ref() {
        let final_candidates = final_candidates_for_pending_check(&candidates, pending)?;
        accepted_head_after_candidates(authorities.as_slice(), final_candidates.as_slice())?
    } else if candidates.is_empty() {
        target.accepted_head().clone()
    } else {
        accepted_head_after_candidates(authorities.as_slice(), candidates.as_slice())?
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
            candidate_head: accepted_head,
        }
    } else if candidates.is_empty() {
        SchemaChangeOutcome::NoOp { accepted_head }
    } else {
        SchemaChangeOutcome::Applied { accepted_head }
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
    let publications =
        application_publications(authorities.as_slice(), &current_bundles, &candidates)?;
    publish_accepted_schema_candidates_with_application_record(publications, operation)?;
    Ok(receipt)
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
            lower_initial_schema_proposal(proposal, stores.as_slice())?
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

/// Complete generated-check additions only after a bounded exact proof.
///
/// Empty domains use maintained exact cardinality. At most one non-empty
/// activation may consume the canonical 0.211 exact scan budget. A journaled
/// proof that exceeds that page becomes one durable pending application;
/// volatile or additional non-empty proofs reject before publication.
fn preflight_existing_application(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<crate::db::schema::AcceptedSchemaRevisionBundle>],
    candidates: &mut [CandidateSchemaRevision],
) -> Result<Option<PendingGeneratedCheck>, InternalError> {
    require_empty_record_member_renames(authorities, current_bundles, candidates)?;
    require_empty_physical_entity_removal(authorities, current_bundles, candidates)?;
    require_empty_physical_field_removals(authorities, current_bundles, candidates)?;
    require_empty_physical_index_removals(authorities, current_bundles, candidates)?;
    require_empty_physical_relation_removals(authorities, current_bundles, candidates)?;
    let proofs = generated_check_proofs(authorities, current_bundles, candidates)?;
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
                match validate_unpublished_check_candidate_bounded(
                    proof.store,
                    proof.store_path,
                    proof.entity_tag,
                    proof.entity_path.as_str(),
                    &candidate,
                    proof.constraint_id,
                )? {
                    UnpublishedCheckValidation::Complete { .. } => {}
                    UnpublishedCheckValidation::Incomplete => {
                        if proof.store.storage_capabilities().recovery()
                            != StoreRecoveryCapability::StableBasePlusJournalReplay
                            || pending.is_some()
                        {
                            return Err(InternalError::store_unsupported());
                        }
                        pending = Some(PendingGeneratedCheck {
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

/// Prove that canonical record-member names change only over exactly empty
/// logical and derived domains.
///
/// Member names are encoded inside canonical composite values and therefore
/// inside any row, index key, or reverse-relation key that projects them.
/// This preflight admits the metadata transition without inventing a row
/// migration path only when none of those physical representations exists.
fn require_empty_record_member_renames(
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
            let member_names_changed = before.fields().iter().any(|before_field| {
                after
                    .fields()
                    .iter()
                    .find(|after_field| after_field.id() == before_field.id())
                    .is_some_and(|after_field| {
                        after_field.nested_leaves() != before_field.nested_leaves()
                    })
            });
            if !member_names_changed {
                continue;
            }
            require_exact_empty_entity(authority.handle, *entity_tag)?;
            authority
                .handle
                .with_index(|store| prove_empty_user_index_domain(store, *entity_tag))
                .map_err(StagedUserIndexDomainError::into_internal_error)?;
            for relation in before.relations() {
                let target_store = accepted_entity_store_for_path(
                    authorities,
                    current_bundles,
                    relation.target_path(),
                )?;
                target_store.with_index(|store| {
                    prove_empty_reverse_relation_domain(store, *entity_tag, before, relation)
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

fn generated_check_proofs(
    authorities: &[StoreApplicationAuthority],
    current_bundles: &[Option<AcceptedSchemaRevisionBundle>],
    candidates: &[CandidateSchemaRevision],
) -> Result<Vec<DirectGeneratedCheckProof>, InternalError> {
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
            for constraint_id in added_generated_check_activations(before, after) {
                let historical_rows = authority
                    .handle
                    .with_data(|store| store.exact_entity_count(*entity_tag))
                    .ok_or_else(InternalError::store_corruption)?;
                proofs.push(DirectGeneratedCheckProof {
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

fn added_generated_check_activations(
    before: &crate::db::schema::PersistedSchemaSnapshot,
    after: &crate::db::schema::PersistedSchemaSnapshot,
) -> Vec<ConstraintId> {
    after
        .constraint_activations()
        .iter()
        .filter(|candidate| {
            candidate.origin() == ConstraintOrigin::Generated
                && matches!(candidate.kind(), ConstraintActivationKind::Check { .. })
                && !before
                    .constraints()
                    .iter()
                    .any(|accepted| accepted.id() == candidate.id())
                && !before
                    .constraint_activations()
                    .iter()
                    .any(|accepted| accepted.id() == candidate.id())
        })
        .map(crate::db::schema::ConstraintActivationSnapshot::id)
        .collect()
}

fn final_candidates_for_pending_check(
    candidates: &[CandidateSchemaRevision],
    pending: &PendingGeneratedCheck,
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
    constraint_name: &str,
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
            let findings = receipt
                .findings()
                .iter()
                .map(|finding| {
                    let primary_key = finding
                        .primary_key()
                        .encoded_primary_key_bytes()
                        .ok_or_else(InternalError::store_invariant)?;
                    Ok(ConstraintDiagnostic::migration_validation(
                        constraint_id.get(),
                        constraint_name.to_string(),
                        ConstraintDiagnosticKind::Check,
                        snapshot.entity_path().to_string(),
                        primary_key.to_vec(),
                        accepted_constraint_field_paths(snapshot, finding.field_ids())?,
                        finding.error_code(),
                    ))
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
    authorities.sort_by(|left, right| left.path.cmp(right.path));
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
        AcceptedSchemaPublication, AcceptedStoreHead, abort_schema_application,
        aborted_generated_check_candidate, apply_schema, continue_schema_application,
        derive_accepted_head, derive_schema_change_job_id, ensure_recovered,
        lower_existing_schema_proposal, lower_initial_schema_proposal,
        publish_accepted_schema_candidates_with_application_record,
        require_exact_empty_entity_count, schema_application_target,
    };
    use crate::{
        db::{
            Db, EntityRegistration,
            commit::forget_recovered_domain_for_tests,
            data::DataStore,
            index::IndexStore,
            journal::JournalTailStore,
            registry::{
                StoreAllocationIdentities, StoreAllocationIdentity, StoreRegistry,
                StoreRuntimeStorageCapabilities,
            },
            schema::{
                ConstraintOrigin, ExistingProposalStore, ProposalStoreTarget,
                SchemaApplicationRecord, SchemaApplicationRecordOp, SchemaChangeActivation,
                SchemaChangeJob, SchemaChangeOutcome, SchemaChangeProgressStatus, SchemaStore,
            },
        },
        error::ErrorClass,
        testing::test_memory,
        traits::{CanisterKind, Path},
    };
    use icydb_schema::{
        ConstraintFragment, ConstraintSourceKey, EntityFragment, EntitySourceKey,
        EntityStoreAssignment, ExpectedAcceptedHead, ExpectedSchemaFingerprint, FieldFragment,
        FieldInsertPolicy, FieldSourceKey, FieldType, ScalarLiteral, ScalarType, SchemaCapability,
        SchemaFragment, SchemaName, SchemaProposal, SchemaSubmissionKey, SourceCheckExpr,
        SourceCheckInstruction, TargetDatabaseIdentity, TargetStoreIdentity,
    };
    use std::cell::RefCell;

    const ABORT_STORE_PATH: &str = "schema_application_tests::AbortStore";

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
        let entity_source =
            EntitySourceKey::try_new("abort:entity:item").expect("entity source should admit");
        let id_source = FieldSourceKey::try_new("abort:field:id").expect("id source should admit");
        let score_source =
            FieldSourceKey::try_new("abort:field:score").expect("score source should admit");
        let check_source =
            ConstraintSourceKey::try_new("abort:check:score").expect("check source should admit");
        let check = SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(score_source.clone()),
            SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
            SourceCheckInstruction::GreaterThanOrEqual,
        ])
        .expect("check expression should admit");
        let constraints = include_check
            .then(|| {
                ConstraintFragment::new(check_source.clone(), name("score_non_negative"), check)
            })
            .into_iter()
            .collect();
        let entity = EntityFragment::try_new(
            entity_source.clone(),
            name("Item"),
            vec![
                FieldFragment::new(
                    id_source.clone(),
                    name("id"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                FieldFragment::new(
                    score_source,
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
        )
        .expect("schema proposal should compose");
        (proposal, entity_source, check_source)
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

        let aborted = aborted_generated_check_candidate(
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
    #[allow(
        clippy::too_many_lines,
        reason = "the journaled abort, replay, and recovery assertions form one scenario"
    )]
    fn pending_generated_check_abort_is_atomic_terminal_and_replayable() {
        let db = Db::<AbortCanister>::new_with_registrations(
            &ABORT_REGISTRY,
            &[] as &[EntityRegistration<AbortCanister>],
        );
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
}
