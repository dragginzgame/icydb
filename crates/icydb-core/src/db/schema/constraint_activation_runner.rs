//! Module: db::schema::constraint_activation_runner
//! Responsibility: bounded historical proof for accepted constraint activations.
//! Does not own: mutation write gates, SQL syntax, or kind-specific physical staging.
//! Boundary: accepted activation + canonical rows -> marker-owned job progress/promotion.

use crate::db::schema::enum_catalog::AcceptedSchemaRevisionBundle;
#[cfg(feature = "sql")]
use crate::{
    db::schema::accepted_constraint_field_paths,
    error::{ConstraintDiagnostic, ConstraintDiagnosticKind, SchemaTransitionBudgetResource},
};
use crate::{
    db::{
        Db,
        commit::{
            publish_accepted_schema_candidate,
            publish_accepted_schema_candidate_with_constraint_validation_job,
            publish_accepted_schema_candidate_with_constraint_validation_job_removal,
            publish_constraint_validation_job,
            publish_constraint_validation_job_with_candidate_index_entries,
            publish_constraint_validation_job_with_candidate_relation_entries,
        },
        data::{
            AcceptedStructuralRowAuthority, DecodedDataStoreKey, RawDataStoreKey, StoreVisit,
            StructuralSlotReader,
        },
        direction::Direction,
        index::{IndexKey, RawIndexStoreKey},
        key_taxonomy::RawDataStoreKeyRange,
        registry::{StoreHandle, StoreRecoveryCapability},
        relation::{
            RelationConstraintIndexEntry, RelationConstraintProjection, ReverseRelationSourceInfo,
        },
        schema::{
            AcceptedConstraintCatalog, AcceptedRowConstraintEvaluationError,
            AcceptedSchemaSnapshot, CandidateSchemaRevision, CompiledAcceptedRowConstraints,
            ConstraintActivationKind, ConstraintActivationState, ConstraintId,
            ConstraintStoreRevision, ConstraintValidationFinding, ConstraintValidationJob,
            ConstraintValidationPhase, ConstraintValidationReceipt, PersistedFieldSnapshot,
            PersistedIndexSnapshot, UniqueConstraintProjection,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};
use std::ops::Bound;

const MAX_VALIDATION_ROWS_PER_PAGE: usize = 256;
const MAX_VALIDATION_DECODED_BYTES_PER_PAGE: usize = 4 * 1024 * 1024;
const MAX_VALIDATION_FINDINGS_PER_PAGE: usize = 64;
const MAX_VALIDATION_STAGED_BYTES_PER_PAGE: usize = 4 * 1024 * 1024;

/// Result of one bounded activation lifecycle step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ConstraintValidationProgress {
    /// A durable Forward job was created for a journaled store.
    Started,
    /// One bounded page advanced without a retained finding receipt.
    Advanced {
        phase: ConstraintValidationPhase,
        rows_scanned: u64,
    },
    /// The same durable finding page remains until its sequence is acknowledged.
    Findings {
        receipt: ConstraintValidationReceipt,
        phase: ConstraintValidationPhase,
        rows_scanned: u64,
    },
    /// Verify authority changed and the durable job restarted at Forward.
    Restarted { rows_scanned: u64 },
    /// Exact proof promoted the activation to one accepted row-local constraint.
    Promoted { rows_scanned: u64 },
}

/// Advance one generated or SQL-owned check activation by at most one bounded page.
pub(in crate::db) fn advance_check_constraint_activation<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    acknowledged_receipt: Option<u64>,
) -> Result<ConstraintValidationProgress, InternalError> {
    advance_row_local_constraint_activation(
        db,
        entity_tag,
        constraint_id,
        acknowledged_receipt,
        RowLocalActivationKind::Check,
    )
}

/// Advance one not-null activation by at most one bounded page.
pub(in crate::db) fn advance_not_null_constraint_activation<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    acknowledged_receipt: Option<u64>,
) -> Result<ConstraintValidationProgress, InternalError> {
    advance_row_local_constraint_activation(
        db,
        entity_tag,
        constraint_id,
        acknowledged_receipt,
        RowLocalActivationKind::NotNull,
    )
}

/// Advance one unique-index activation by at most one bounded page.
pub(in crate::db) fn advance_unique_constraint_activation<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    acknowledged_receipt: Option<u64>,
) -> Result<ConstraintValidationProgress, InternalError> {
    let hooks = db.runtime_hook_for_entity_tag(entity_tag)?;
    let store = db.store_handle(hooks.store_path)?;
    if store.storage_capabilities().recovery()
        != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_unsupported());
    }
    let selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                entity_tag,
                hooks.entity_path,
                hooks.store_path,
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let accepted = selection.decode_verified()?;
    let activation = accepted
        .persisted_snapshot()
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_invariant)?;
    let candidate = unique_candidate_for_activation(&accepted, constraint_id)?;

    match activation.state() {
        ConstraintActivationState::EnforcingNewWrites => start_journaled_staged_validation(
            store,
            hooks.store_path,
            entity_tag,
            hooks.entity_path,
            constraint_id,
            candidate.physical_generation(),
        ),
        ConstraintActivationState::Validating => resume_journaled_unique_validation(
            store,
            hooks.store_path,
            entity_tag,
            hooks.entity_path,
            constraint_id,
            acknowledged_receipt,
            &accepted,
            &selection,
            candidate,
        ),
    }
}

/// Advance one relation activation by at most one bounded page.
pub(in crate::db) fn advance_relation_constraint_activation<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    acknowledged_receipt: Option<u64>,
) -> Result<ConstraintValidationProgress, InternalError> {
    let hooks = db.runtime_hook_for_entity_tag(entity_tag)?;
    let store = db.store_handle(hooks.store_path)?;
    if store.storage_capabilities().recovery()
        != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_unsupported());
    }
    let selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                entity_tag,
                hooks.entity_path,
                hooks.store_path,
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let accepted = selection.decode_verified()?;
    let activation = accepted
        .persisted_snapshot()
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_invariant)?;
    let candidate = relation_candidate_for_activation(&accepted, constraint_id)?;
    let contract =
        AcceptedStructuralRowAuthority::from_catalog_selection(hooks.entity_path, &selection)?
            .into_row_contract();
    let projection = RelationConstraintProjection::new(
        db,
        ReverseRelationSourceInfo::new(hooks.entity_path, entity_tag),
        accepted.persisted_snapshot(),
        &contract,
        candidate,
    )?;
    if projection.target_store().storage_capabilities().recovery()
        != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_unsupported());
    }

    match activation.state() {
        ConstraintActivationState::EnforcingNewWrites => start_journaled_staged_validation(
            store,
            hooks.store_path,
            entity_tag,
            hooks.entity_path,
            constraint_id,
            candidate.physical_generation(),
        ),
        ConstraintActivationState::Validating => resume_journaled_relation_validation(
            store,
            hooks.store_path,
            entity_tag,
            hooks.entity_path,
            constraint_id,
            acknowledged_receipt,
            &selection,
            candidate,
            projection,
        ),
    }
}

/// Prove one unpublished SQL check candidate in a single bounded scan.
///
/// This is the atomic plain-`ADD` boundary: it never publishes the temporary
/// activation used to compile the candidate semantics.
#[cfg(feature = "sql")]
pub(in crate::db) fn validate_unpublished_check_candidate_exact(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    entity_path: &'static str,
    candidate: &CandidateSchemaRevision,
    constraint_id: ConstraintId,
) -> Result<usize, InternalError> {
    let selection = crate::db::schema::AcceptedCatalogSnapshotSelection::from_candidate(
        candidate,
        entity_tag,
        entity_path,
        store_path,
    )?
    .ok_or_else(InternalError::store_corruption)?;
    let accepted = selection.decode_verified()?;
    let constraints = compile_row_local_activation(
        &accepted,
        &selection,
        constraint_id,
        RowLocalActivationKind::Check,
    )?;
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, &selection)?
        .into_row_contract();
    let scan = scan_row_local_validation_page(
        store,
        entity_tag,
        None,
        &contract,
        &constraints,
        selection.identity().accepted_schema_fingerprint(),
        constraint_id,
        activation_dependency_fields(&accepted, constraint_id)?,
    )?;
    if !scan.findings.is_empty() {
        let activation = accepted
            .persisted_snapshot()
            .constraint_catalog()
            .activation(constraint_id)
            .ok_or_else(InternalError::store_corruption)?;
        let finding = scan
            .findings
            .first()
            .ok_or_else(InternalError::store_invariant)?;
        let primary_key = finding
            .primary_key()
            .encoded_primary_key_bytes()
            .ok_or_else(InternalError::store_invariant)?;
        return Err(InternalError::mutation_constraint_violation(
            ConstraintDiagnostic::migration_validation(
                constraint_id.get(),
                activation.name().to_string(),
                ConstraintDiagnosticKind::Check,
                entity_path.to_string(),
                primary_key.to_vec(),
                accepted_constraint_field_paths(
                    accepted.persisted_snapshot(),
                    finding.field_ids(),
                )?,
                finding.error_code(),
            ),
        ));
    }
    if !scan.exhausted {
        return Err(InternalError::schema_transition_budget_exceeded(
            SchemaTransitionBudgetResource::SourceRows,
        ));
    }
    Ok(scan.rows_scanned)
}

/// Row-local evaluator selected by a typed activation entrypoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RowLocalActivationKind {
    /// Evaluate one accepted check expression.
    Check,
    /// Evaluate one accepted field's not-null contract.
    NotNull,
}

fn advance_row_local_constraint_activation<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    acknowledged_receipt: Option<u64>,
    required_kind: RowLocalActivationKind,
) -> Result<ConstraintValidationProgress, InternalError> {
    let hooks = db.runtime_hook_for_entity_tag(entity_tag)?;
    let store = db.store_handle(hooks.store_path)?;
    let selection = store
        .with_schema(|schema_store| {
            schema_store.current_accepted_catalog_selection(
                entity_tag,
                hooks.entity_path,
                hooks.store_path,
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    let accepted = selection.decode_verified()?;
    let activation = accepted
        .persisted_snapshot()
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_invariant)?;
    if !required_kind.matches(activation.kind()) {
        return Err(InternalError::store_unsupported());
    }

    match activation.state() {
        ConstraintActivationState::EnforcingNewWrites => {
            match store.storage_capabilities().recovery() {
                StoreRecoveryCapability::None => validate_exact_heap_row_local_activation(
                    store,
                    constraint_id,
                    &selection,
                    &accepted,
                    required_kind,
                ),
                StoreRecoveryCapability::StableBasePlusJournalReplay => {
                    start_journaled_row_local_validation(
                        store,
                        hooks.store_path,
                        entity_tag,
                        hooks.entity_path,
                        constraint_id,
                    )
                }
            }
        }
        ConstraintActivationState::Validating => resume_journaled_row_local_validation(
            store,
            hooks.store_path,
            entity_tag,
            hooks.entity_path,
            constraint_id,
            acknowledged_receipt,
            &selection,
            &accepted,
            required_kind,
        ),
    }
}

impl RowLocalActivationKind {
    const fn matches(self, kind: &ConstraintActivationKind) -> bool {
        matches!(
            (self, kind),
            (Self::Check, ConstraintActivationKind::Check { .. })
                | (Self::NotNull, ConstraintActivationKind::NotNull { .. })
        )
    }
}

fn start_journaled_row_local_validation(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    entity_path: &'static str,
    constraint_id: ConstraintId,
) -> Result<ConstraintValidationProgress, InternalError> {
    let current = current_bundle(store, store_path)?;
    let snapshot = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    if snapshot.entity_path() != entity_path {
        return Err(InternalError::store_corruption());
    }
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_validation_started(constraint_id)
        .map_err(|_| InternalError::store_invariant())?;
    let candidate = candidate_with_catalog(&current, entity_tag, catalog)?;
    let candidate_snapshot = candidate
        .bundle()
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let activation = candidate_snapshot
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_corruption)?;
    let job =
        ConstraintValidationJob::start(entity_tag, entity_path.to_string(), activation, None)?;
    publish_accepted_schema_candidate_with_constraint_validation_job(
        store_path,
        store,
        current.revision(),
        &candidate,
        &job,
    )?;
    Ok(ConstraintValidationProgress::Started)
}

fn start_journaled_staged_validation(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    entity_path: &'static str,
    constraint_id: ConstraintId,
    staged_generation: u64,
) -> Result<ConstraintValidationProgress, InternalError> {
    let current = current_bundle(store, store_path)?;
    let snapshot = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    if snapshot.entity_path() != entity_path {
        return Err(InternalError::store_corruption());
    }
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_validation_started(constraint_id)
        .map_err(|_| InternalError::store_invariant())?;
    let candidate = candidate_with_catalog(&current, entity_tag, catalog)?;
    let candidate_snapshot = candidate
        .bundle()
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let activation = candidate_snapshot
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_corruption)?;
    if activation.activation_epoch() != staged_generation {
        return Err(InternalError::store_corruption());
    }
    let job = ConstraintValidationJob::start(
        entity_tag,
        entity_path.to_string(),
        activation,
        Some(staged_generation),
    )?;
    publish_accepted_schema_candidate_with_constraint_validation_job(
        store_path,
        store,
        current.revision(),
        &candidate,
        &job,
    )?;
    Ok(ConstraintValidationProgress::Started)
}

#[expect(
    clippy::too_many_arguments,
    reason = "unique activation keeps accepted identity, candidate owner, and job inputs explicit"
)]
fn resume_journaled_unique_validation(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    entity_path: &'static str,
    constraint_id: ConstraintId,
    acknowledged_receipt: Option<u64>,
    accepted: &AcceptedSchemaSnapshot,
    selection: &crate::db::schema::AcceptedCatalogSnapshotSelection,
    candidate: &PersistedIndexSnapshot,
) -> Result<ConstraintValidationProgress, InternalError> {
    let mut job = store
        .with_schema(|schema_store| {
            schema_store.constraint_validation_job(entity_tag, constraint_id)
        })?
        .ok_or_else(InternalError::store_corruption)?;
    if !job.acknowledge_receipt(acknowledged_receipt) {
        return job
            .last_receipt()
            .cloned()
            .map(|receipt| ConstraintValidationProgress::Findings {
                receipt,
                phase: job.phase(),
                rows_scanned: job.rows_scanned(),
            })
            .ok_or_else(InternalError::store_corruption);
    }
    if job.staged_generation() != Some(candidate.physical_generation()) {
        return Err(InternalError::store_corruption());
    }
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, selection)?
        .into_row_contract();
    let projection = UniqueConstraintProjection::new(entity_tag, candidate, &contract)?;
    let dependency_fields = unique_index_key_fields(accepted.persisted_snapshot(), candidate)?;

    match job.phase() {
        ConstraintValidationPhase::Forward => {
            let scan = scan_unique_validation_page(
                store,
                entity_tag,
                job.checkpoint(),
                &contract,
                &projection,
                dependency_fields.as_slice(),
                UniqueValidationMode::Forward,
            )?;
            let captured_revision = scan
                .exhausted
                .then(|| current_store_revision(store, store_path))
                .transpose()?
                .map(|revision| vec![revision]);
            job.record_forward_page(
                scan.checkpoint,
                scan.rows_scanned,
                scan.findings,
                scan.exhausted,
                captured_revision,
            )?;
            publish_constraint_validation_job_with_candidate_index_entries(
                store_path,
                store,
                &job,
                scan.staged_entries,
            )?;
            Ok(progress_for_job(job))
        }
        ConstraintValidationPhase::Verify => {
            let captured = required_captured_revision(&job, store_path)?;
            if current_store_revision(store, store_path)?.revision() != captured {
                job.restart_forward(0, Vec::new())?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(restarted_progress(&job));
            }
            let scan = scan_unique_validation_page(
                store,
                entity_tag,
                job.checkpoint(),
                &contract,
                &projection,
                dependency_fields.as_slice(),
                UniqueValidationMode::Verify,
            )?;
            if !scan.findings.is_empty() {
                job.restart_forward(scan.rows_scanned, scan.findings)?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(progress_for_job(job));
            }
            job.record_verify_page(scan.checkpoint, scan.rows_scanned)?;
            if !scan.exhausted {
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(progress_for_job(job));
            }
            if current_store_revision(store, store_path)?.revision() != captured {
                job.restart_forward(0, Vec::new())?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(restarted_progress(&job));
            }
            let rows_scanned = job.rows_scanned();
            promote_unique_activation(store, store_path, entity_tag, constraint_id)?;
            Ok(ConstraintValidationProgress::Promoted { rows_scanned })
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "relation activation keeps source, target, candidate, and job authority explicit"
)]
fn resume_journaled_relation_validation(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    entity_path: &'static str,
    constraint_id: ConstraintId,
    acknowledged_receipt: Option<u64>,
    selection: &crate::db::schema::AcceptedCatalogSnapshotSelection,
    candidate: &crate::db::schema::PersistedRelationEdgeSnapshot,
    projection: RelationConstraintProjection,
) -> Result<ConstraintValidationProgress, InternalError> {
    let mut job = store
        .with_schema(|schema_store| {
            schema_store.constraint_validation_job(entity_tag, constraint_id)
        })?
        .ok_or_else(InternalError::store_corruption)?;
    if !job.acknowledge_receipt(acknowledged_receipt) {
        return job
            .last_receipt()
            .cloned()
            .map(|receipt| ConstraintValidationProgress::Findings {
                receipt,
                phase: job.phase(),
                rows_scanned: job.rows_scanned(),
            })
            .ok_or_else(InternalError::store_corruption);
    }
    if job.staged_generation() != Some(candidate.physical_generation()) {
        return Err(InternalError::store_corruption());
    }
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, selection)?
        .into_row_contract();

    match job.phase() {
        ConstraintValidationPhase::Forward => {
            let scan = scan_relation_validation_page(
                store,
                entity_tag,
                job.checkpoint(),
                &contract,
                &projection,
                candidate.local_field_ids(),
                RelationValidationMode::Forward,
            )?;
            let captured_revisions = scan
                .exhausted
                .then(|| current_relation_store_revisions(store, store_path, &projection))
                .transpose()?;
            job.record_forward_page(
                scan.checkpoint,
                scan.rows_scanned,
                scan.findings,
                scan.exhausted,
                captured_revisions,
            )?;
            publish_constraint_validation_job_with_candidate_relation_entries(
                store_path,
                store,
                &job,
                &projection,
                scan.staged_entries,
            )?;
            Ok(progress_for_job(job))
        }
        ConstraintValidationPhase::Verify => {
            let captured = required_captured_revisions(&job)?;
            if current_relation_store_revisions(store, store_path, &projection)? != captured {
                job.restart_forward(0, Vec::new())?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(restarted_progress(&job));
            }
            let scan = scan_relation_validation_page(
                store,
                entity_tag,
                job.checkpoint(),
                &contract,
                &projection,
                candidate.local_field_ids(),
                RelationValidationMode::Verify,
            )?;
            if !scan.findings.is_empty() {
                job.restart_forward(scan.rows_scanned, scan.findings)?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(progress_for_job(job));
            }
            job.record_verify_page(scan.checkpoint, scan.rows_scanned)?;
            if !scan.exhausted {
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(progress_for_job(job));
            }
            if current_relation_store_revisions(store, store_path, &projection)? != captured {
                job.restart_forward(0, Vec::new())?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(restarted_progress(&job));
            }
            let rows_scanned = job.rows_scanned();
            promote_relation_activation(store, store_path, entity_tag, constraint_id)?;
            Ok(ConstraintValidationProgress::Promoted { rows_scanned })
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the runner keeps accepted identity, storage, and compiled proof inputs explicit"
)]
fn resume_journaled_row_local_validation(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    entity_path: &'static str,
    constraint_id: ConstraintId,
    acknowledged_receipt: Option<u64>,
    selection: &crate::db::schema::AcceptedCatalogSnapshotSelection,
    accepted: &AcceptedSchemaSnapshot,
    required_kind: RowLocalActivationKind,
) -> Result<ConstraintValidationProgress, InternalError> {
    if store.storage_capabilities().recovery()
        != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_corruption());
    }
    let mut job = store
        .with_schema(|schema_store| {
            schema_store.constraint_validation_job(entity_tag, constraint_id)
        })?
        .ok_or_else(InternalError::store_corruption)?;
    if !job.acknowledge_receipt(acknowledged_receipt) {
        return job
            .last_receipt()
            .cloned()
            .map(|receipt| ConstraintValidationProgress::Findings {
                receipt,
                phase: job.phase(),
                rows_scanned: job.rows_scanned(),
            })
            .ok_or_else(InternalError::store_corruption);
    }
    let constraints =
        compile_row_local_activation(accepted, selection, constraint_id, required_kind)?;
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(entity_path, selection)?
        .into_row_contract();

    match job.phase() {
        ConstraintValidationPhase::Forward => {
            let scan = scan_row_local_validation_page(
                store,
                entity_tag,
                job.checkpoint(),
                &contract,
                &constraints,
                selection.identity().accepted_schema_fingerprint(),
                constraint_id,
                activation_dependency_fields(accepted, constraint_id)?,
            )?;
            let captured_revision = scan
                .exhausted
                .then(|| current_store_revision(store, store_path))
                .transpose()?
                .map(|revision| vec![revision]);
            job.record_forward_page(
                scan.checkpoint,
                scan.rows_scanned,
                scan.findings,
                scan.exhausted,
                captured_revision,
            )?;
            publish_constraint_validation_job(store_path, store, &job)?;
            Ok(progress_for_job(job))
        }
        ConstraintValidationPhase::Verify => {
            let captured = required_captured_revision(&job, store_path)?;
            if current_store_revision(store, store_path)?.revision() != captured {
                job.restart_forward(0, Vec::new())?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(restarted_progress(&job));
            }
            let scan = scan_row_local_validation_page(
                store,
                entity_tag,
                job.checkpoint(),
                &contract,
                &constraints,
                selection.identity().accepted_schema_fingerprint(),
                constraint_id,
                activation_dependency_fields(accepted, constraint_id)?,
            )?;
            if !scan.findings.is_empty() {
                job.restart_forward(scan.rows_scanned, scan.findings)?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(progress_for_job(job));
            }
            job.record_verify_page(scan.checkpoint, scan.rows_scanned)?;
            if !scan.exhausted {
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(progress_for_job(job));
            }
            if current_store_revision(store, store_path)?.revision() != captured {
                job.restart_forward(0, Vec::new())?;
                publish_constraint_validation_job(store_path, store, &job)?;
                return Ok(restarted_progress(&job));
            }
            let rows_scanned = job.rows_scanned();
            promote_row_local_activation(store, store_path, entity_tag, constraint_id)?;
            Ok(ConstraintValidationProgress::Promoted { rows_scanned })
        }
    }
}

fn validate_exact_heap_row_local_activation(
    store: StoreHandle,
    constraint_id: ConstraintId,
    selection: &crate::db::schema::AcceptedCatalogSnapshotSelection,
    accepted: &AcceptedSchemaSnapshot,
    required_kind: RowLocalActivationKind,
) -> Result<ConstraintValidationProgress, InternalError> {
    let identity = selection.identity();
    let constraints =
        compile_row_local_activation(accepted, selection, constraint_id, required_kind)?;
    let contract =
        AcceptedStructuralRowAuthority::from_catalog_selection(identity.entity_path(), selection)?
            .into_row_contract();
    let scan = scan_row_local_validation_page(
        store,
        identity.entity_tag(),
        None,
        &contract,
        &constraints,
        selection.identity().accepted_schema_fingerprint(),
        constraint_id,
        activation_dependency_fields(accepted, constraint_id)?,
    )?;
    if !scan.exhausted {
        return Err(InternalError::store_unsupported());
    }
    if !scan.findings.is_empty() {
        return Ok(ConstraintValidationProgress::Findings {
            receipt: ConstraintValidationReceipt::new(1, scan.findings),
            phase: ConstraintValidationPhase::Forward,
            rows_scanned: u64::try_from(scan.rows_scanned).unwrap_or(u64::MAX),
        });
    }
    let current = current_bundle(store, identity.store_path())?;
    let candidate = candidate_with_promoted_row_local_activation(
        &current,
        identity.entity_tag(),
        constraint_id,
    )?;
    publish_accepted_schema_candidate(
        identity.store_path(),
        store,
        current.revision(),
        &candidate,
    )?;
    Ok(ConstraintValidationProgress::Promoted {
        rows_scanned: u64::try_from(scan.rows_scanned).unwrap_or(u64::MAX),
    })
}

fn promote_row_local_activation(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
) -> Result<(), InternalError> {
    let current = current_bundle(store, store_path)?;
    let candidate =
        candidate_with_promoted_row_local_activation(&current, entity_tag, constraint_id)?;
    publish_accepted_schema_candidate_with_constraint_validation_job_removal(
        store_path,
        store,
        current.revision(),
        &candidate,
        entity_tag,
        constraint_id,
    )
}

fn promote_unique_activation(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
) -> Result<(), InternalError> {
    let current = current_bundle(store, store_path)?;
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let version = before
        .version()
        .get()
        .checked_add(1)
        .map(crate::db::schema::SchemaVersion::new)
        .ok_or_else(InternalError::store_unsupported)?;
    let after = before
        .with_promoted_unique_activation(constraint_id, version)
        .map_err(|_| InternalError::store_invariant())?;
    let candidate = candidate_with_snapshot(&current, entity_tag, after)?;
    publish_accepted_schema_candidate_with_constraint_validation_job_removal(
        store_path,
        store,
        current.revision(),
        &candidate,
        entity_tag,
        constraint_id,
    )
}

fn promote_relation_activation(
    store: StoreHandle,
    store_path: &'static str,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
) -> Result<(), InternalError> {
    let current = current_bundle(store, store_path)?;
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let version = before
        .version()
        .get()
        .checked_add(1)
        .map(crate::db::schema::SchemaVersion::new)
        .ok_or_else(InternalError::store_unsupported)?;
    let after = before
        .with_promoted_relation_activation(constraint_id, version)
        .map_err(|_| InternalError::store_invariant())?;
    let candidate = candidate_with_snapshot(&current, entity_tag, after)?;
    publish_accepted_schema_candidate_with_constraint_validation_job_removal(
        store_path,
        store,
        current.revision(),
        &candidate,
        entity_tag,
        constraint_id,
    )
}

fn current_bundle(
    store: StoreHandle,
    store_path: &'static str,
) -> Result<AcceptedSchemaRevisionBundle, InternalError> {
    let bundle = store
        .with_schema(crate::db::schema::SchemaStore::current_accepted_schema_bundle)?
        .ok_or_else(InternalError::store_corruption)?;
    if bundle.store_path() != store_path {
        return Err(InternalError::store_corruption());
    }
    Ok(bundle)
}

fn compile_row_local_activation(
    accepted: &AcceptedSchemaSnapshot,
    selection: &crate::db::schema::AcceptedCatalogSnapshotSelection,
    constraint_id: ConstraintId,
    required_kind: RowLocalActivationKind,
) -> Result<CompiledAcceptedRowConstraints, InternalError> {
    let value_catalog = selection.value_catalog_handle();
    let fingerprint = selection.identity().accepted_schema_fingerprint();
    match required_kind {
        RowLocalActivationKind::Check => CompiledAcceptedRowConstraints::compile_check_activation(
            accepted,
            value_catalog,
            fingerprint,
            constraint_id,
        ),
        RowLocalActivationKind::NotNull => {
            CompiledAcceptedRowConstraints::compile_not_null_activation(
                accepted,
                value_catalog,
                fingerprint,
                constraint_id,
            )
        }
    }
    .map_err(map_row_constraint_program_error)
}

fn candidate_with_promoted_row_local_activation(
    current: &AcceptedSchemaRevisionBundle,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
) -> Result<CandidateSchemaRevision, InternalError> {
    let before = current
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    let activation = before
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_corruption)?;
    let after = match activation.kind() {
        ConstraintActivationKind::Check { .. } => {
            let catalog = match activation.state() {
                ConstraintActivationState::EnforcingNewWrites => before
                    .constraint_catalog()
                    .clone()
                    .with_directly_validated_activation(constraint_id),
                ConstraintActivationState::Validating => before
                    .constraint_catalog()
                    .clone()
                    .with_promoted_activation(constraint_id),
            }
            .map_err(|_| InternalError::store_invariant())?;
            before.clone().with_constraint_catalog(catalog)
        }
        ConstraintActivationKind::NotNull { .. } => {
            let version = before
                .version()
                .get()
                .checked_add(1)
                .map(crate::db::schema::SchemaVersion::new)
                .ok_or_else(InternalError::store_unsupported)?;
            before
                .with_promoted_not_null_activation(constraint_id, version)
                .map_err(|_| InternalError::store_invariant())?
        }
        ConstraintActivationKind::Unique { .. } | ConstraintActivationKind::Relation { .. } => {
            return Err(InternalError::store_unsupported());
        }
    };
    candidate_with_snapshot(current, entity_tag, after)
}

fn candidate_with_catalog(
    current: &AcceptedSchemaRevisionBundle,
    entity_tag: EntityTag,
    catalog: AcceptedConstraintCatalog,
) -> Result<CandidateSchemaRevision, InternalError> {
    let snapshot = current
        .entity_snapshots()
        .get(&entity_tag)
        .cloned()
        .ok_or_else(InternalError::store_corruption)?
        .with_constraint_catalog(catalog);
    candidate_with_snapshot(current, entity_tag, snapshot)
}

fn candidate_with_snapshot(
    current: &AcceptedSchemaRevisionBundle,
    entity_tag: EntityTag,
    snapshot: crate::db::schema::PersistedSchemaSnapshot,
) -> Result<CandidateSchemaRevision, InternalError> {
    let mut entity_snapshots = current.entity_snapshots().clone();
    entity_snapshots.insert(entity_tag, snapshot);
    let revision = current
        .revision()
        .checked_next()
        .ok_or_else(InternalError::store_unsupported)?;
    let bundle = AcceptedSchemaRevisionBundle::new(
        revision,
        current.store_path(),
        current.enum_catalog().clone(),
        current.composite_catalog().clone(),
        entity_snapshots,
    )?;
    CandidateSchemaRevision::new(bundle)
}

struct ValidationPageScan {
    checkpoint: Option<RawDataStoreKey>,
    rows_scanned: usize,
    findings: Vec<ConstraintValidationFinding>,
    exhausted: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "one scan keeps exact row, schema, program, and finding identities explicit"
)]
fn scan_row_local_validation_page(
    store: StoreHandle,
    entity_tag: EntityTag,
    checkpoint: Option<&RawDataStoreKey>,
    contract: &crate::db::data::StructuralRowContract,
    constraints: &CompiledAcceptedRowConstraints,
    schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
    constraint_id: ConstraintId,
    dependency_fields: Vec<crate::db::schema::FieldId>,
) -> Result<ValidationPageScan, InternalError> {
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
    let mut rows_scanned = 0usize;
    let mut decoded_bytes = 0usize;
    let mut findings = Vec::new();
    let mut has_more = false;

    store.with_data(|data| {
        data.visit_range((lower, upper), |raw_key, raw_row| {
            let row_bytes = raw_row.len();
            if rows_scanned == MAX_VALIDATION_ROWS_PER_PAGE
                || findings.len() == MAX_VALIDATION_FINDINGS_PER_PAGE
                || (rows_scanned != 0
                    && decoded_bytes.saturating_add(row_bytes)
                        > MAX_VALIDATION_DECODED_BYTES_PER_PAGE)
            {
                has_more = true;
                return Ok(StoreVisit::Stop);
            }
            let decoded_key = DecodedDataStoreKey::try_from_raw(raw_key)
                .map_err(|_| InternalError::identity_corruption())?;
            if decoded_key.entity_tag() != entity_tag {
                return Err(InternalError::identity_corruption());
            }
            let row = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                raw_row, contract,
            )?;
            row.validate_primary_key(&decoded_key)?;
            let values = row.decode_selected_slot_values(constraints.required_slots())?;
            match constraints.evaluate(schema_fingerprint, values.as_slice()) {
                Ok(()) => {}
                Err(AcceptedRowConstraintEvaluationError::Violation {
                    constraint_id: violated,
                    constraint_name: _,
                    kind: _,
                    field_paths: _,
                }) if violated == constraint_id => {
                    findings.push(ConstraintValidationFinding::new(
                        raw_key.clone(),
                        dependency_fields.clone(),
                        icydb_diagnostic_code::ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION
                            .raw(),
                    ));
                }
                Err(error) => return Err(map_row_constraint_program_error(error)),
            }
            decoded_bytes = decoded_bytes.saturating_add(row_bytes);
            rows_scanned = rows_scanned.saturating_add(1);
            final_checkpoint = Some(raw_key.clone());
            Ok(StoreVisit::Continue)
        })
    })?;

    Ok(ValidationPageScan {
        checkpoint: final_checkpoint,
        rows_scanned,
        findings,
        exhausted: !has_more,
    })
}

fn activation_dependency_fields(
    accepted: &AcceptedSchemaSnapshot,
    constraint_id: ConstraintId,
) -> Result<Vec<crate::db::schema::FieldId>, InternalError> {
    let activation = accepted
        .persisted_snapshot()
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_corruption)?;
    let mut fields = match activation.kind() {
        ConstraintActivationKind::Check { expression } => expression.dependencies(),
        ConstraintActivationKind::NotNull { field_id } => vec![*field_id],
        ConstraintActivationKind::Unique { .. } | ConstraintActivationKind::Relation { .. } => {
            return Err(InternalError::store_corruption());
        }
    };
    fields.sort_unstable();
    fields.dedup();
    Ok(fields)
}

fn current_store_revision(
    store: StoreHandle,
    store_path: &'static str,
) -> Result<ConstraintStoreRevision, InternalError> {
    let journal = store
        .journal_tail_store()
        .ok_or_else(InternalError::store_unsupported)?;
    let revision =
        journal.with_borrow(crate::db::journal::JournalTailStore::data_mutation_revision)?;
    Ok(ConstraintStoreRevision::new(
        store_path.to_string(),
        revision,
    ))
}

fn required_captured_revision(
    job: &ConstraintValidationJob,
    store_path: &'static str,
) -> Result<u64, InternalError> {
    let revisions = job
        .captured_store_revisions()
        .ok_or_else(InternalError::store_corruption)?;
    let [revision] = revisions else {
        return Err(InternalError::store_corruption());
    };
    if revision.store_path() != store_path {
        return Err(InternalError::store_corruption());
    }
    Ok(revision.revision())
}

fn current_relation_store_revisions(
    source_store: StoreHandle,
    source_store_path: &'static str,
    projection: &RelationConstraintProjection,
) -> Result<Vec<ConstraintStoreRevision>, InternalError> {
    let mut revisions = vec![current_store_revision(source_store, source_store_path)?];
    if projection.target_store_path() != source_store_path {
        revisions.push(current_store_revision(
            projection.target_store(),
            projection.target_store_path(),
        )?);
    }
    revisions.sort_unstable_by(|left, right| left.store_path().cmp(right.store_path()));
    Ok(revisions)
}

fn required_captured_revisions(
    job: &ConstraintValidationJob,
) -> Result<Vec<ConstraintStoreRevision>, InternalError> {
    job.captured_store_revisions()
        .filter(|revisions| !revisions.is_empty())
        .map(<[ConstraintStoreRevision]>::to_vec)
        .ok_or_else(InternalError::store_corruption)
}

fn progress_for_job(job: ConstraintValidationJob) -> ConstraintValidationProgress {
    if let Some(receipt) = job.last_receipt().cloned() {
        return ConstraintValidationProgress::Findings {
            receipt,
            phase: job.phase(),
            rows_scanned: job.rows_scanned(),
        };
    }
    ConstraintValidationProgress::Advanced {
        phase: job.phase(),
        rows_scanned: job.rows_scanned(),
    }
}

const fn restarted_progress(job: &ConstraintValidationJob) -> ConstraintValidationProgress {
    ConstraintValidationProgress::Restarted {
        rows_scanned: job.rows_scanned(),
    }
}

fn map_row_constraint_program_error(_error: AcceptedRowConstraintEvaluationError) -> InternalError {
    InternalError::accepted_row_constraint_program_corrupt()
}

fn unique_candidate_for_activation(
    accepted: &AcceptedSchemaSnapshot,
    constraint_id: ConstraintId,
) -> Result<&PersistedIndexSnapshot, InternalError> {
    let snapshot = accepted.persisted_snapshot();
    let activation = snapshot
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_corruption)?;
    let ConstraintActivationKind::Unique { index_id } = activation.kind() else {
        return Err(InternalError::store_unsupported());
    };
    let mut matching = snapshot
        .candidate_indexes()
        .iter()
        .filter(|index| index.schema_id() == *index_id);
    let candidate = matching
        .next()
        .ok_or_else(InternalError::store_corruption)?;
    if matching.next().is_some()
        || candidate.physical_generation() != activation.activation_epoch()
        || !candidate.unique()
    {
        return Err(InternalError::store_corruption());
    }
    Ok(candidate)
}

fn relation_candidate_for_activation(
    accepted: &AcceptedSchemaSnapshot,
    constraint_id: ConstraintId,
) -> Result<&crate::db::schema::PersistedRelationEdgeSnapshot, InternalError> {
    let snapshot = accepted.persisted_snapshot();
    let activation = snapshot
        .constraint_catalog()
        .activation(constraint_id)
        .ok_or_else(InternalError::store_corruption)?;
    let ConstraintActivationKind::Relation { relation_id } = activation.kind() else {
        return Err(InternalError::store_unsupported());
    };
    let mut matching = snapshot
        .candidate_relations()
        .iter()
        .filter(|relation| relation.id() == *relation_id);
    let candidate = matching
        .next()
        .ok_or_else(InternalError::store_corruption)?;
    if matching.next().is_some() || candidate.physical_generation() != activation.activation_epoch()
    {
        return Err(InternalError::store_corruption());
    }
    Ok(candidate)
}

/// Whether one relation page builds isolated reverse state or proves it read-only.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RelationValidationMode {
    Forward,
    Verify,
}

/// Bounded result of scanning one relation-activation page.

struct RelationValidationPageScan {
    checkpoint: Option<RawDataStoreKey>,
    rows_scanned: usize,
    findings: Vec<ConstraintValidationFinding>,
    staged_entries: Vec<RelationConstraintIndexEntry>,
    exhausted: bool,
}

fn scan_relation_validation_page(
    store: StoreHandle,
    entity_tag: EntityTag,
    checkpoint: Option<&RawDataStoreKey>,
    contract: &crate::db::data::StructuralRowContract,
    projection: &RelationConstraintProjection,
    dependency_fields: &[crate::db::schema::FieldId],
    mode: RelationValidationMode,
) -> Result<RelationValidationPageScan, InternalError> {
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
    let mut rows_scanned = 0usize;
    let mut decoded_bytes = 0usize;
    let mut staged_bytes = 0usize;
    let mut findings = Vec::new();
    let mut staged_entries = Vec::new();
    let mut has_more = false;

    store.with_data(|data| {
        data.visit_range((lower, upper), |raw_key, raw_row| {
            let row_bytes = raw_row.len();
            if rows_scanned == MAX_VALIDATION_ROWS_PER_PAGE
                || findings.len() == MAX_VALIDATION_FINDINGS_PER_PAGE
                || (rows_scanned != 0
                    && decoded_bytes.saturating_add(row_bytes)
                        > MAX_VALIDATION_DECODED_BYTES_PER_PAGE)
            {
                has_more = true;
                return Ok(StoreVisit::Stop);
            }
            let decoded_key = DecodedDataStoreKey::try_from_raw(raw_key)
                .map_err(|_| InternalError::identity_corruption())?;
            if decoded_key.entity_tag() != entity_tag {
                return Err(InternalError::identity_corruption());
            }
            let row = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                raw_row, contract,
            )?;
            row.validate_primary_key(&decoded_key)?;
            let candidate = projection.project_row(&decoded_key.primary_key_value(), &row, true)?;
            let next_staged_bytes = candidate
                .entries()
                .iter()
                .fold(staged_bytes, |bytes, entry| {
                    bytes.saturating_add(entry.key().as_bytes().len())
                });
            if next_staged_bytes > MAX_VALIDATION_STAGED_BYTES_PER_PAGE {
                if rows_scanned == 0 {
                    return Err(InternalError::store_unsupported());
                }
                has_more = true;
                return Ok(StoreVisit::Stop);
            }
            let missing_entry = mode == RelationValidationMode::Verify
                && candidate.entries().iter().any(|entry| {
                    entry
                        .target_store()
                        .with_index(|index_store| index_store.get(entry.key()).is_none())
                });
            if !candidate.missing_targets().is_empty() || missing_entry {
                let error = match candidate.missing_targets().first() {
                    Some(target) => projection.missing_target_error(target)?,
                    None => InternalError::index_violation(contract.entity_path(), &[]),
                };
                findings.push(ConstraintValidationFinding::new(
                    raw_key.clone(),
                    dependency_fields.to_vec(),
                    error.diagnostic().error_code().raw(),
                ));
            }
            staged_bytes = next_staged_bytes;
            if mode == RelationValidationMode::Forward {
                staged_entries.extend(candidate.into_entries());
            }
            decoded_bytes = decoded_bytes.saturating_add(row_bytes);
            rows_scanned = rows_scanned.saturating_add(1);
            final_checkpoint = Some(raw_key.clone());
            Ok(StoreVisit::Continue)
        })
    })?;
    staged_entries.sort_unstable_by(|left, right| {
        (left.target_store_path(), left.key()).cmp(&(right.target_store_path(), right.key()))
    });

    Ok(RelationValidationPageScan {
        checkpoint: final_checkpoint,
        rows_scanned,
        findings,
        staged_entries,
        exhausted: !has_more,
    })
}

/// Whether one unique page builds isolated state or proves it read-only.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UniqueValidationMode {
    Forward,
    Verify,
}

/// Bounded result of scanning one unique-activation page.

struct UniqueValidationPageScan {
    checkpoint: Option<RawDataStoreKey>,
    rows_scanned: usize,
    findings: Vec<ConstraintValidationFinding>,
    staged_entries: Vec<RawIndexStoreKey>,
    exhausted: bool,
}

fn scan_unique_validation_page(
    store: StoreHandle,
    entity_tag: EntityTag,
    checkpoint: Option<&RawDataStoreKey>,
    contract: &crate::db::data::StructuralRowContract,
    projection: &UniqueConstraintProjection,
    dependency_fields: &[crate::db::schema::FieldId],
    mode: UniqueValidationMode,
) -> Result<UniqueValidationPageScan, InternalError> {
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
    let mut rows_scanned = 0usize;
    let mut decoded_bytes = 0usize;
    let mut staged_bytes = 0usize;
    let mut findings = Vec::new();
    let mut staged_entries = Vec::new();
    let mut page_keys = Vec::new();
    let mut has_more = false;

    store.with_data(|data| {
        data.visit_range((lower, upper), |raw_key, raw_row| {
            let row_bytes = raw_row.len();
            if rows_scanned == MAX_VALIDATION_ROWS_PER_PAGE
                || findings.len() == MAX_VALIDATION_FINDINGS_PER_PAGE
                || (rows_scanned != 0
                    && decoded_bytes.saturating_add(row_bytes)
                        > MAX_VALIDATION_DECODED_BYTES_PER_PAGE)
            {
                has_more = true;
                return Ok(StoreVisit::Stop);
            }
            let decoded_key = DecodedDataStoreKey::try_from_raw(raw_key)
                .map_err(|_| InternalError::identity_corruption())?;
            if decoded_key.entity_tag() != entity_tag {
                return Err(InternalError::identity_corruption());
            }
            let row = StructuralSlotReader::from_raw_row_with_validated_borrowed_contract(
                raw_row, contract,
            )?;
            row.validate_primary_key(&decoded_key)?;
            let candidate_key = projection.derive_key(&decoded_key.primary_key_value(), &row)?;
            if let Some(candidate_key) = candidate_key {
                let next_staged_bytes = staged_bytes.saturating_add(candidate_key.as_bytes().len());
                if rows_scanned != 0 && next_staged_bytes > MAX_VALIDATION_STAGED_BYTES_PER_PAGE {
                    has_more = true;
                    return Ok(StoreVisit::Stop);
                }
                let conflict =
                    candidate_unique_key_conflicts(store, &candidate_key, page_keys.as_slice())?;
                let missing = mode == UniqueValidationMode::Verify
                    && store.with_index(|index_store| index_store.get(&candidate_key).is_none());
                if conflict || missing {
                    let error = InternalError::index_violation(contract.entity_path(), &[]);
                    findings.push(ConstraintValidationFinding::new(
                        raw_key.clone(),
                        dependency_fields.to_vec(),
                        error.diagnostic().error_code().raw(),
                    ));
                } else {
                    staged_bytes = next_staged_bytes;
                    page_keys.push(candidate_key.clone());
                    if mode == UniqueValidationMode::Forward {
                        staged_entries.push(candidate_key);
                    }
                }
            }
            decoded_bytes = decoded_bytes.saturating_add(row_bytes);
            rows_scanned = rows_scanned.saturating_add(1);
            final_checkpoint = Some(raw_key.clone());
            Ok(StoreVisit::Continue)
        })
    })?;
    staged_entries.sort_unstable();

    Ok(UniqueValidationPageScan {
        checkpoint: final_checkpoint,
        rows_scanned,
        findings,
        staged_entries,
        exhausted: !has_more,
    })
}

fn unique_index_key_fields(
    snapshot: &crate::db::schema::PersistedSchemaSnapshot,
    candidate: &PersistedIndexSnapshot,
) -> Result<Vec<crate::db::schema::FieldId>, InternalError> {
    // Durable findings retain the exact accepted key fields that define the
    // duplicate domain; public projection must not reconstruct them later.
    let fields = snapshot
        .fields()
        .iter()
        .filter(|field| candidate.key().references_field(field.id()))
        .map(PersistedFieldSnapshot::id)
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Err(InternalError::store_corruption());
    }

    Ok(fields)
}

fn candidate_unique_key_conflicts(
    store: StoreHandle,
    candidate_raw: &RawIndexStoreKey,
    page_keys: &[RawIndexStoreKey],
) -> Result<bool, InternalError> {
    let candidate =
        IndexKey::try_from_raw(candidate_raw).map_err(|_| InternalError::index_invariant())?;
    for page_raw in page_keys {
        let page =
            IndexKey::try_from_raw(page_raw).map_err(|_| InternalError::index_invariant())?;
        if page.index_id() == candidate.index_id() && page.has_same_components(&candidate) {
            return Ok(true);
        }
    }
    let (lower, upper) = candidate
        .raw_bounds_for_all_components()
        .map_err(|_| InternalError::index_invariant())?;
    let mut conflict = false;
    store.with_index(|index_store| {
        index_store.visit_raw_entries_in_range(
            (&Bound::Included(lower), &Bound::Included(upper)),
            Direction::Asc,
            |raw, _| {
                if raw != candidate_raw {
                    conflict = true;
                }
                Ok(conflict)
            },
        )
    })?;
    Ok(conflict)
}
