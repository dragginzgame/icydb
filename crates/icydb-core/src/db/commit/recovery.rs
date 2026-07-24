//! Module: db::commit::recovery
//! Responsibility: publish marker-bound journal batches and rebuild durable state before operations.
//! Does not own: marker storage encoding, mutation planning, or query semantics.
//! Boundary: db entrypoints -> commit::recovery -> commit::{rebuild,store} + journal fold (one-way).
//!
//! This module implements a **system recovery step** that restores global
//! database invariants by completing marker-owned work forward and rebuilding
//! derived state before any new operation proceeds.
//!
//! Important semantic notes:
//! - Recovery runs once at startup.
//! - Read and write paths both perform a cheap marker check and replay if needed.
//! - Reads must not proceed while a persisted partial commit marker is present.
//!
//! Invocation from read or mutation entrypoints is permitted only as an
//! unconditional invariant-restoration step. Recovery must not be
//! interleaved with read logic or mutation planning/apply phases.

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        commit::{
            CommitMarker, CommitRowOp, CommitSchemaFingerprint,
            memory::{
                CommitMemoryAllocation, configure_commit_memory_id,
                current_commit_memory_allocation,
            },
            rebuild::rebuild_secondary_indexes_from_rows,
            store::{
                commit_marker_may_be_present, commit_marker_present_fast,
                mark_commit_marker_verified_absent, with_commit_store,
            },
        },
        data::{
            AcceptedStructuralRowAuthority, DataStore, DecodedDataStoreKey, RawDataStoreKey,
            RawRow, StructuralSlotReader,
        },
        database_format::ensure_database_format_admitted,
        index::IndexStore,
        journal::{FoldWatermark, JournalBatch, JournalRecord, JournalSequence, JournalTailStore},
        registry::{StoreHandle, StoreRecoveryCapability},
        schema::{
            AcceptedCatalogSnapshotSelection, CandidateSchemaRevision, ConstraintId, SchemaStore,
            accepted_commit_schema_fingerprint, decode_constraint_validation_job,
            decode_persisted_schema_snapshot, ensure_accepted_schema_snapshot,
            reconcile_runtime_schemas, reconcile_runtime_schemas_before_recovery_rebuild,
        },
    },
    error::{ErrorOrigin, InternalError},
    traits::CanisterKind,
    types::EntityTag,
};
#[cfg(not(test))]
use std::sync::{Mutex, OnceLock};
use std::{cell::RefCell, collections::BTreeSet};

#[cfg(test)]
use crate::db::commit::failpoint::{CommitFailpoint, hit_commit_failpoint};
#[cfg(test)]
use crate::db::database_format::clear_database_format_admission_for_tests;

#[cfg(not(test))]
static RECOVERED_KEYS: OnceLock<Mutex<Vec<RecoveryDomainKey>>> = OnceLock::new();
#[cfg(not(test))]
static RECOVERY_IN_PROGRESS_KEYS: OnceLock<Mutex<Vec<RecoveryDomainKey>>> = OnceLock::new();

thread_local! {
    static SCHEMA_RECONCILED_KEYS: RefCell<Vec<SchemaReconciliationKey>> =
        const { RefCell::new(Vec::new()) };
    // Test stores use thread-local stable memory, so their recovered authority
    // must have the same ownership boundary.
    #[cfg(test)]
    static RECOVERED_KEYS: RefCell<Vec<RecoveryDomainKey>> =
        const { RefCell::new(Vec::new()) };
    #[cfg(test)]
    static RECOVERY_IN_PROGRESS_KEYS: RefCell<Vec<RecoveryDomainKey>> =
        const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SchemaReconciliationKey {
    store_registry: usize,
    runtime_hooks: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecoveryDomainKey {
    commit_allocation: CommitMemoryAllocation,
    schema: SchemaReconciliationKey,
}

/// Ensure global database invariants are restored before proceeding.
///
/// This function performs a **system recovery step**:
/// - It completes any marker-owned commit and derived-state rebuild forward.
/// - It leaves the database in a fully consistent state on return.
///
/// This function is:
/// - **Not part of mutation atomicity**
/// - **Mandatory before read execution**
/// - **Not conditional on read semantics**
///
/// It may be invoked at operation boundaries (including read or mutation
/// entrypoints), but must always complete **before** any operation-specific
/// planning, validation, or apply phase begins.
pub(crate) fn ensure_recovered<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    configure_commit_memory_id(C::COMMIT_MEMORY_ID, C::COMMIT_STABLE_KEY)
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    ensure_database_format_admitted(db)?;
    let recovery_key =
        recovery_domain_key(db).map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;

    if !recovery_domain_recovered(recovery_key)
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?
    {
        return recover_domain(db, recovery_key);
    }

    let recovery_in_progress = recovery_domain_in_progress(recovery_key);

    if !commit_marker_may_be_present() && !recovery_in_progress {
        return ensure_schema_reconciled(db);
    }

    if commit_marker_present_fast().map_err(|err| err.with_origin(ErrorOrigin::Recovery))? {
        return recover_domain(db, recovery_key);
    }

    if recovery_in_progress {
        // A previous recovery can be interrupted after marker clear but before
        // volatile readiness is restored. Marker absence alone is not enough
        // to prove this recovery domain completed.
        return recover_domain(db, recovery_key);
    }

    mark_commit_marker_verified_absent();

    ensure_schema_reconciled(db)
}

#[cfg(test)]
pub(in crate::db::commit) fn clear_recovery_in_progress_for_tests() {
    RECOVERY_IN_PROGRESS_KEYS.with(|keys| keys.borrow_mut().clear());
}

#[cfg(test)]
pub(in crate::db) fn clear_recovery_runtime_state_for_tests<C: CanisterKind>(
    db: &Db<C>,
) -> Result<(), InternalError> {
    let recovery_key = recovery_domain_key(db)?;
    RECOVERED_KEYS.with(|keys| {
        keys.try_borrow_mut()
            .map_err(|_| InternalError::store_invariant())?
            .retain(|existing| *existing != recovery_key);
        Ok::<(), InternalError>(())
    })?;

    clear_recovery_domain_in_progress(recovery_key);
    let schema_key = schema_reconciliation_key(db);
    SCHEMA_RECONCILED_KEYS.with(|keys| {
        keys.borrow_mut().retain(|existing| *existing != schema_key);
    });
    clear_database_format_admission_for_tests();

    Ok(())
}

fn recover_domain<C: CanisterKind>(
    db: &Db<C>,
    recovery_key: RecoveryDomainKey,
) -> Result<(), InternalError> {
    mark_recovery_domain_in_progress(recovery_key);
    let marker = with_commit_store(super::store::CommitStore::load)
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    // Ordinary row replay needs the current schema reconciled before it can
    // decode rows. A schema-publication marker instead owns accepted-after:
    // replay that candidate first, then reconcile the generated proposal
    // against the newly authoritative accepted schema.
    if !marker
        .as_ref()
        .is_some_and(marker_authorizes_schema_publication)
    {
        ensure_schema_reconciled_before_rebuild(db)?;
    }
    perform_recovery(db, marker)?;
    mark_recovery_domain_recovered(recovery_key)
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    clear_recovery_domain_in_progress(recovery_key);
    mark_schema_reconciliation_dirty(db);

    ensure_schema_reconciled(db)
}

fn perform_recovery<C: CanisterKind>(
    db: &Db<C>,
    marker: Option<CommitMarker>,
) -> Result<(), InternalError> {
    let had_marker = marker.is_some();
    if let Some(marker) = marker.as_ref() {
        publish_marker_bound_journal_batches(db, marker)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    }

    // Phase 1: fold committed journal-tail records into the canonical stable
    // base, then use the fold watermark as the replay boundary.
    if let Err(err) = fold_journaled_tails(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 2: rebuild journaled live projections from durable base + any
    // committed tail that remains above the fold watermark.
    if let Err(err) = rebuild_journaled_live_projections(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 3: rebuild secondary indexes from authoritative data rows.
    if let Err(err) = rebuild_secondary_indexes_from_rows(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 4: fold rebuilt journaled index materializations into canonical
    // index storage. Indexes are derived state, not independent journal truth.
    if let Err(err) = fold_journaled_index_materialized_views(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 5: verify only marker-owned effects and terminal fold state before
    // clearing marker authority. Whole-database integrity is an explicit
    // bounded inspection workflow, not a recovery side effect.
    if let Err(err) = verify_recovered_effects(db, marker.as_ref()) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 6: clear marker only after replay + rebuild + integrity validation succeed.
    if had_marker {
        with_commit_store(super::store::CommitStore::clear_verified)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    }

    // Phase 7: authoritative rebuild succeeded, so every registered index is
    // query-visible again.
    db.mark_all_registered_index_stores_ready();
    mark_commit_marker_verified_absent();

    Ok(())
}

fn marker_authorizes_schema_publication(marker: &CommitMarker) -> bool {
    marker.journal_batches().iter().any(|batch| {
        batch
            .records()
            .iter()
            .any(|record| matches!(record, JournalRecord::AcceptedSchemaPublish { .. }))
    })
}

fn publish_marker_bound_journal_batches<C: CanisterKind>(
    db: &Db<C>,
    marker: &CommitMarker,
) -> Result<(), InternalError> {
    for batch in marker.journal_batches() {
        let (_, handle) = journal_batch_store_handle(db, batch)?;
        let journal_store = handle
            .journal_tail_store()
            .ok_or_else(InternalError::store_corruption)?;
        journal_store.with_borrow_mut(|store| {
            #[cfg(test)]
            hit_commit_failpoint(CommitFailpoint::BeforeMarkerBoundJournalAppend)?;
            store.append_batch(batch)?;
            #[cfg(test)]
            hit_commit_failpoint(CommitFailpoint::AfterMarkerBoundJournalAppend)?;

            Ok::<(), InternalError>(())
        })?;
    }

    Ok(())
}

fn rebuild_journaled_live_projections<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    let stores = sorted_journaled_store_handles(db);
    for (_, handle) in &stores {
        handle.with_data_mut(DataStore::reset_journaled_live_projection)?;
        handle.with_schema_mut(SchemaStore::reset_journaled_live_projection)?;
    }

    for (store_path, handle) in stores {
        let journal_store = handle
            .journal_tail_store()
            .ok_or_else(InternalError::store_corruption)?;
        journal_store.with_borrow(|store| {
            let watermark = store.fold_watermark()?.highest_folded_journal_sequence();
            store.visit_batches_after(watermark, |batch| {
                replay_journal_batch(db, store_path, handle, batch)?;
                Ok(())
            })
        })?;
    }

    Ok(())
}

fn fold_journaled_tails<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    for (store_path, handle) in sorted_journaled_store_handles(db) {
        // Tail records form a sequence over canonical state. A newer volatile
        // schema overlay may contain later activation/job state and must not
        // become the predecessor used to validate an earlier fold record.
        // Recovery already owns exclusivity, and phase 2 rebuilds this
        // disposable projection from the resulting canonical state.
        handle.with_schema_mut(SchemaStore::reset_journaled_live_projection)?;
        let journal_store = handle
            .journal_tail_store()
            .ok_or_else(InternalError::store_corruption)?;
        let watermark = journal_store.with_borrow(JournalTailStore::fold_watermark)?;
        let mut highest_folded = watermark.highest_folded_journal_sequence();

        journal_store.with_borrow(|store| {
            store.visit_batches_after(watermark.highest_folded_journal_sequence(), |batch| {
                #[cfg(test)]
                hit_commit_failpoint(CommitFailpoint::BeforeJournalTailFoldBatch)?;
                fold_journal_batch(db, store_path, handle, batch)?;
                highest_folded = batch.journal_sequence();
                Ok(())
            })
        })?;

        if highest_folded > watermark.highest_folded_journal_sequence() {
            let next_epoch = watermark
                .fold_epoch()
                .checked_add(1)
                .ok_or_else(InternalError::store_corruption)?;
            let next_watermark = FoldWatermark::new(highest_folded, next_epoch);
            journal_store.with_borrow_mut(|store| {
                store.persist_fold_watermark(next_watermark)?;
                #[cfg(test)]
                hit_commit_failpoint(CommitFailpoint::AfterJournalTailFoldWatermarkPersist)?;
                store.clear_batches_through(highest_folded);

                Ok::<(), InternalError>(())
            })?;
        } else if watermark.highest_folded_journal_sequence() > JournalSequence::new(0) {
            journal_store.with_borrow_mut(|store| {
                store.clear_batches_through(watermark.highest_folded_journal_sequence());

                Ok::<(), InternalError>(())
            })?;
        }
    }

    Ok(())
}

fn fold_journaled_index_materialized_views<C: CanisterKind>(
    db: &Db<C>,
) -> Result<(), InternalError> {
    for (_, handle) in sorted_journaled_store_handles(db) {
        handle.with_index_mut(IndexStore::fold_journaled_materialized_view)?;
        #[cfg(test)]
        hit_commit_failpoint(CommitFailpoint::AfterJournaledIndexMaterializedViewFold)?;
    }

    Ok(())
}

fn sorted_journaled_store_handles<C: CanisterKind>(db: &Db<C>) -> Vec<(&'static str, StoreHandle)> {
    let mut stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    stores.retain(|(_, handle)| {
        handle.storage_capabilities().recovery()
            == StoreRecoveryCapability::StableBasePlusJournalReplay
    });
    stores.sort_by_key(|(path, _)| *path);
    stores
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalRecordApplyMode {
    Replay,
    Fold,
}

fn replay_journal_batch<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
) -> Result<(), InternalError> {
    let (_, batch_handle) = journal_batch_store_handle(db, batch)?;
    if !std::ptr::eq(batch_handle.data_store(), expected_handle.data_store()) {
        return Err(InternalError::store_corruption());
    }
    let _candidate = validate_journal_batch_records(
        db,
        expected_store_path,
        expected_handle,
        batch,
        JournalRecordApplyMode::Replay,
    )?;

    for record in batch.records() {
        replay_journal_record(db, expected_store_path, expected_handle, record)?;
    }

    Ok(())
}

fn fold_journal_batch<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
) -> Result<(), InternalError> {
    let (_, batch_handle) = journal_batch_store_handle(db, batch)?;
    if !std::ptr::eq(batch_handle.data_store(), expected_handle.data_store()) {
        return Err(InternalError::store_corruption());
    }
    let _candidate = validate_journal_batch_records(
        db,
        expected_store_path,
        expected_handle,
        batch,
        JournalRecordApplyMode::Fold,
    )?;

    for record in batch.records() {
        fold_journal_record(db, expected_store_path, expected_handle, record)?;
    }

    Ok(())
}

fn replay_journal_record<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    record: &JournalRecord,
) -> Result<(), InternalError> {
    apply_journal_record(
        db,
        expected_store_path,
        expected_handle,
        record,
        JournalRecordApplyMode::Replay,
    )
}

fn fold_journal_record<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    record: &JournalRecord,
) -> Result<(), InternalError> {
    apply_journal_record(
        db,
        expected_store_path,
        expected_handle,
        record,
        JournalRecordApplyMode::Fold,
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "recovery keeps every journal record's replay and fold behavior in one exhaustive authority"
)]
fn apply_journal_record<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    record: &JournalRecord,
    mode: JournalRecordApplyMode,
) -> Result<(), InternalError> {
    match record {
        JournalRecord::RowPut {
            primary_key,
            row_bytes,
            ..
        } => {
            let row =
                RawRow::from_untrusted_bytes(row_bytes.clone()).map_err(InternalError::from)?;
            expected_handle.with_data_mut(|store| match mode {
                JournalRecordApplyMode::Replay => store
                    .apply_recovered_journal_put(primary_key.clone(), row)
                    .map(|_| ()),
                JournalRecordApplyMode::Fold => store
                    .fold_recovered_journal_put(primary_key.clone(), row)
                    .map(|_| ()),
            })
        }
        JournalRecord::RowDelete { primary_key, .. } => {
            expected_handle.with_data_mut(|store| match mode {
                JournalRecordApplyMode::Replay => store
                    .apply_recovered_journal_delete(primary_key)
                    .map(|_| ()),
                JournalRecordApplyMode::Fold => {
                    store.fold_recovered_journal_delete(primary_key).map(|_| ())
                }
            })
        }
        JournalRecord::SchemaPut {
            store_path,
            schema_snapshot_bytes,
        } => {
            if store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            let snapshot = decode_persisted_schema_snapshot(schema_snapshot_bytes)?;
            let hooks = db.runtime_hook_for_entity_path(snapshot.entity_path())?;
            if hooks.store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            expected_handle.with_schema_mut(|schema_store| match mode {
                JournalRecordApplyMode::Replay => {
                    schema_store.insert_persisted_snapshot(hooks.entity_tag, &snapshot)
                }
                JournalRecordApplyMode::Fold => {
                    schema_store.fold_persisted_snapshot(hooks.entity_tag, &snapshot)
                }
            })
        }
        JournalRecord::AcceptedSchemaPublish {
            store_path,
            expected_revision,
            schema_bundle_bytes,
            schema_root_bytes,
        } => {
            if store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            let candidate = crate::db::schema::CandidateSchemaRevision::from_encoded(
                schema_bundle_bytes.clone(),
                schema_root_bytes.clone(),
            )?;
            if candidate.store_path() != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            expected_handle.with_schema_mut(|schema_store| match mode {
                JournalRecordApplyMode::Replay => schema_store
                    .apply_journaled_accepted_schema_candidate(*expected_revision, &candidate),
                JournalRecordApplyMode::Fold => schema_store
                    .fold_journaled_accepted_schema_candidate(*expected_revision, &candidate),
            })
        }
        JournalRecord::ConstraintValidationJobPut {
            store_path,
            entity_tag,
            constraint_id,
            job_bytes,
        } => {
            validate_constraint_validation_job_record_identity(
                db,
                expected_store_path,
                store_path,
                *entity_tag,
                *constraint_id,
            )?;
            let job = decode_constraint_validation_job(job_bytes)?;
            if job.entity_tag() != *entity_tag || job.constraint_id() != *constraint_id {
                return Err(InternalError::store_corruption());
            }
            expected_handle.with_schema_mut(|schema_store| match mode {
                JournalRecordApplyMode::Replay => {
                    schema_store.apply_constraint_validation_job(&job)
                }
                JournalRecordApplyMode::Fold => schema_store.fold_constraint_validation_job(&job),
            })
        }
        JournalRecord::ConstraintValidationJobDelete {
            store_path,
            entity_tag,
            constraint_id,
        } => {
            validate_constraint_validation_job_record_identity(
                db,
                expected_store_path,
                store_path,
                *entity_tag,
                *constraint_id,
            )?;
            expected_handle.with_schema_mut(|schema_store| match mode {
                JournalRecordApplyMode::Replay => schema_store
                    .apply_constraint_validation_job_removal(*entity_tag, *constraint_id),
                JournalRecordApplyMode::Fold => {
                    schema_store.fold_constraint_validation_job_removal(*entity_tag, *constraint_id)
                }
            })
        }
    }
}

fn validate_journal_batch_records<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
    mode: JournalRecordApplyMode,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    let candidate = journal_batch_schema_candidate(db, expected_store_path, batch)?;
    validate_journal_batch_constraint_validation_job_change(
        db,
        expected_store_path,
        expected_handle,
        batch,
        candidate.as_ref(),
    )?;

    for record in batch.records() {
        match record {
            JournalRecord::RowPut { .. } => {
                validate_journal_batch_row_put(
                    db,
                    expected_store_path,
                    expected_handle,
                    candidate.as_ref(),
                    record,
                    mode,
                )?;
            }
            JournalRecord::RowDelete {
                entity_path,
                primary_key,
                schema_fingerprint,
            } => validate_journal_batch_row_delete(
                db,
                expected_store_path,
                expected_handle,
                entity_path,
                primary_key,
                *schema_fingerprint,
                mode,
            )?,
            JournalRecord::SchemaPut {
                store_path,
                schema_snapshot_bytes,
            } => {
                if store_path != expected_store_path {
                    return Err(InternalError::store_corruption());
                }
                let snapshot = decode_persisted_schema_snapshot(schema_snapshot_bytes)?;
                let hooks = db.runtime_hook_for_entity_path(snapshot.entity_path())?;
                if hooks.store_path != expected_store_path {
                    return Err(InternalError::store_corruption());
                }
            }
            JournalRecord::AcceptedSchemaPublish { .. }
            | JournalRecord::ConstraintValidationJobPut { .. }
            | JournalRecord::ConstraintValidationJobDelete { .. } => {
                // The first pass decoded and verified the candidate before any
                // candidate-bound row rewrite was admitted, including the exact
                // final activation/job closure.
            }
        }
    }

    Ok(candidate)
}

fn validate_journal_batch_row_put<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    candidate: Option<&CandidateSchemaRevision>,
    record: &JournalRecord,
    mode: JournalRecordApplyMode,
) -> Result<(), InternalError> {
    let JournalRecord::RowPut {
        entity_path,
        primary_key,
        row_bytes,
        schema_fingerprint,
    } = record
    else {
        return Err(InternalError::store_invariant());
    };
    if let Some(candidate) = candidate {
        return validate_candidate_journal_row_put(
            db,
            expected_store_path,
            candidate,
            entity_path,
            primary_key,
            row_bytes,
            *schema_fingerprint,
        );
    }

    match mode {
        JournalRecordApplyMode::Replay => {
            validate_journal_row_record(
                db,
                expected_store_path,
                expected_handle,
                entity_path,
                primary_key,
                schema_fingerprint,
            )?;
            RawRow::from_untrusted_bytes(row_bytes.clone()).map_err(InternalError::from)?;
            validate_journal_row_put_preflight_if_needed(
                db,
                expected_handle,
                entity_path,
                primary_key,
                row_bytes,
                *schema_fingerprint,
            )
        }
        JournalRecordApplyMode::Fold => validate_canonical_journal_row_put(
            db,
            expected_store_path,
            expected_handle,
            entity_path,
            primary_key,
            row_bytes,
            *schema_fingerprint,
        ),
    }
}

fn validate_journal_batch_row_delete<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    schema_fingerprint: [u8; 16],
    mode: JournalRecordApplyMode,
) -> Result<(), InternalError> {
    match mode {
        JournalRecordApplyMode::Replay => {
            validate_journal_row_record(
                db,
                expected_store_path,
                expected_handle,
                entity_path,
                primary_key,
                &schema_fingerprint,
            )?;
            validate_journal_row_delete_preflight_if_needed(
                db,
                expected_handle,
                entity_path,
                primary_key,
                schema_fingerprint,
            )
        }
        JournalRecordApplyMode::Fold => canonical_journal_row_selection(
            db,
            expected_store_path,
            expected_handle,
            entity_path,
            primary_key,
            schema_fingerprint,
        )
        .map(drop),
    }
}

fn journal_batch_schema_candidate<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    batch: &JournalBatch,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    let mut candidate = None;
    for (position, record) in batch.records().iter().enumerate() {
        match record {
            JournalRecord::AcceptedSchemaPublish {
                store_path,
                expected_revision,
                schema_bundle_bytes,
                schema_root_bytes,
            } => {
                if position != 0 || candidate.is_some() || store_path != expected_store_path {
                    return Err(InternalError::store_corruption());
                }
                let decoded = CandidateSchemaRevision::from_encoded(
                    schema_bundle_bytes.clone(),
                    schema_root_bytes.clone(),
                )?;
                if decoded.store_path() != expected_store_path
                    || expected_revision.checked_next() != Some(decoded.revision())
                {
                    return Err(InternalError::store_corruption());
                }
                for (entity_tag, snapshot) in decoded.bundle().entity_snapshots() {
                    let hooks = db.runtime_hook_for_entity_path(snapshot.entity_path())?;
                    if hooks.store_path != expected_store_path || hooks.entity_tag != *entity_tag {
                        return Err(InternalError::store_corruption());
                    }
                }
                candidate = Some(decoded);
            }
            JournalRecord::RowDelete { .. } | JournalRecord::SchemaPut { .. }
                if candidate.is_some() =>
            {
                return Err(InternalError::store_corruption());
            }
            JournalRecord::RowPut { .. }
            | JournalRecord::RowDelete { .. }
            | JournalRecord::SchemaPut { .. }
            | JournalRecord::ConstraintValidationJobPut { .. }
            | JournalRecord::ConstraintValidationJobDelete { .. } => {}
        }
    }

    Ok(candidate)
}

fn validate_journal_batch_constraint_validation_job_change<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
    candidate: Option<&CandidateSchemaRevision>,
) -> Result<(), InternalError> {
    let mut replacement = None;
    let mut removal = None;
    for record in batch.records() {
        match record {
            JournalRecord::ConstraintValidationJobPut {
                store_path,
                entity_tag,
                constraint_id,
                job_bytes,
            } => {
                if replacement.is_some() || removal.is_some() {
                    return Err(InternalError::store_corruption());
                }
                validate_constraint_validation_job_record_identity(
                    db,
                    expected_store_path,
                    store_path,
                    *entity_tag,
                    *constraint_id,
                )?;
                let job = decode_constraint_validation_job(job_bytes)?;
                if job.entity_tag() != *entity_tag || job.constraint_id() != *constraint_id {
                    return Err(InternalError::store_corruption());
                }
                replacement = Some(job);
            }
            JournalRecord::ConstraintValidationJobDelete {
                store_path,
                entity_tag,
                constraint_id,
            } => {
                if replacement.is_some() || removal.is_some() {
                    return Err(InternalError::store_corruption());
                }
                validate_constraint_validation_job_record_identity(
                    db,
                    expected_store_path,
                    store_path,
                    *entity_tag,
                    *constraint_id,
                )?;
                removal = Some((*entity_tag, *constraint_id));
            }
            _ => {}
        }
    }

    let candidate_bundle = candidate.map(CandidateSchemaRevision::bundle);
    expected_handle.with_schema(|schema_store| {
        if let Some(bundle) = candidate_bundle {
            schema_store.validate_live_activation_transition(bundle)?;
        }
        if replacement.is_none() && removal.is_none() {
            if let Some(bundle) = candidate_bundle {
                schema_store.validate_constraint_validation_job_closure(bundle)?;
            }
            return Ok(());
        }
        let bundle = match candidate_bundle {
            Some(bundle) => bundle.clone(),
            None => schema_store
                .current_accepted_schema_bundle()?
                .ok_or_else(InternalError::store_corruption)?,
        };
        schema_store.validate_constraint_validation_job_closure_with_change(
            &bundle,
            replacement.as_ref(),
            removal,
        )
    })
}

fn validate_constraint_validation_job_record_identity<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    record_store_path: &str,
    entity_tag: crate::types::EntityTag,
    _constraint_id: crate::db::schema::ConstraintId,
) -> Result<(), InternalError> {
    if record_store_path != expected_store_path {
        return Err(InternalError::store_corruption());
    }
    let hooks = db
        .runtime_hook_for_entity_tag(entity_tag)
        .map_err(|_| InternalError::store_corruption())?;
    if hooks.store_path != expected_store_path {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

fn validate_candidate_journal_row_put<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    candidate: &CandidateSchemaRevision,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    row_bytes: &[u8],
    schema_fingerprint: [u8; 16],
) -> Result<(), InternalError> {
    let decoded_key = DecodedDataStoreKey::try_from_raw(primary_key)
        .map_err(|_| InternalError::store_corruption())?;
    let hooks = recovery_runtime_hook_for_entity_path(db, entity_path)?;
    if hooks.store_path != expected_store_path || decoded_key.entity_tag() != hooks.entity_tag {
        return Err(InternalError::store_corruption());
    }
    let selection = crate::db::schema::AcceptedCatalogSnapshotSelection::from_candidate(
        candidate,
        hooks.entity_tag,
        hooks.entity_path,
        hooks.store_path,
    )?
    .ok_or_else(InternalError::store_corruption)?;
    if selection.identity().accepted_schema_fingerprint() != schema_fingerprint {
        return Err(InternalError::store_corruption());
    }
    let row = RawRow::from_untrusted_bytes(row_bytes.to_vec()).map_err(InternalError::from)?;
    let contract =
        AcceptedStructuralRowAuthority::from_catalog_selection(hooks.entity_path, &selection)?
            .into_row_contract();
    let reader = StructuralSlotReader::from_raw_row_with_validated_contract(&row, contract)?;
    reader.validate_primary_key(&decoded_key)
}

fn validate_canonical_journal_row_put<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    row_bytes: &[u8],
    schema_fingerprint: [u8; 16],
) -> Result<(), InternalError> {
    let (decoded_key, selection) = canonical_journal_row_selection(
        db,
        expected_store_path,
        expected_handle,
        entity_path,
        primary_key,
        schema_fingerprint,
    )?;
    let row = RawRow::from_untrusted_bytes(row_bytes.to_vec()).map_err(InternalError::from)?;
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(
        selection.identity().entity_path(),
        &selection,
    )?
    .into_row_contract();
    let reader = StructuralSlotReader::from_raw_row_with_validated_contract(&row, contract)?;
    reader.validate_primary_key(&decoded_key)
}

fn canonical_journal_row_selection<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    schema_fingerprint: [u8; 16],
) -> Result<(DecodedDataStoreKey, AcceptedCatalogSnapshotSelection), InternalError> {
    let decoded_key = DecodedDataStoreKey::try_from_raw(primary_key)
        .map_err(|_| InternalError::store_corruption())?;
    let hooks = recovery_runtime_hook_for_entity_path(db, entity_path)?;
    if hooks.store_path != expected_store_path || decoded_key.entity_tag() != hooks.entity_tag {
        return Err(InternalError::store_corruption());
    }
    let selection = expected_handle
        .with_schema(|schema_store| {
            schema_store.current_canonical_accepted_catalog_selection(
                hooks.entity_tag,
                hooks.entity_path,
                hooks.store_path,
            )
        })?
        .ok_or_else(InternalError::store_corruption)?;
    if selection.identity().accepted_schema_fingerprint() != schema_fingerprint {
        return Err(InternalError::store_corruption());
    }

    Ok((decoded_key, selection))
}

fn validate_journal_row_record<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    schema_fingerprint: &[u8; 16],
) -> Result<(), InternalError> {
    let decoded_key = DecodedDataStoreKey::try_from_raw(primary_key)
        .map_err(|_| InternalError::store_corruption())?;
    let hooks = recovery_runtime_hook_for_entity_path(db, entity_path)?;
    if hooks.store_path != expected_store_path || decoded_key.entity_tag() != hooks.entity_tag {
        return Err(InternalError::store_corruption());
    }
    let accepted = expected_handle.with_schema_mut(|schema_store| {
        ensure_accepted_schema_snapshot(
            schema_store,
            hooks.entity_tag,
            hooks.entity_path,
            hooks.store_path,
            hooks.model,
        )
    })?;
    let expected_fingerprint = accepted_commit_schema_fingerprint(&accepted)?;
    if &expected_fingerprint != schema_fingerprint {
        return Err(InternalError::store_corruption());
    }

    Ok(())
}

// Generated-hook recovery can validate unapplied journal row effects through
// normal commit preflight. Already-reflected effects must skip that path because
// commit preflight is stateful against the current live projection.
fn validate_journal_row_put_preflight_if_needed<C: CanisterKind>(
    db: &Db<C>,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    row_bytes: &[u8],
    schema_fingerprint: [u8; 16],
) -> Result<(), InternalError> {
    if expected_handle.with_data(|store| {
        store
            .get(primary_key)
            .is_some_and(|row| row.as_bytes() == row_bytes)
    }) {
        return Ok(());
    }

    let hooks = recovery_runtime_hook_for_entity_path(db, entity_path)?;
    let before = expected_handle
        .with_data(|store| store.get(primary_key).map(|row| row.as_bytes().to_vec()));
    let op = CommitRowOp::try_new_bytes(
        hooks.entity_path,
        primary_key.as_bytes(),
        before,
        Some(row_bytes.to_vec()),
        schema_fingerprint,
    )?;
    db.prepare_row_commit_op(&op)?;

    Ok(())
}

fn validate_journal_row_delete_preflight_if_needed<C: CanisterKind>(
    db: &Db<C>,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    schema_fingerprint: [u8; 16],
) -> Result<(), InternalError> {
    if !expected_handle.with_data(|store| store.contains(primary_key)) {
        return Ok(());
    }

    let hooks = recovery_runtime_hook_for_entity_path(db, entity_path)?;
    let before = expected_handle
        .with_data(|store| store.get(primary_key).map(|row| row.as_bytes().to_vec()));
    let op = CommitRowOp::try_new_bytes(
        hooks.entity_path,
        primary_key.as_bytes(),
        before,
        None,
        schema_fingerprint,
    )?;
    db.prepare_row_commit_op(&op)?;

    Ok(())
}

fn journal_batch_store_handle<C: CanisterKind>(
    db: &Db<C>,
    batch: &JournalBatch,
) -> Result<(&'static str, StoreHandle), InternalError> {
    let mut resolved = None::<(&'static str, StoreHandle)>;
    for record in batch.records() {
        let (path, handle) = journal_record_store_handle(db, record)?;
        if let Some((existing_path, _)) = resolved {
            if existing_path != path {
                return Err(InternalError::store_corruption());
            }
        } else {
            resolved = Some((path, handle));
        }
    }

    let Some((path, handle)) = resolved else {
        return Err(InternalError::store_corruption());
    };
    if handle.storage_capabilities().recovery()
        != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_corruption());
    }

    Ok((path, handle))
}

fn journal_record_store_handle<C: CanisterKind>(
    db: &Db<C>,
    record: &JournalRecord,
) -> Result<(&'static str, StoreHandle), InternalError> {
    match record {
        JournalRecord::RowPut { entity_path, .. }
        | JournalRecord::RowDelete { entity_path, .. } => {
            journal_row_record_store_handle(db, entity_path.as_str(), record)
        }
        JournalRecord::SchemaPut { store_path, .. }
        | JournalRecord::AcceptedSchemaPublish { store_path, .. }
        | JournalRecord::ConstraintValidationJobPut { store_path, .. }
        | JournalRecord::ConstraintValidationJobDelete { store_path, .. } => {
            registry_store_handle_for_path(db, store_path)
        }
    }
}

fn registry_store_handle_for_path<C: CanisterKind>(
    db: &Db<C>,
    store_path: &str,
) -> Result<(&'static str, StoreHandle), InternalError> {
    db.with_store_registry(|registry| {
        registry
            .iter()
            .find(|(path, _)| *path == store_path)
            .ok_or_else(InternalError::store_corruption)
    })
}

fn journal_row_record_store_handle<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &str,
    _record: &JournalRecord,
) -> Result<(&'static str, StoreHandle), InternalError> {
    let hooks = recovery_runtime_hook_for_entity_path(db, entity_path)?;
    registry_store_handle_for_path(db, hooks.store_path)
}

fn recovery_runtime_hook_for_entity_path<'a, C: CanisterKind>(
    db: &'a Db<C>,
    entity_path: &str,
) -> Result<&'a EntityRuntimeHooks<C>, InternalError> {
    db.runtime_hook_for_entity_path(entity_path)
        .map_err(|_| InternalError::store_corruption())
}

// Reconcile generated entity metadata with the schema store once per generated
// store registry. This keeps the fast recovery path cheap while still allowing
// independent test registries and canister domains to initialize their own
// schema metadata.
fn ensure_schema_reconciled<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    ensure_schema_reconciled_for_phase(db, SchemaReconciliationPhase::Ordinary)
}

fn ensure_schema_reconciled_before_rebuild<C: CanisterKind>(
    db: &Db<C>,
) -> Result<(), InternalError> {
    ensure_schema_reconciled_for_phase(db, SchemaReconciliationPhase::BeforeRecoveryRebuild)
}

/// Derived-state phase paired with one schema-reconciliation invocation.
///
/// Recovery owns this distinction so schema code never infers rebuild
/// authority from marker presence or index readiness.
#[derive(Clone, Copy)]
enum SchemaReconciliationPhase {
    Ordinary,
    BeforeRecoveryRebuild,
}

fn ensure_schema_reconciled_for_phase<C: CanisterKind>(
    db: &Db<C>,
    phase: SchemaReconciliationPhase,
) -> Result<(), InternalError> {
    let key = schema_reconciliation_key(db);
    if schema_reconciliation_clean(key) {
        return Ok(());
    }

    match phase {
        SchemaReconciliationPhase::Ordinary => {
            reconcile_runtime_schemas(db, db.entity_runtime_hooks)
        }
        SchemaReconciliationPhase::BeforeRecoveryRebuild => {
            reconcile_runtime_schemas_before_recovery_rebuild(db, db.entity_runtime_hooks)
        }
    }
    .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    mark_schema_reconciliation_clean(key);

    Ok(())
}

fn schema_reconciliation_key<C: CanisterKind>(db: &Db<C>) -> SchemaReconciliationKey {
    SchemaReconciliationKey {
        store_registry: std::ptr::from_ref(db.store).cast::<()>() as usize,
        runtime_hooks: db.entity_runtime_hooks.as_ptr().cast::<()>() as usize,
    }
}

fn recovery_domain_key<C: CanisterKind>(db: &Db<C>) -> Result<RecoveryDomainKey, InternalError> {
    Ok(RecoveryDomainKey {
        commit_allocation: current_commit_memory_allocation()?,
        schema: schema_reconciliation_key(db),
    })
}

#[cfg(not(test))]
fn recovered_keys() -> &'static Mutex<Vec<RecoveryDomainKey>> {
    RECOVERED_KEYS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(not(test))]
fn recovery_in_progress_keys() -> &'static Mutex<Vec<RecoveryDomainKey>> {
    RECOVERY_IN_PROGRESS_KEYS.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(not(test))]
fn recovery_domain_recovered(key: RecoveryDomainKey) -> Result<bool, InternalError> {
    recovered_keys()
        .lock()
        .map(|keys| keys.contains(&key))
        .map_err(|_| InternalError::store_invariant())
}

#[cfg(test)]
fn recovery_domain_recovered(key: RecoveryDomainKey) -> Result<bool, InternalError> {
    RECOVERED_KEYS.with(|keys| {
        Ok(keys
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?
            .contains(&key))
    })
}

#[cfg(not(test))]
fn recovery_domain_in_progress(key: RecoveryDomainKey) -> bool {
    recovery_in_progress_keys()
        .lock()
        .map_or(true, |keys| keys.contains(&key))
}

#[cfg(test)]
fn recovery_domain_in_progress(key: RecoveryDomainKey) -> bool {
    RECOVERY_IN_PROGRESS_KEYS.with(|keys| keys.borrow().contains(&key))
}

#[cfg(not(test))]
fn mark_recovery_domain_recovered(key: RecoveryDomainKey) -> Result<(), InternalError> {
    {
        let mut keys = recovered_keys()
            .lock()
            .map_err(|_| InternalError::store_invariant())?;
        if !keys.contains(&key) {
            keys.push(key);
        }
    }

    Ok(())
}

#[cfg(test)]
fn mark_recovery_domain_recovered(key: RecoveryDomainKey) -> Result<(), InternalError> {
    RECOVERED_KEYS.with(|keys| {
        let mut keys = keys
            .try_borrow_mut()
            .map_err(|_| InternalError::store_invariant())?;
        if !keys.contains(&key) {
            keys.push(key);
        }

        Ok(())
    })
}

#[cfg(not(test))]
fn mark_recovery_domain_in_progress(key: RecoveryDomainKey) {
    if let Ok(mut keys) = recovery_in_progress_keys().lock()
        && !keys.contains(&key)
    {
        keys.push(key);
    }
}

#[cfg(test)]
fn mark_recovery_domain_in_progress(key: RecoveryDomainKey) {
    RECOVERY_IN_PROGRESS_KEYS.with(|keys| {
        let mut keys = keys.borrow_mut();
        if !keys.contains(&key) {
            keys.push(key);
        }
    });
}

#[cfg(not(test))]
fn clear_recovery_domain_in_progress(key: RecoveryDomainKey) {
    if let Ok(mut keys) = recovery_in_progress_keys().lock() {
        keys.retain(|existing| *existing != key);
    }
}

#[cfg(test)]
fn clear_recovery_domain_in_progress(key: RecoveryDomainKey) {
    RECOVERY_IN_PROGRESS_KEYS.with(|keys| {
        keys.borrow_mut().retain(|existing| *existing != key);
    });
}

fn schema_reconciliation_clean(key: SchemaReconciliationKey) -> bool {
    SCHEMA_RECONCILED_KEYS.with(|keys| keys.borrow().contains(&key))
}

fn mark_schema_reconciliation_clean(key: SchemaReconciliationKey) {
    SCHEMA_RECONCILED_KEYS.with(|keys| {
        let mut keys = keys.borrow_mut();
        if !keys.contains(&key) {
            keys.push(key);
        }
    });
}

#[cfg(test)]
pub(in crate::db::commit) fn mark_schema_reconciliation_dirty_for_tests<C: CanisterKind>(
    db: &Db<C>,
) {
    mark_schema_reconciliation_dirty(db);
}

fn mark_schema_reconciliation_dirty<C: CanisterKind>(db: &Db<C>) {
    let key = schema_reconciliation_key(db);
    SCHEMA_RECONCILED_KEYS.with(|keys| {
        keys.borrow_mut().retain(|existing| *existing != key);
    });
}
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RecoveredEffectIdentity {
    Row {
        entity_path: String,
        primary_key: Vec<u8>,
    },
    Schema {
        store_path: String,
        entity_tag: u64,
        schema_version: u32,
    },
    AcceptedSchema {
        store_path: String,
    },
    ConstraintValidationJob {
        store_path: String,
        entity_tag: u64,
        constraint_id: u32,
    },
}

// Verify the bounded final effect set owned by the recovered marker.
//
// The marker is capped by `MAX_COMMIT_BYTES`; reverse traversal retains only
// the last record for each logical target. Tail folds and full derived-state
// rebuilds have already completed, so row deletes need only prove authoritative
// row absence: the rebuilt index store cannot retain entries from absent rows.
pub(in crate::db::commit) fn verify_recovered_effects<C: CanisterKind>(
    db: &Db<C>,
    marker: Option<&CommitMarker>,
) -> Result<(), InternalError> {
    let mut verified = BTreeSet::new();

    if let Some(marker) = marker {
        for batch in marker.journal_batches().iter().rev() {
            let (_, handle) = journal_batch_store_handle(db, batch)?;
            let watermark = handle
                .journal_tail_store()
                .ok_or_else(InternalError::recovery_effect_verification_failed)?
                .with_borrow(JournalTailStore::fold_watermark)?;
            if watermark.highest_folded_journal_sequence() < batch.journal_sequence() {
                return Err(InternalError::recovery_effect_verification_failed());
            }

            for record in batch.records().iter().rev() {
                verify_recovered_record(db, record, &mut verified)?;
            }
        }
    }

    // Every journaled store must have reached one terminal fold boundary.
    // This is one ordered-map lookup per registered store, not a tail scan.
    for (_, handle) in sorted_journaled_store_handles(db) {
        let journal_store = handle
            .journal_tail_store()
            .ok_or_else(InternalError::recovery_effect_verification_failed)?;
        let has_stored_batch = journal_store.with_borrow(JournalTailStore::has_stored_batch);
        if has_stored_batch {
            return Err(InternalError::recovery_effect_verification_failed());
        }
    }

    Ok(())
}

fn verify_recovered_record<C: CanisterKind>(
    db: &Db<C>,
    record: &JournalRecord,
    verified: &mut BTreeSet<RecoveredEffectIdentity>,
) -> Result<(), InternalError> {
    match record {
        JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            schema_fingerprint,
        } => verify_recovered_row_put(
            db,
            entity_path,
            primary_key,
            row_bytes,
            *schema_fingerprint,
            verified,
        )?,
        JournalRecord::RowDelete {
            entity_path,
            primary_key,
            ..
        } => verify_recovered_row_delete(db, entity_path, primary_key, verified)?,
        JournalRecord::SchemaPut {
            store_path,
            schema_snapshot_bytes,
        } => verify_recovered_schema_put(db, store_path, schema_snapshot_bytes, verified)?,
        JournalRecord::AcceptedSchemaPublish {
            store_path,
            schema_bundle_bytes,
            schema_root_bytes,
            ..
        } => verify_recovered_accepted_schema(
            db,
            store_path,
            schema_bundle_bytes,
            schema_root_bytes,
            verified,
        )?,
        JournalRecord::ConstraintValidationJobPut {
            store_path,
            entity_tag,
            constraint_id,
            job_bytes,
        } => verify_recovered_validation_job(
            db,
            store_path,
            *entity_tag,
            *constraint_id,
            Some(job_bytes),
            verified,
        )?,
        JournalRecord::ConstraintValidationJobDelete {
            store_path,
            entity_tag,
            constraint_id,
        } => verify_recovered_validation_job(
            db,
            store_path,
            *entity_tag,
            *constraint_id,
            None,
            verified,
        )?,
    }

    Ok(())
}

fn verify_recovered_row_put<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    row_bytes: &[u8],
    schema_fingerprint: CommitSchemaFingerprint,
    verified: &mut BTreeSet<RecoveredEffectIdentity>,
) -> Result<(), InternalError> {
    let identity = RecoveredEffectIdentity::Row {
        entity_path: entity_path.to_string(),
        primary_key: primary_key.as_bytes().to_vec(),
    };
    if !verified.insert(identity) {
        return Ok(());
    }

    let hooks = recovery_runtime_hook_for_entity_path(db, entity_path)?;
    let (_, handle) = registry_store_handle_for_path(db, hooks.store_path)?;
    let row_matches = handle
        .with_data(|store| store.get(primary_key))
        .is_some_and(|row| row.as_bytes() == row_bytes);
    if !row_matches {
        return Err(InternalError::recovery_effect_verification_failed());
    }

    let row_op = CommitRowOp::new(
        entity_path.to_string(),
        primary_key.clone(),
        None,
        Some(row_bytes.to_vec()),
        schema_fingerprint,
    );
    let prepared = db.prepare_row_commit_op_for_rebuild(&row_op)?;
    if !std::ptr::eq(prepared.data_store, handle.data_store())
        || prepared.data_key != *primary_key
        || prepared
            .data_value
            .as_ref()
            .is_none_or(|row| row.as_raw_row().as_bytes() != row_bytes)
    {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    for index_op in prepared.index_ops {
        let actual = index_op
            .index_store
            .with_borrow(|store| store.get(&index_op.key));
        if actual != index_op.value {
            return Err(InternalError::recovery_effect_verification_failed());
        }
    }

    Ok(())
}

fn verify_recovered_row_delete<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    verified: &mut BTreeSet<RecoveredEffectIdentity>,
) -> Result<(), InternalError> {
    let identity = RecoveredEffectIdentity::Row {
        entity_path: entity_path.to_string(),
        primary_key: primary_key.as_bytes().to_vec(),
    };
    if !verified.insert(identity) {
        return Ok(());
    }

    let hooks = recovery_runtime_hook_for_entity_path(db, entity_path)?;
    let (_, handle) = registry_store_handle_for_path(db, hooks.store_path)?;
    if handle.with_data(|store| store.contains(primary_key)) {
        return Err(InternalError::recovery_effect_verification_failed());
    }

    Ok(())
}

fn verify_recovered_schema_put<C: CanisterKind>(
    db: &Db<C>,
    store_path: &str,
    schema_snapshot_bytes: &[u8],
    verified: &mut BTreeSet<RecoveredEffectIdentity>,
) -> Result<(), InternalError> {
    let snapshot = decode_persisted_schema_snapshot(schema_snapshot_bytes)?;
    let hooks = db.runtime_hook_for_entity_path(snapshot.entity_path())?;
    if hooks.store_path != store_path {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    let identity = RecoveredEffectIdentity::Schema {
        store_path: store_path.to_string(),
        entity_tag: hooks.entity_tag.value(),
        schema_version: snapshot.version().get(),
    };
    if !verified.insert(identity) {
        return Ok(());
    }

    let (_, handle) = registry_store_handle_for_path(db, store_path)?;
    let persisted = handle
        .with_schema(|store| store.get_persisted_snapshot(hooks.entity_tag, snapshot.version()))?;
    if persisted.as_ref() != Some(&snapshot) {
        return Err(InternalError::recovery_effect_verification_failed());
    }

    Ok(())
}

fn verify_recovered_accepted_schema<C: CanisterKind>(
    db: &Db<C>,
    store_path: &str,
    schema_bundle_bytes: &[u8],
    schema_root_bytes: &[u8],
    verified: &mut BTreeSet<RecoveredEffectIdentity>,
) -> Result<(), InternalError> {
    let identity = RecoveredEffectIdentity::AcceptedSchema {
        store_path: store_path.to_string(),
    };
    if !verified.insert(identity) {
        return Ok(());
    }

    let candidate = CandidateSchemaRevision::from_encoded(
        schema_bundle_bytes.to_vec(),
        schema_root_bytes.to_vec(),
    )?;
    if candidate.store_path() != store_path {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    let (_, handle) = registry_store_handle_for_path(db, store_path)?;
    let accepted_matches = handle.with_schema(|store| -> Result<bool, InternalError> {
        let Some(root) = store.current_accepted_schema_root()? else {
            return Ok(false);
        };
        if root.root() != candidate.root() {
            return Ok(false);
        }
        Ok(store.current_accepted_schema_bundle()?.as_ref() == Some(candidate.bundle()))
    })?;
    if !accepted_matches {
        return Err(InternalError::recovery_effect_verification_failed());
    }

    Ok(())
}

fn verify_recovered_validation_job<C: CanisterKind>(
    db: &Db<C>,
    store_path: &str,
    entity_tag: EntityTag,
    constraint_id: ConstraintId,
    job_bytes: Option<&[u8]>,
    verified: &mut BTreeSet<RecoveredEffectIdentity>,
) -> Result<(), InternalError> {
    let identity = RecoveredEffectIdentity::ConstraintValidationJob {
        store_path: store_path.to_string(),
        entity_tag: entity_tag.value(),
        constraint_id: constraint_id.get(),
    };
    if !verified.insert(identity) {
        return Ok(());
    }

    let expected = job_bytes
        .map(decode_constraint_validation_job)
        .transpose()?;
    let (_, handle) = registry_store_handle_for_path(db, store_path)?;
    let actual =
        handle.with_schema(|store| store.constraint_validation_job(entity_tag, constraint_id))?;
    if actual != expected {
        return Err(InternalError::recovery_effect_verification_failed());
    }

    Ok(())
}
