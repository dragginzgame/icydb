//! Module: db::commit::recovery
//! Responsibility: publish and resumably fold marker-bound journal batches before operations.
//! Does not own: marker storage encoding, mutation planning, or query semantics.
//! Boundary: db entrypoints -> commit::recovery -> commit::store + journal fold (one-way).
//!
//! This module implements a **system recovery step** that restores global
//! database invariants by completing marker-owned work and folding derived
//! state forward before any new operation proceeds.
//!
//! Important semantic notes:
//! - Recovery runs in bounded durable pages at startup.
//! - Read and write paths both perform a cheap marker check and replay if needed.
//! - Reads must not proceed while a persisted partial commit marker is present.
//!
//! Invocation from read or mutation entrypoints is permitted only as an
//! unconditional invariant-restoration step. Recovery must not be
//! interleaved with read logic or mutation planning/apply phases.

use crate::db::index::IndexEntryValue;
#[cfg(any(test, feature = "migration"))]
use crate::db::index::IndexKey;
#[cfg(any(test, feature = "migration"))]
use crate::db::schema::{apply_schema_migration_record_op, verify_schema_migration_record_op};
use crate::{
    db::{
        Db,
        commit::{
            CommitMarker, CommitRowOp, CommitSchemaFingerprint, database_incarnation_id,
            marker::DatabaseControlOp,
            memory::{
                CommitMemoryAllocation, configure_commit_memory_id,
                current_commit_memory_allocation,
            },
            store::{
                commit_marker_may_be_present, commit_marker_present_fast,
                mark_commit_marker_verified_absent, with_commit_store,
            },
        },
        data::{
            AcceptedStructuralRowAuthority, DecodedDataStoreKey, RawDataStoreKey, RawRow,
            StructuralSlotReader,
        },
        database_format::ensure_database_format_admitted,
        integrity::{apply_mutation_progress_record_op, verify_mutation_progress_record_op},
        journal::{
            FoldRecordCursor, FoldWatermark, JournalBatch, JournalRecord, JournalSequence,
            JournalTailStore, journal_batch_encoded_len,
        },
        registry::{StoreHandle, StoreRecoveryCapability, StoreSchemaMetadataCapability},
        runtime_entity_catalog::AcceptedRuntimeEntity,
        schema::{
            AcceptedCatalogSnapshotSelection, AcceptedSchemaRevision, CandidateSchemaRevision,
            ConstraintId, IdentityAdvanceId, SchemaStore, accepted_commit_schema_fingerprint,
            apply_live_identity_range_checkpoint, apply_live_schema_checkpoint,
            apply_schema_application_record_op, decode_constraint_validation_job,
            decode_persisted_schema_snapshot, load_accepted_schema_snapshot,
            load_live_schema_checkpoint, verify_live_identity_range_checkpoint,
            verify_live_schema_checkpoint, verify_schema_application_record_op,
        },
    },
    error::{ErrorOrigin, InternalError},
    traits::CanisterKind,
    types::EntityTag,
};
use std::{cell::RefCell, collections::BTreeSet, thread::LocalKey};

thread_local! {
    // Generated stores use thread-local stable memory, so their recovered
    // authority must have the same ownership boundary.
    static RECOVERED_KEYS: RefCell<Vec<RecoveryDomainKey>> =
        const { RefCell::new(Vec::new()) };
    static RECOVERY_IN_PROGRESS_KEYS: RefCell<Vec<RecoveryDomainKey>> =
        const { RefCell::new(Vec::new()) };
}

#[cfg(any(test, feature = "migration"))]
fn validate_schema_migration_journal_plan(
    plan_digest: icydb_schema::SchemaMigrationPlanDigest,
) -> Result<(), InternalError> {
    let record = crate::db::schema::load_schema_migration_record()?
        .ok_or_else(InternalError::store_corruption)?;
    if !record.permits_private_physical_journal(plan_digest) {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeStoreDomainKey {
    store_registry: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecoveryDomainKey {
    commit_allocation: CommitMemoryAllocation,
    runtime_stores: RuntimeStoreDomainKey,
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
    match continue_recovery(db)? {
        RecoveryProgress::Complete => Ok(()),
        RecoveryProgress::Pending => Err(InternalError::recovery_pending()),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryProgress {
    Complete,
    Pending,
}

pub(crate) fn continue_recovery<C: CanisterKind>(
    db: &Db<C>,
) -> Result<RecoveryProgress, InternalError> {
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
        return Ok(RecoveryProgress::Complete);
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

    Ok(RecoveryProgress::Complete)
}

fn recover_domain<C: CanisterKind>(
    db: &Db<C>,
    recovery_key: RecoveryDomainKey,
) -> Result<RecoveryProgress, InternalError> {
    mark_recovery_domain_in_progress(recovery_key);
    let marker = with_commit_store(super::store::CommitStore::load)
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    let progress = if marker.is_none() && journaled_tails_are_empty(db) {
        restore_live_schema_checkpoints(db, None)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
        db.mark_all_registered_index_stores_ready()
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
        mark_commit_marker_verified_absent();
        RecoveryProgress::Complete
    } else {
        perform_recovery_page(db, marker)?
    };
    if progress == RecoveryProgress::Complete {
        mark_recovery_domain_recovered(recovery_key)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
        clear_recovery_domain_in_progress(recovery_key);
    }
    Ok(progress)
}

fn journaled_tails_are_empty<C: CanisterKind>(db: &Db<C>) -> bool {
    sorted_journaled_store_handles(db)
        .into_iter()
        .all(|(_, handle)| {
            handle.journal_tail_store().is_some_and(|tail| {
                tail.with_borrow(|store| {
                    !store.has_stored_batch() && !store.has_fold_record_cursor()
                })
            })
        })
}

fn perform_recovery_page<C: CanisterKind>(
    db: &Db<C>,
    marker: Option<CommitMarker>,
) -> Result<RecoveryProgress, InternalError> {
    let had_marker = marker.is_some();
    restore_live_schema_checkpoints(db, marker.as_ref())
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    if let Some(marker) = marker.as_ref() {
        apply_marker_live_schema_checkpoints(db, marker)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
        publish_marker_bound_journal_batches(db, marker)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
        for operation in marker.database_control() {
            match operation {
                DatabaseControlOp::SchemaApplication(operation) => {
                    apply_schema_application_record_op(operation)
                        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
                }
                #[cfg(any(test, feature = "migration"))]
                DatabaseControlOp::EntitySourceLineage(operation) => {
                    crate::db::schema::apply_entity_source_lineage_catalog_op(operation)
                        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
                }
                #[cfg(any(test, feature = "migration"))]
                DatabaseControlOp::SchemaMigration(operation) => {
                    apply_schema_migration_record_op(operation)
                        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
                }
                DatabaseControlOp::MutationProgress(operation) => {
                    apply_mutation_progress_record_op::<C>(operation)
                        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
                }
            }
        }
    }

    // Disposable overlays may contain effects from the predecessor Wasm or a
    // same-process interruption test. Canonical row, index, and schema stores
    // remain the only fold inputs across resumable recovery pages.
    reset_journaled_live_projections(db).map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;

    // Fold one bounded journal page. Each row transition updates its canonical
    // data and derived-index effects before the existing watermark advances.
    if !fold_journaled_tail_page(db).map_err(|err| err.with_origin(ErrorOrigin::Recovery))? {
        return Ok(RecoveryProgress::Pending);
    }

    // Verify only marker-owned effects and terminal fold state before
    // clearing marker authority. Whole-database integrity is an explicit
    // bounded inspection workflow, not a recovery side effect.
    verify_recovered_effects(db, marker.as_ref())
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;

    // Clear marker only after replay + fold + effect validation succeed.
    if had_marker {
        with_commit_store(super::store::CommitStore::clear_verified)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    }

    db.mark_all_registered_index_stores_ready()?;
    mark_commit_marker_verified_absent();

    Ok(RecoveryProgress::Complete)
}

fn restore_live_schema_checkpoints<C: CanisterKind>(
    db: &Db<C>,
    marker: Option<&CommitMarker>,
) -> Result<(), InternalError> {
    let incarnation = database_incarnation_id()?;
    db.with_store_registry(|registry| {
        for (store_path, handle) in registry.iter() {
            if handle.storage_capabilities().schema_metadata()
                != StoreSchemaMetadataCapability::LiveRebuiltMetadata
            {
                continue;
            }
            let checkpoint = load_live_schema_checkpoint(store_path)?;
            let current = handle.with_schema(SchemaStore::current_accepted_schema_root)?;
            match (current, checkpoint) {
                (None, Some(checkpoint)) => handle.with_schema_mut(|store| {
                    store.restore_live_accepted_schema_checkpoint(
                        incarnation,
                        checkpoint.candidate(),
                        checkpoint.identity_states(),
                    )
                })?,
                (Some(current), Some(checkpoint))
                    if current.root() == checkpoint.candidate().root() =>
                {
                    handle.with_schema_mut(|store| {
                        store.restore_live_accepted_schema_checkpoint(
                            incarnation,
                            checkpoint.candidate(),
                            checkpoint.identity_states(),
                        )
                    })?;
                }
                (Some(current), Some(checkpoint))
                    if marker.is_some_and(|marker| {
                        marker_advances_live_checkpoint(
                            marker,
                            store_path,
                            current.root().revision(),
                            checkpoint.candidate(),
                        )
                    }) => {}
                (Some(_), Some(_) | None) => {
                    return Err(InternalError::store_corruption());
                }
                (None, None) => {}
            }
        }
        Ok(())
    })
}

fn marker_advances_live_checkpoint(
    marker: &CommitMarker,
    store_path: &str,
    current_revision: AcceptedSchemaRevision,
    checkpoint: &CandidateSchemaRevision,
) -> bool {
    marker.journal_batches().iter().any(|batch| {
        let [
            JournalRecord::AcceptedSchemaPublish {
                store_path: record_store_path,
                expected_revision,
                schema_bundle_bytes,
                schema_root_bytes,
            },
        ] = batch.records()
        else {
            return false;
        };
        let store_matches = record_store_path == store_path;
        let revision_matches = *expected_revision == current_revision;
        let bundle_matches = schema_bundle_bytes.as_slice() == checkpoint.encoded_bundle();
        let root_matches = schema_root_bytes.as_slice() == checkpoint.encoded_root();
        store_matches && revision_matches && bundle_matches && root_matches
    })
}

fn apply_marker_live_schema_checkpoints<C: CanisterKind>(
    db: &Db<C>,
    marker: &CommitMarker,
) -> Result<(), InternalError> {
    let incarnation = database_incarnation_id()?;
    for batch in marker.journal_batches() {
        let (store_path, handle) = journal_batch_store_handle(db, batch)?;
        if handle.storage_capabilities().schema_metadata()
            != StoreSchemaMetadataCapability::LiveRebuiltMetadata
        {
            continue;
        }
        let Some(candidate) = journal_batch_schema_candidate(store_path, batch)? else {
            continue;
        };
        let expected_revision = match batch.records().first() {
            Some(JournalRecord::AcceptedSchemaPublish {
                expected_revision, ..
            }) => *expected_revision,
            _ => return Err(InternalError::store_corruption()),
        };
        apply_live_schema_checkpoint(incarnation, store_path, expected_revision, &candidate)?;
    }
    Ok(())
}

fn publish_marker_bound_journal_batches<C: CanisterKind>(
    db: &Db<C>,
    marker: &CommitMarker,
) -> Result<(), InternalError> {
    for batch in marker.journal_batches() {
        let (store_path, handle) = journal_batch_store_handle(db, batch)?;
        match (
            journal_batch_is_direct_schema_publication(batch),
            handle.journal_tail_store(),
        ) {
            (true, _) | (false, None) => replay_journal_batch(db, store_path, handle, batch)?,
            (false, Some(journal_store)) => {
                journal_store.with_borrow_mut(|store| {
                    store.append_batch(batch)?;

                    Ok::<(), InternalError>(())
                })?;
            }
        }
    }

    Ok(())
}

fn reset_journaled_live_projections<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    for (_, handle) in sorted_journaled_store_handles(db) {
        handle.mark_index_building()?;
        let data_generation = handle.with_data_mut(|store| {
            store.reset_journaled_live_projection()?;
            Ok::<_, InternalError>(store.generation())
        })?;
        handle.with_index_mut(|store| store.reset_journaled_live_projection(data_generation))?;
        handle.with_schema_mut(SchemaStore::reset_journaled_live_projection)?;
    }

    Ok(())
}

#[cfg(test)]
const RECOVERY_JOURNAL_BATCHES_PER_PAGE: usize = 128;
#[cfg(not(test))]
const RECOVERY_JOURNAL_BATCHES_PER_PAGE: usize = 2_048;
#[cfg(test)]
const RECOVERY_JOURNAL_RECORDS_PER_PAGE: usize = 128;
#[cfg(not(test))]
const RECOVERY_JOURNAL_RECORDS_PER_PAGE: usize = 4_096;
const RECOVERY_JOURNAL_BYTES_PER_PAGE: usize = 8 * 1_024 * 1_024;
#[cfg(target_arch = "wasm32")]
const RECOVERY_INSTRUCTIONS_PER_PAGE: u64 = 20_000_000_000;

fn fold_journaled_tail_page<C: CanisterKind>(db: &Db<C>) -> Result<bool, InternalError> {
    let mut remaining_batches = RECOVERY_JOURNAL_BATCHES_PER_PAGE;
    let mut remaining_records = RECOVERY_JOURNAL_RECORDS_PER_PAGE;
    let mut remaining_bytes = RECOVERY_JOURNAL_BYTES_PER_PAGE;
    let instruction_start = recovery_instruction_counter();
    let mut page_progressed = false;
    'stores: for (store_path, handle) in sorted_journaled_store_handles(db) {
        let journal_store = handle
            .journal_tail_store()
            .ok_or_else(InternalError::store_corruption)?;
        loop {
            if remaining_batches == 0 || remaining_records == 0 {
                break 'stores;
            }
            let watermark = journal_store.with_borrow(JournalTailStore::fold_watermark)?;
            let Some(batch) = journal_store.with_borrow(|store| {
                store.next_batch_after(watermark.highest_folded_journal_sequence())
            })?
            else {
                if journal_store.with_borrow(JournalTailStore::has_fold_record_cursor) {
                    return Err(InternalError::store_corruption());
                }
                break;
            };
            let batch_bytes = journal_batch_encoded_len(&batch);
            let stored_cursor = journal_store.with_borrow(JournalTailStore::fold_record_cursor)?;
            if stored_cursor.is_none()
                && page_progressed
                && (batch_bytes > remaining_bytes
                    || recovery_instruction_budget_reached(instruction_start))
            {
                break 'stores;
            }

            let (mut next_record_ordinal, cursor_created) = prepare_fold_record_cursor(
                db,
                store_path,
                handle,
                journal_store,
                &batch,
                stored_cursor,
            )?;
            page_progressed |= cursor_created;
            let candidate = validated_journal_batch_schema_candidate(store_path, &batch)?;
            while next_record_ordinal < batch.records().len() {
                if page_progressed
                    && (remaining_records == 0
                        || recovery_instruction_budget_reached(instruction_start))
                {
                    break 'stores;
                }
                let record = batch
                    .records()
                    .get(next_record_ordinal)
                    .ok_or_else(InternalError::store_corruption)?;
                validate_journal_batch_record(
                    db,
                    store_path,
                    handle,
                    candidate.as_ref(),
                    &batch,
                    next_record_ordinal,
                    record,
                    JournalRecordApplyMode::Fold,
                )?;
                apply_journal_record(
                    db,
                    store_path,
                    handle,
                    &batch,
                    next_record_ordinal,
                    record,
                    JournalRecordApplyMode::Fold,
                )?;
                next_record_ordinal = next_record_ordinal
                    .checked_add(1)
                    .ok_or_else(InternalError::store_corruption)?;
                persist_fold_record_cursor(journal_store, &batch, next_record_ordinal)?;
                remaining_records = remaining_records.saturating_sub(1);
                page_progressed = true;
            }

            let next_epoch = watermark
                .fold_epoch()
                .checked_add(1)
                .ok_or_else(InternalError::store_corruption)?;
            let next_watermark = FoldWatermark::new(batch.journal_sequence(), next_epoch);
            journal_store.with_borrow_mut(|store| {
                store.persist_fold_watermark(next_watermark)?;
                store.clear_fold_record_cursor();
                store.clear_batches_through(batch.journal_sequence());

                Ok::<(), InternalError>(())
            })?;
            remaining_batches = remaining_batches.saturating_sub(1);
            remaining_bytes = remaining_bytes.saturating_sub(batch_bytes);
        }
    }

    Ok(journaled_tails_are_empty(db))
}

fn prepare_fold_record_cursor<C: CanisterKind>(
    db: &Db<C>,
    store_path: &'static str,
    handle: StoreHandle,
    journal_store: &'static LocalKey<RefCell<JournalTailStore>>,
    batch: &JournalBatch,
    stored_cursor: Option<FoldRecordCursor>,
) -> Result<(usize, bool), InternalError> {
    if let Some(cursor) = stored_cursor {
        return validate_fold_record_cursor(batch, cursor).map(|ordinal| (ordinal, false));
    }

    validate_journal_batch_envelope(db, store_path, handle, batch)?;
    persist_fold_record_cursor(journal_store, batch, 0)?;
    Ok((0, true))
}

fn persist_fold_record_cursor(
    journal_store: &'static LocalKey<RefCell<JournalTailStore>>,
    batch: &JournalBatch,
    next_record_ordinal: usize,
) -> Result<(), InternalError> {
    let cursor = FoldRecordCursor::new(
        batch.journal_sequence(),
        batch.batch_id(),
        u32::try_from(batch.records().len()).map_err(|_| InternalError::store_corruption())?,
        u32::try_from(next_record_ordinal).map_err(|_| InternalError::store_corruption())?,
    );
    journal_store.with_borrow_mut(|store| store.persist_fold_record_cursor(cursor))
}

fn validate_fold_record_cursor(
    batch: &JournalBatch,
    cursor: FoldRecordCursor,
) -> Result<usize, InternalError> {
    let record_count =
        u32::try_from(batch.records().len()).map_err(|_| InternalError::store_corruption())?;
    if cursor.journal_sequence() != batch.journal_sequence()
        || cursor.batch_id() != batch.batch_id()
        || cursor.record_count() != record_count
        || cursor.next_record_ordinal() > record_count
    {
        return Err(InternalError::store_corruption());
    }
    usize::try_from(cursor.next_record_ordinal()).map_err(|_| InternalError::store_corruption())
}

#[cfg(target_arch = "wasm32")]
fn recovery_instruction_counter() -> u64 {
    ic_cdk::api::performance_counter(0)
}

#[cfg(not(target_arch = "wasm32"))]
const fn recovery_instruction_counter() -> u64 {
    0
}

#[cfg(target_arch = "wasm32")]
fn recovery_instruction_budget_reached(start: u64) -> bool {
    recovery_instruction_counter().saturating_sub(start) >= RECOVERY_INSTRUCTIONS_PER_PAGE
}

#[cfg(not(target_arch = "wasm32"))]
const fn recovery_instruction_budget_reached(_start: u64) -> bool {
    false
}

fn sorted_journaled_store_handles<C: CanisterKind>(db: &Db<C>) -> Vec<(&'static str, StoreHandle)> {
    let mut stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    stores.retain(|(_, handle)| {
        handle.storage_capabilities().recovery()
            == StoreRecoveryCapability::StableBasePlusJournalReplay
    });
    stores.sort_unstable_by_key(|(path, _)| *path);
    stores
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalRecordApplyMode {
    Replay,
    Fold,
}

fn identity_advance_id(
    batch: &JournalBatch,
    record_ordinal: usize,
) -> Result<IdentityAdvanceId, InternalError> {
    IdentityAdvanceId::try_new(
        batch.commit_marker_id(),
        batch.batch_id(),
        batch.journal_sequence().get(),
        u32::try_from(record_ordinal).map_err(|_| InternalError::store_corruption())?,
    )
    .map_err(|_| InternalError::store_corruption())
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

    for (record_ordinal, record) in batch.records().iter().enumerate() {
        apply_journal_record(
            db,
            expected_store_path,
            expected_handle,
            batch,
            record_ordinal,
            record,
            JournalRecordApplyMode::Replay,
        )?;
    }

    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "recovery keeps every journal record's replay and fold behavior in one exhaustive authority"
)]
fn apply_journal_record<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
    record_ordinal: usize,
    record: &JournalRecord,
    mode: JournalRecordApplyMode,
) -> Result<(), InternalError> {
    match record {
        JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            schema_fingerprint,
        } => {
            let row =
                RawRow::from_untrusted_bytes(row_bytes.clone()).map_err(InternalError::from)?;
            match mode {
                JournalRecordApplyMode::Replay => expected_handle.with_data_mut(|store| {
                    store
                        .apply_recovered_journal_put(primary_key.clone(), row)
                        .map(|_| ())
                }),
                JournalRecordApplyMode::Fold => fold_recovered_row_transition(
                    db,
                    expected_handle,
                    entity_path,
                    primary_key,
                    Some(row),
                    *schema_fingerprint,
                ),
            }
        }
        JournalRecord::RowDelete {
            entity_path,
            primary_key,
            schema_fingerprint,
        } => match mode {
            JournalRecordApplyMode::Replay => expected_handle.with_data_mut(|store| {
                store
                    .apply_recovered_journal_delete(primary_key)
                    .map(|_| ())
            }),
            JournalRecordApplyMode::Fold => fold_recovered_row_transition(
                db,
                expected_handle,
                entity_path,
                primary_key,
                None,
                *schema_fingerprint,
            ),
        },
        JournalRecord::SchemaPut {
            store_path,
            schema_snapshot_bytes,
        } => {
            if store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            let snapshot = decode_persisted_schema_snapshot(schema_snapshot_bytes)?;
            let runtime_entity = db.accepted_runtime_entity_for_path(snapshot.entity_path())?;
            if runtime_entity.store_path() != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            expected_handle.with_schema_mut(|schema_store| match mode {
                JournalRecordApplyMode::Replay => {
                    schema_store.insert_persisted_snapshot(runtime_entity.entity_tag(), &snapshot)
                }
                JournalRecordApplyMode::Fold => {
                    schema_store.fold_persisted_snapshot(runtime_entity.entity_tag(), &snapshot)
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
            let incarnation = database_incarnation_id()?;
            expected_handle.with_schema_mut(|schema_store| {
                match (
                    *expected_revision,
                    expected_handle.storage_capabilities().recovery(),
                    mode,
                ) {
                    (AcceptedSchemaRevision::NONE, _, JournalRecordApplyMode::Replay)
                    | (_, StoreRecoveryCapability::None, JournalRecordApplyMode::Replay) => {
                        schema_store.publish_accepted_schema_candidate(
                            incarnation,
                            *expected_revision,
                            &candidate,
                        )
                    }
                    (
                        _,
                        StoreRecoveryCapability::StableBasePlusJournalReplay,
                        JournalRecordApplyMode::Replay,
                    ) => schema_store.apply_journaled_accepted_schema_candidate(
                        incarnation,
                        *expected_revision,
                        &candidate,
                    ),
                    (AcceptedSchemaRevision::NONE, _, JournalRecordApplyMode::Fold)
                    | (_, StoreRecoveryCapability::None, JournalRecordApplyMode::Fold) => {
                        Err(InternalError::store_corruption())
                    }
                    (
                        _,
                        StoreRecoveryCapability::StableBasePlusJournalReplay,
                        JournalRecordApplyMode::Fold,
                    ) => schema_store.fold_journaled_accepted_schema_candidate(
                        incarnation,
                        *expected_revision,
                        &candidate,
                    ),
                }
            })
        }
        JournalRecord::ConstraintValidationJobPut {
            entity_tag,
            constraint_id,
            job_bytes,
            ..
        } => {
            // Batch preflight already proves the record identity and final
            // activation/job closure. Re-resolving accepted authority here
            // would observe the intentional intermediate state after the
            // preceding schema record but before this paired job record.
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
            entity_tag,
            constraint_id,
            ..
        } => {
            // The batch-level closure proof owns identity validation; applying
            // the paired removal must not inspect the intermediate schema/job
            // state between records.
            expected_handle.with_schema_mut(|schema_store| match mode {
                JournalRecordApplyMode::Replay => schema_store
                    .apply_constraint_validation_job_removal(*entity_tag, *constraint_id),
                JournalRecordApplyMode::Fold => {
                    schema_store.fold_constraint_validation_job_removal(*entity_tag, *constraint_id)
                }
            })
        }
        JournalRecord::ConstraintValidationIndexPut { key, .. } => {
            expected_handle.with_index_mut(|store| match mode {
                JournalRecordApplyMode::Replay => {
                    store.insert(key.clone(), IndexEntryValue::presence());
                    Ok(())
                }
                JournalRecordApplyMode::Fold => store
                    .fold_recovered_journal_entry(key.clone(), Some(IndexEntryValue::presence())),
            })
        }
        JournalRecord::IdentityRangeAdvance { range } => {
            let advance_id = identity_advance_id(batch, record_ordinal)?;
            match mode {
                JournalRecordApplyMode::Replay => {
                    if expected_handle.storage_capabilities().schema_metadata()
                        == StoreSchemaMetadataCapability::LiveRebuiltMetadata
                    {
                        apply_live_identity_range_checkpoint(
                            expected_store_path,
                            *range,
                            advance_id,
                        )?;
                    }
                    expected_handle.with_schema_mut(|schema_store| {
                        schema_store.apply_identity_range_advance(*range, advance_id)
                    })
                }
                JournalRecordApplyMode::Fold => expected_handle.with_schema_mut(|schema_store| {
                    schema_store.fold_identity_range_advance(*range, advance_id)
                }),
            }
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut {
            store_path,
            primary_key,
            row_bytes,
            plan_digest,
            ..
        } => {
            if store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            validate_schema_migration_journal_plan(*plan_digest)?;
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
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationIndexPut {
            store_path,
            key,
            plan_digest,
        } => {
            if store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            validate_schema_migration_journal_plan(*plan_digest)?;
            expected_handle.with_index_mut(|store| match mode {
                JournalRecordApplyMode::Replay => match store.get(key) {
                    None => {
                        store.insert(key.clone(), IndexEntryValue::presence());
                        Ok(())
                    }
                    Some(value) if value == IndexEntryValue::presence() => Ok(()),
                    Some(_) => Err(InternalError::store_corruption()),
                },
                JournalRecordApplyMode::Fold => store
                    .fold_recovered_journal_entry(key.clone(), Some(IndexEntryValue::presence())),
            })
        }
    }
}

fn fold_recovered_row_transition<C: CanisterKind>(
    db: &Db<C>,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    after: Option<RawRow>,
    schema_fingerprint: CommitSchemaFingerprint,
) -> Result<(), InternalError> {
    let before = expected_handle
        .with_data(|store| store.get(primary_key).map(|row| row.as_bytes().to_vec()));
    let op = CommitRowOp::try_new_bytes(
        entity_path,
        primary_key.as_bytes(),
        before,
        after.as_ref().map(|row| row.as_bytes().to_vec()),
        schema_fingerprint,
    )?;
    let prepared = db.prepare_row_commit_op_for_replay(&op)?;
    if !std::ptr::eq(prepared.data_store, expected_handle.data_store())
        || prepared.data_key != *primary_key
    {
        return Err(InternalError::store_corruption());
    }
    for index_op in prepared.index_ops {
        index_op.fold_recovered()?;
    }

    let data_generation = expected_handle.with_data_mut(|store| match after {
        Some(row) => store
            .fold_recovered_journal_put(primary_key.clone(), row)
            .map(|_| store.generation()),
        None => store
            .fold_recovered_journal_delete(primary_key)
            .map(|_| store.generation()),
    })?;
    expected_handle.with_index_mut(|store| {
        store.mark_prefix_cardinality_data_generation(data_generation);
    });
    Ok(())
}

fn validate_journal_batch_records<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
    mode: JournalRecordApplyMode,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    let candidate =
        validate_journal_batch_envelope(db, expected_store_path, expected_handle, batch)?;

    for (record_ordinal, record) in batch.records().iter().enumerate() {
        validate_journal_batch_record(
            db,
            expected_store_path,
            expected_handle,
            candidate.as_ref(),
            batch,
            record_ordinal,
            record,
            mode,
        )?;
    }

    Ok(candidate)
}

fn validate_journal_batch_envelope<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    let (_, batch_handle) = journal_batch_store_handle(db, batch)?;
    if !std::ptr::eq(batch_handle.data_store(), expected_handle.data_store()) {
        return Err(InternalError::store_corruption());
    }
    let candidate = journal_batch_schema_candidate(expected_store_path, batch)?;
    validate_journal_batch_constraint_validation_job_change(
        db,
        expected_store_path,
        expected_handle,
        batch,
        candidate.as_ref(),
    )?;

    Ok(candidate)
}

fn validated_journal_batch_schema_candidate(
    expected_store_path: &'static str,
    batch: &JournalBatch,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    journal_batch_schema_candidate(expected_store_path, batch)
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the exhaustive current journal record validator keeps store routing and apply-mode closure in one match"
)]
fn validate_journal_batch_record<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    candidate: Option<&CandidateSchemaRevision>,
    batch: &JournalBatch,
    record_ordinal: usize,
    record: &JournalRecord,
    mode: JournalRecordApplyMode,
) -> Result<(), InternalError> {
    match record {
        JournalRecord::RowPut { .. } => {
            validate_journal_batch_row_put(
                db,
                expected_store_path,
                expected_handle,
                candidate,
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
            let runtime_entity = db.accepted_runtime_entity_for_path(snapshot.entity_path())?;
            if runtime_entity.store_path() != expected_store_path {
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
        JournalRecord::ConstraintValidationIndexPut {
            store_path,
            entity_tag,
            constraint_id,
            key,
        } => {
            if store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            let Some(JournalRecord::ConstraintValidationJobPut { job_bytes, .. }) =
                batch.records().first()
            else {
                return Err(InternalError::store_corruption());
            };
            let job = decode_constraint_validation_job(job_bytes)?;
            if job.entity_tag() != *entity_tag || job.constraint_id() != *constraint_id {
                return Err(InternalError::store_corruption());
            }
            let bundle = expected_handle
                .with_schema(SchemaStore::current_accepted_schema_bundle)?
                .ok_or_else(InternalError::store_corruption)?;
            super::schema_publication::validate_candidate_index_entries(
                &bundle,
                &job,
                std::slice::from_ref(key),
            )?;
        }
        JournalRecord::IdentityRangeAdvance { range } => {
            let runtime_entity = db
                .accepted_runtime_entity_for_tag(range.owner().entity_tag())
                .map_err(|_| InternalError::store_corruption())?;
            if runtime_entity.store_path() != expected_store_path
                || range.owner().database_incarnation_id() != database_incarnation_id()?
            {
                return Err(InternalError::store_corruption());
            }
            let advance_id = identity_advance_id(batch, record_ordinal)?;
            let commit_state = expected_handle.with_schema(|store| {
                store.identity_range_commit_state(
                    *range,
                    advance_id,
                    mode == JournalRecordApplyMode::Fold,
                )
            })?;
            if commit_state.materialized_high_water() > commit_state.committed_high_water() {
                return Err(InternalError::identity_state_corruption());
            }
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut {
            store_path,
            primary_key,
            row_bytes,
            plan_digest,
            ..
        } => {
            if store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            validate_schema_migration_journal_plan(*plan_digest)?;
            DecodedDataStoreKey::try_from_raw(primary_key)
                .map_err(|_| InternalError::store_corruption())?;
            RawRow::from_untrusted_bytes(row_bytes.clone()).map_err(InternalError::from)?;
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationIndexPut {
            store_path,
            key,
            plan_digest,
        } => {
            if store_path != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            validate_schema_migration_journal_plan(*plan_digest)?;
            IndexKey::try_from_raw(key).map_err(|_| InternalError::store_corruption())?;
        }
    }
    Ok(())
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

fn journal_batch_schema_candidate(
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
                    let source = icydb_schema::EntitySourceKey::try_new(snapshot.entity_path())
                        .map_err(|_| InternalError::store_corruption())?;
                    if decoded.bundle().source_bindings().entity(&source) != Some(*entity_tag) {
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
            | JournalRecord::ConstraintValidationJobDelete { .. }
            | JournalRecord::ConstraintValidationIndexPut { .. }
            | JournalRecord::IdentityRangeAdvance { .. } => {}
            #[cfg(any(test, feature = "migration"))]
            JournalRecord::SchemaMigrationRowPut { .. }
            | JournalRecord::SchemaMigrationIndexPut { .. } => {}
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
    let runtime_entity = db
        .accepted_runtime_entity_for_tag(entity_tag)
        .map_err(|_| InternalError::store_corruption())?;
    if runtime_entity.store_path() != expected_store_path {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

fn validate_candidate_journal_row_put(
    expected_store_path: &'static str,
    candidate: &CandidateSchemaRevision,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    row_bytes: &[u8],
    schema_fingerprint: [u8; 16],
) -> Result<(), InternalError> {
    let decoded_key = DecodedDataStoreKey::try_from_raw(primary_key)
        .map_err(|_| InternalError::store_corruption())?;
    let runtime_entity = crate::db::runtime_entity_catalog::candidate_runtime_entity_for_path(
        candidate,
        expected_store_path,
        entity_path,
    )?;
    if runtime_entity.store_path() != expected_store_path
        || decoded_key.entity_tag() != runtime_entity.entity_tag()
    {
        return Err(InternalError::store_corruption());
    }
    let selection = crate::db::schema::AcceptedCatalogSnapshotSelection::from_candidate(
        candidate,
        runtime_entity.entity_tag(),
        runtime_entity.entity_path(),
        runtime_entity.store_path(),
    )?
    .ok_or_else(InternalError::store_corruption)?;
    if selection.identity().accepted_schema_fingerprint() != schema_fingerprint {
        return Err(InternalError::store_corruption());
    }
    let row = RawRow::from_untrusted_bytes(row_bytes.to_vec()).map_err(InternalError::from)?;
    let contract = AcceptedStructuralRowAuthority::from_catalog_selection(
        runtime_entity.entity_path(),
        &selection,
    )?
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
    let runtime_entity = recovery_accepted_runtime_entity_for_path(db, entity_path)?;
    if runtime_entity.store_path() != expected_store_path
        || decoded_key.entity_tag() != runtime_entity.entity_tag()
    {
        return Err(InternalError::store_corruption());
    }
    let selection = expected_handle
        .with_schema(|schema_store| {
            schema_store.current_canonical_accepted_catalog_selection(
                runtime_entity.entity_tag(),
                runtime_entity.entity_path(),
                runtime_entity.store_path(),
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
    let runtime_entity = recovery_accepted_runtime_entity_for_path(db, entity_path)?;
    if runtime_entity.store_path() != expected_store_path
        || decoded_key.entity_tag() != runtime_entity.entity_tag()
    {
        return Err(InternalError::store_corruption());
    }
    let accepted = expected_handle.with_schema(|schema_store| {
        load_accepted_schema_snapshot(
            schema_store,
            runtime_entity.entity_tag(),
            runtime_entity.entity_path(),
        )
    })?;
    let expected_fingerprint = accepted_commit_schema_fingerprint(&accepted)?;
    if &expected_fingerprint != schema_fingerprint {
        return Err(InternalError::store_corruption());
    }

    Ok(())
}

// Accepted-entity recovery can validate unapplied journal row effects through
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

    let runtime_entity = recovery_accepted_runtime_entity_for_path(db, entity_path)?;
    let before = expected_handle
        .with_data(|store| store.get(primary_key).map(|row| row.as_bytes().to_vec()));
    let op = CommitRowOp::try_new_bytes(
        runtime_entity.entity_path(),
        primary_key.as_bytes(),
        before,
        Some(row_bytes.to_vec()),
        schema_fingerprint,
    )?;
    db.prepare_row_commit_op_for_replay(&op)?;

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

    let runtime_entity = recovery_accepted_runtime_entity_for_path(db, entity_path)?;
    let before = expected_handle
        .with_data(|store| store.get(primary_key).map(|row| row.as_bytes().to_vec()));
    let op = CommitRowOp::try_new_bytes(
        runtime_entity.entity_path(),
        primary_key.as_bytes(),
        before,
        None,
        schema_fingerprint,
    )?;
    db.prepare_row_commit_op_for_replay(&op)?;

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
    if handle.storage_capabilities().recovery() == StoreRecoveryCapability::None
        && !journal_batch_is_direct_schema_publication(batch)
        && !journal_batch_is_direct_identity_commit(batch)
    {
        return Err(InternalError::store_corruption());
    }

    Ok((path, handle))
}

fn journal_batch_is_direct_schema_publication(batch: &JournalBatch) -> bool {
    batch.journal_sequence() == JournalSequence::new(0)
        && matches!(
            batch.records(),
            [JournalRecord::AcceptedSchemaPublish { .. }]
        )
}

fn journal_batch_is_direct_identity_commit(batch: &JournalBatch) -> bool {
    batch.journal_sequence() == JournalSequence::new(0)
        && batch
            .records()
            .iter()
            .any(|record| matches!(record, JournalRecord::IdentityRangeAdvance { .. }))
        && batch.records().iter().all(|record| {
            matches!(
                record,
                JournalRecord::RowPut { .. } | JournalRecord::IdentityRangeAdvance { .. }
            )
        })
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
        | JournalRecord::ConstraintValidationJobDelete { store_path, .. }
        | JournalRecord::ConstraintValidationIndexPut { store_path, .. } => {
            registry_store_handle_for_path(db, store_path)
        }
        JournalRecord::IdentityRangeAdvance { range } => {
            let runtime_entity = db
                .accepted_runtime_entity_for_tag(range.owner().entity_tag())
                .map_err(|_| InternalError::store_corruption())?;
            registry_store_handle_for_path(db, runtime_entity.store_path())
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut { store_path, .. }
        | JournalRecord::SchemaMigrationIndexPut { store_path, .. } => {
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
    let runtime_entity = recovery_accepted_runtime_entity_for_path(db, entity_path)?;
    registry_store_handle_for_path(db, runtime_entity.store_path())
}

fn recovery_accepted_runtime_entity_for_path<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &str,
) -> Result<AcceptedRuntimeEntity, InternalError> {
    db.accepted_runtime_entity_for_path(entity_path)
        .map_err(|_| InternalError::store_corruption())
}

fn runtime_store_domain_key<C: CanisterKind>(db: &Db<C>) -> RuntimeStoreDomainKey {
    RuntimeStoreDomainKey {
        store_registry: std::ptr::from_ref(db.store).cast::<()>() as usize,
    }
}

fn recovery_domain_key<C: CanisterKind>(db: &Db<C>) -> Result<RecoveryDomainKey, InternalError> {
    Ok(RecoveryDomainKey {
        commit_allocation: current_commit_memory_allocation()?,
        runtime_stores: runtime_store_domain_key(db),
    })
}

fn recovery_domain_recovered(key: RecoveryDomainKey) -> Result<bool, InternalError> {
    RECOVERED_KEYS.with(|keys| {
        Ok(keys
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?
            .contains(&key))
    })
}

fn recovery_domain_in_progress(key: RecoveryDomainKey) -> bool {
    RECOVERY_IN_PROGRESS_KEYS.with(|keys| keys.borrow().contains(&key))
}

pub(in crate::db) fn startup_recovery_witness(
    stores: &'static std::thread::LocalKey<crate::db::registry::StoreRegistry>,
) -> Result<(bool, bool), InternalError> {
    let key = RecoveryDomainKey {
        commit_allocation: current_commit_memory_allocation()?,
        runtime_stores: RuntimeStoreDomainKey {
            store_registry: std::ptr::from_ref(stores).cast::<()>() as usize,
        },
    };
    Ok((
        recovery_domain_recovered(key)?,
        recovery_domain_in_progress(key),
    ))
}

#[cfg(test)]
pub(in crate::db) fn mark_startup_recovery_complete_for_tests(
    stores: &'static std::thread::LocalKey<crate::db::registry::StoreRegistry>,
) -> Result<(), InternalError> {
    let key = RecoveryDomainKey {
        commit_allocation: current_commit_memory_allocation()?,
        runtime_stores: RuntimeStoreDomainKey {
            store_registry: std::ptr::from_ref(stores).cast::<()>() as usize,
        },
    };
    mark_recovery_domain_recovered(key)
}

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

fn mark_recovery_domain_in_progress(key: RecoveryDomainKey) {
    RECOVERY_IN_PROGRESS_KEYS.with(|keys| {
        let mut keys = keys.borrow_mut();
        if !keys.contains(&key) {
            keys.push(key);
        }
    });
}

fn clear_recovery_domain_in_progress(key: RecoveryDomainKey) {
    RECOVERY_IN_PROGRESS_KEYS.with(|keys| {
        keys.borrow_mut().retain(|existing| *existing != key);
    });
}

#[cfg(test)]
pub(in crate::db) fn forget_recovered_domain_for_tests<C: CanisterKind>(
    db: &Db<C>,
) -> Result<(), InternalError> {
    let key = recovery_domain_key(db)?;
    RECOVERED_KEYS.with(|keys| {
        keys.try_borrow_mut()
            .map_err(|_| InternalError::store_invariant())?
            .retain(|existing| *existing != key);
        Ok(())
    })
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
    IdentityRange {
        store_path: String,
        entity_tag: u64,
        field_id: u32,
    },
    Index {
        store_path: String,
        raw_key: Vec<u8>,
    },
}

// Verify the bounded final effect set owned by the recovered marker.
//
// The marker is capped by `MAX_COMMIT_BYTES`; reverse traversal retains only
// the last record for each logical target. Tail folds and full derived-state
// rebuilds have already completed, so row deletes need only prove authoritative
// row absence: incremental row folding applies the matching derived-index
// transition before the row watermark advances.
pub(in crate::db::commit) fn verify_recovered_effects<C: CanisterKind>(
    db: &Db<C>,
    marker: Option<&CommitMarker>,
) -> Result<(), InternalError> {
    let mut verified = BTreeSet::new();

    if let Some(marker) = marker {
        for operation in marker.database_control() {
            match operation {
                DatabaseControlOp::SchemaApplication(operation) => {
                    verify_schema_application_record_op(operation)?;
                }
                #[cfg(any(test, feature = "migration"))]
                DatabaseControlOp::EntitySourceLineage(operation) => {
                    crate::db::schema::verify_entity_source_lineage_catalog_op(operation)?;
                }
                #[cfg(any(test, feature = "migration"))]
                DatabaseControlOp::SchemaMigration(operation) => {
                    verify_schema_migration_record_op(operation)?;
                }
                DatabaseControlOp::MutationProgress(operation) => {
                    verify_mutation_progress_record_op::<C>(operation)?;
                }
            }
        }
        for batch in marker.journal_batches().iter().rev() {
            let (_, handle) = journal_batch_store_handle(db, batch)?;
            if !journal_batch_is_direct_schema_publication(batch)
                && let Some(journal_store) = handle.journal_tail_store()
            {
                let watermark = journal_store.with_borrow(JournalTailStore::fold_watermark)?;
                if watermark.highest_folded_journal_sequence() < batch.journal_sequence() {
                    return Err(InternalError::recovery_effect_verification_failed());
                }
            }

            for (record_ordinal, record) in batch.records().iter().enumerate().rev() {
                verify_recovered_record(db, batch, record_ordinal, record, &mut verified)?;
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

#[expect(
    clippy::too_many_lines,
    reason = "the exhaustive marker-effect verifier binds every current journal variant to its exact durable postcondition"
)]
fn verify_recovered_record<C: CanisterKind>(
    db: &Db<C>,
    batch: &JournalBatch,
    record_ordinal: usize,
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
        JournalRecord::ConstraintValidationIndexPut {
            store_path, key, ..
        } => verify_recovered_index_put(db, store_path, key, verified)?,
        JournalRecord::IdentityRangeAdvance { range } => {
            let runtime_entity = db
                .accepted_runtime_entity_for_tag(range.owner().entity_tag())
                .map_err(|_| InternalError::recovery_effect_verification_failed())?;
            let identity = RecoveredEffectIdentity::IdentityRange {
                store_path: runtime_entity.store_path().to_string(),
                entity_tag: range.owner().entity_tag().value(),
                field_id: range.owner().field_id().get(),
            };
            if verified.insert(identity) {
                let (_, handle) = registry_store_handle_for_path(db, runtime_entity.store_path())?;
                let advance_id = identity_advance_id(batch, record_ordinal)?;
                handle
                    .with_schema(|store| store.verify_identity_range_advance(*range, advance_id))?;
                if handle.storage_capabilities().schema_metadata()
                    == StoreSchemaMetadataCapability::LiveRebuiltMetadata
                {
                    verify_live_identity_range_checkpoint(
                        runtime_entity.store_path(),
                        *range,
                        advance_id,
                    )?;
                }
            }
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut {
            store_path,
            primary_key,
            row_bytes,
            plan_digest,
            ..
        } => {
            validate_schema_migration_journal_plan(*plan_digest)?;
            let identity = RecoveredEffectIdentity::Row {
                entity_path: store_path.clone(),
                primary_key: primary_key.as_bytes().to_vec(),
            };
            if verified.insert(identity) {
                let (_, handle) = registry_store_handle_for_path(db, store_path)?;
                let matches = handle
                    .with_data(|store| store.get(primary_key))
                    .is_some_and(|row| row.as_bytes() == row_bytes);
                if !matches {
                    return Err(InternalError::recovery_effect_verification_failed());
                }
            }
        }
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationIndexPut {
            store_path,
            key,
            plan_digest,
        } => {
            validate_schema_migration_journal_plan(*plan_digest)?;
            verify_recovered_index_put(db, store_path, key, verified)?;
        }
    }

    Ok(())
}

fn verify_recovered_index_put<C: CanisterKind>(
    db: &Db<C>,
    store_path: &str,
    key: &crate::db::index::RawIndexStoreKey,
    verified: &mut BTreeSet<RecoveredEffectIdentity>,
) -> Result<(), InternalError> {
    let identity = RecoveredEffectIdentity::Index {
        store_path: store_path.to_string(),
        raw_key: key.as_bytes().to_vec(),
    };
    if verified.insert(identity) {
        let (_, handle) = registry_store_handle_for_path(db, store_path)?;
        if handle.with_index(|store| store.get(key)) != Some(IndexEntryValue::presence()) {
            return Err(InternalError::recovery_effect_verification_failed());
        }
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

    let runtime_entity = recovery_accepted_runtime_entity_for_path(db, entity_path)?;
    let (_, handle) = registry_store_handle_for_path(db, runtime_entity.store_path())?;
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

    let runtime_entity = recovery_accepted_runtime_entity_for_path(db, entity_path)?;
    let (_, handle) = registry_store_handle_for_path(db, runtime_entity.store_path())?;
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
    let runtime_entity = db.accepted_runtime_entity_for_path(snapshot.entity_path())?;
    if runtime_entity.store_path() != store_path {
        return Err(InternalError::recovery_effect_verification_failed());
    }
    let identity = RecoveredEffectIdentity::Schema {
        store_path: store_path.to_string(),
        entity_tag: runtime_entity.entity_tag().value(),
        schema_version: snapshot.version().get(),
    };
    if !verified.insert(identity) {
        return Ok(());
    }

    let (_, handle) = registry_store_handle_for_path(db, store_path)?;
    let persisted = handle.with_schema(|store| {
        store.get_persisted_snapshot(runtime_entity.entity_tag(), snapshot.version())
    })?;
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
    if handle.storage_capabilities().schema_metadata()
        == StoreSchemaMetadataCapability::LiveRebuiltMetadata
    {
        verify_live_schema_checkpoint(store_path, &candidate)?;
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
