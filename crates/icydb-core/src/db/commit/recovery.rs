//! Module: db::commit::recovery
//! Responsibility: publish and atomically fold complete marker-bound journal batches from startup.
//! Does not own: marker storage encoding, mutation planning, or query semantics.
//! Boundary: startup driver -> commit::recovery -> commit::store + journal fold (one-way).
//!
//! This module implements a **system recovery step** that restores global
//! database invariants by completing marker-owned work and folding derived
//! state forward before any new operation proceeds.
//!
//! Important semantic notes:
//! - Recovery runs in bounded pages of complete durable batches at startup.
//! - Read and write paths perform state-only admission and never replay.
//! - Reads must not proceed while a persisted partial commit marker is present.

use crate::db::index::{
    IndexEntryExistenceWitness, IndexEntryValue, IndexKey, IndexKeyKind, IndexStore,
};
#[cfg(any(test, feature = "migration"))]
use crate::db::schema::{apply_schema_migration_record_op, verify_schema_migration_record_op};
use crate::{
    db::{
        Db,
        commit::{
            CommitMarker, CommitRowOp, CommitSchemaFingerprint, PreparedRowCommitOp,
            database_incarnation_id,
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
            AcceptedStructuralRowAuthority, DataStore, DecodedDataStoreKey,
            PreparedDataPositionRetirement, RawDataStoreKey, RawRow, StructuralSlotReader,
        },
        database_format::ensure_database_format_admitted,
        integrity::{apply_mutation_progress_record_op, verify_mutation_progress_record_op},
        journal::{
            DatabaseCommitSequence, FoldWatermark, JournalBatch, JournalRecord, JournalSequence,
            JournalTailStore,
        },
        positioned_overlay::{
            JournalOverlayPosition, classify_derived_index_overlay, classify_journal_overlay,
        },
        registry::{StoreHandle, StoreRecoveryCapability, StoreSchemaMetadataCapability},
        runtime_entity_catalog::AcceptedRuntimeEntity,
        schema::{
            AcceptedCatalogSnapshotSelection, AcceptedSchemaRevision, CandidateSchemaRevision,
            ConstraintId, IdentityAdvanceId, PreparedCardinalityMaintenance,
            PreparedSchemaPositionRetirement, SchemaStore, accepted_commit_schema_fingerprint,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
            apply_live_identity_range_checkpoint, apply_live_schema_checkpoint,
            apply_schema_application_record_op,
            cardinality_build::CardinalityBuildAuthority,
            cardinality_generation::{CardinalityCountDigest, CardinalityGenerationState},
            decode_constraint_validation_job, decode_persisted_schema_snapshot,
            load_accepted_schema_snapshot, load_live_schema_checkpoint,
            verify_live_identity_range_checkpoint, verify_live_schema_checkpoint,
            verify_schema_application_record_op,
        },
    },
    error::{ErrorOrigin, InternalError},
    traits::CanisterKind,
    types::EntityTag,
};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    thread::LocalKey,
};

use crate::db::index::PreparedIndexPositionRetirement;

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

/// Admit ordinary work only after the dedicated startup driver has completed.
///
/// This is a state-only defense below generated `db!()` admission. It never
/// decodes a journal batch, advances a recovery page, or mutates stable state.
pub(crate) fn ensure_recovery_admitted<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    configure_commit_memory_id(C::COMMIT_MEMORY_ID, C::COMMIT_STABLE_KEY)
        .map_err(|error| error.with_origin(ErrorOrigin::Recovery))?;
    let recovery_key =
        recovery_domain_key(db).map_err(|error| error.with_origin(ErrorOrigin::Recovery))?;
    let recovered = recovery_domain_recovered(recovery_key)
        .map_err(|error| error.with_origin(ErrorOrigin::Recovery))?;
    if !recovered || recovery_domain_in_progress(recovery_key) {
        return Err(InternalError::recovery_pending());
    }
    if commit_marker_may_be_present()
        && commit_marker_present_fast().map_err(|error| error.with_origin(ErrorOrigin::Recovery))?
    {
        return Err(InternalError::recovery_pending());
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryProgress {
    Complete,
    Pending,
}

/// Persisted authority whose current bytes caused one startup recovery page
/// to fail deterministically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum StartupRecoveryFailureAuthority {
    DatabaseControl,
    JournalStore(&'static str),
}

/// One recovery-page failure paired with the durable authority needed for a
/// binding-checked startup receipt.
pub(in crate::db) struct StartupRecoveryFailure {
    authority: StartupRecoveryFailureAuthority,
    error: InternalError,
}

impl StartupRecoveryFailure {
    const fn database_control(error: InternalError) -> Self {
        Self {
            authority: StartupRecoveryFailureAuthority::DatabaseControl,
            error,
        }
    }

    const fn journal_store(store_path: &'static str, error: InternalError) -> Self {
        Self {
            authority: StartupRecoveryFailureAuthority::JournalStore(store_path),
            error,
        }
    }

    pub(in crate::db) const fn authority(&self) -> StartupRecoveryFailureAuthority {
        self.authority
    }

    pub(in crate::db) const fn error(&self) -> &InternalError {
        &self.error
    }

    pub(in crate::db) fn into_error(self) -> InternalError {
        self.error
    }
}

#[cfg(test)]
pub(crate) fn continue_recovery<C: CanisterKind>(
    db: &Db<C>,
) -> Result<RecoveryProgress, InternalError> {
    continue_recovery_with_failure_authority(db).map_err(StartupRecoveryFailure::into_error)
}

pub(in crate::db) fn continue_recovery_with_failure_authority<C: CanisterKind>(
    db: &Db<C>,
) -> Result<RecoveryProgress, StartupRecoveryFailure> {
    configure_commit_memory_id(C::COMMIT_MEMORY_ID, C::COMMIT_STABLE_KEY).map_err(|error| {
        StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
    })?;
    ensure_database_format_admitted(db).map_err(StartupRecoveryFailure::database_control)?;
    let recovery_key = recovery_domain_key(db).map_err(|error| {
        StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
    })?;

    if !recovery_domain_recovered(recovery_key).map_err(|error| {
        StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
    })? {
        return recover_domain(db, recovery_key);
    }

    let recovery_in_progress = recovery_domain_in_progress(recovery_key);

    if !commit_marker_may_be_present() && !recovery_in_progress {
        return continue_online_convergence(db);
    }

    if commit_marker_present_fast().map_err(|error| {
        StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
    })? {
        return recover_domain(db, recovery_key);
    }

    if recovery_in_progress {
        // A previous recovery can be interrupted after marker clear but before
        // volatile readiness is restored. Marker absence alone is not enough
        // to prove this recovery domain completed.
        return recover_domain(db, recovery_key);
    }

    mark_commit_marker_verified_absent();

    continue_online_convergence(db)
}

fn continue_online_convergence<C: CanisterKind>(
    db: &Db<C>,
) -> Result<RecoveryProgress, StartupRecoveryFailure> {
    fold_oldest_journal_batch(db, JournalFoldProjection::OnlinePositioned).map(|complete| {
        if complete {
            RecoveryProgress::Complete
        } else {
            RecoveryProgress::Pending
        }
    })
}

fn recover_domain<C: CanisterKind>(
    db: &Db<C>,
    recovery_key: RecoveryDomainKey,
) -> Result<RecoveryProgress, StartupRecoveryFailure> {
    mark_recovery_domain_in_progress(recovery_key);
    let marker = with_commit_store(super::store::CommitStore::load).map_err(|error| {
        StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
    })?;
    let progress = if marker.is_none() && journaled_tails_are_empty(db)? {
        restore_live_schema_checkpoints(db, None).map_err(|error| {
            StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
        })?;
        reset_journaled_live_projections(db)?;
        db.mark_all_registered_index_stores_ready()
            .map_err(|error| {
                StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
            })?;
        mark_commit_marker_verified_absent();
        RecoveryProgress::Complete
    } else {
        perform_recovery_page(db, marker)?
    };
    if progress == RecoveryProgress::Complete {
        mark_recovery_domain_recovered(recovery_key).map_err(|error| {
            StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
        })?;
        clear_recovery_domain_in_progress(recovery_key);
    }
    Ok(progress)
}

fn journaled_tails_are_empty<C: CanisterKind>(db: &Db<C>) -> Result<bool, StartupRecoveryFailure> {
    let mut all_empty = true;
    for (store_path, handle) in sorted_journaled_store_handles(db) {
        let journal_failure = |error| StartupRecoveryFailure::journal_store(store_path, error);
        let journal = handle
            .journal_tail_store()
            .ok_or_else(InternalError::store_corruption)
            .map_err(journal_failure)?;
        let control = journal
            .with_borrow(JournalTailStore::validate_current_tail_authority)
            .map_err(journal_failure)?;
        all_empty &= control.is_empty();
    }
    Ok(all_empty)
}

fn perform_recovery_page<C: CanisterKind>(
    db: &Db<C>,
    marker: Option<CommitMarker>,
) -> Result<RecoveryProgress, StartupRecoveryFailure> {
    let had_marker = marker.is_some();
    restore_live_schema_checkpoints(db, marker.as_ref()).map_err(|error| {
        StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
    })?;
    if let Some(marker) = marker.as_ref() {
        apply_marker_live_schema_checkpoints(db, marker).map_err(|error| {
            StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
        })?;
        publish_marker_bound_journal_batches(db, marker)?;
        for operation in marker.database_control() {
            match operation {
                DatabaseControlOp::SchemaApplication(operation) => {
                    apply_schema_application_record_op(operation).map_err(|error| {
                        StartupRecoveryFailure::database_control(
                            error.with_origin(ErrorOrigin::Recovery),
                        )
                    })?;
                }
                #[cfg(any(test, feature = "migration"))]
                DatabaseControlOp::EntitySourceLineage(operation) => {
                    crate::db::schema::apply_entity_source_lineage_catalog_op(operation).map_err(
                        |error| {
                            StartupRecoveryFailure::database_control(
                                error.with_origin(ErrorOrigin::Recovery),
                            )
                        },
                    )?;
                }
                #[cfg(any(test, feature = "migration"))]
                DatabaseControlOp::SchemaMigration(operation) => {
                    apply_schema_migration_record_op(operation).map_err(|error| {
                        StartupRecoveryFailure::database_control(
                            error.with_origin(ErrorOrigin::Recovery),
                        )
                    })?;
                }
                DatabaseControlOp::MutationProgress(operation) => {
                    apply_mutation_progress_record_op::<C>(operation).map_err(|error| {
                        StartupRecoveryFailure::database_control(
                            error.with_origin(ErrorOrigin::Recovery),
                        )
                    })?;
                }
            }
        }
    }

    // Disposable overlays may contain effects from the predecessor Wasm or a
    // same-process interruption test. Canonical row, index, and schema stores
    // remain the only fold inputs across recovery pages.
    reset_journaled_live_projections(db)?;

    // Fold one bounded journal batch. Every batch validates completely before
    // its canonical effects and watermark retire in one replicated message.
    if !fold_oldest_journal_batch(db, JournalFoldProjection::StartupUnpositioned)? {
        return Ok(RecoveryProgress::Pending);
    }

    // Verify only marker-owned effects and terminal fold state before
    // clearing marker authority. Whole-database integrity is an explicit
    // bounded inspection workflow, not a recovery side effect.
    verify_recovered_effects(db, marker.as_ref()).map_err(|error| {
        StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
    })?;

    // Clear marker only after replay + fold + effect validation succeed.
    if had_marker {
        with_commit_store(super::store::CommitStore::clear_verified).map_err(|error| {
            StartupRecoveryFailure::database_control(error.with_origin(ErrorOrigin::Recovery))
        })?;
    }

    db.mark_all_registered_index_stores_ready()
        .map_err(StartupRecoveryFailure::database_control)?;
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
) -> Result<(), StartupRecoveryFailure> {
    let mut prepared = Vec::with_capacity(marker.journal_batches().len());
    for batch in marker.journal_batches() {
        let (store_path, handle) = journal_batch_store_handle(db, batch)
            .map_err(StartupRecoveryFailure::database_control)?;
        let journal_store = handle.journal_tail_store();
        let direct = journal_batch_is_direct_schema_publication(batch) || journal_store.is_none();
        if direct {
            validate_replayed_journal_batch(db, store_path, handle, batch)
                .map_err(StartupRecoveryFailure::database_control)?;
        }
        prepared.push((store_path, handle, batch, journal_store, direct));
    }

    // Finish every fallible tail append before the first direct canonical
    // mutation. Direct batches were completely preflighted above; an
    // impossible Apply contradiction therefore traps for message rollback.
    for (store_path, _, batch, journal_store, direct) in &prepared {
        if *direct {
            continue;
        }
        let journal_store = journal_store.ok_or_else(|| {
            StartupRecoveryFailure::database_control(InternalError::store_corruption())
        })?;
        journal_store
            .with_borrow_mut(|store| {
                store.append_batch(batch)?;

                Ok::<(), InternalError>(())
            })
            .map_err(|error| StartupRecoveryFailure::journal_store(store_path, error))?;
    }
    for (store_path, handle, batch, _, direct) in prepared {
        if direct {
            apply_validated_replayed_journal_batch(db, store_path, handle, batch);
        }
    }

    Ok(())
}

fn reset_journaled_live_projections<C: CanisterKind>(
    db: &Db<C>,
) -> Result<(), StartupRecoveryFailure> {
    for (store_path, handle) in sorted_journaled_store_handles(db) {
        let journal_failure = |error| StartupRecoveryFailure::journal_store(store_path, error);
        handle.mark_index_building().map_err(journal_failure)?;
        let data_generation = handle
            .with_data_mut(|store| {
                store.reset_journaled_live_projection()?;
                Ok::<_, InternalError>(store.generation())
            })
            .map_err(journal_failure)?;
        let fold_watermark = handle
            .journal_tail_store()
            .ok_or_else(InternalError::store_corruption)
            .and_then(|journal| journal.with_borrow(JournalTailStore::fold_watermark))
            .map_err(journal_failure)?;
        handle
            .with_index_mut(|store| {
                store.reset_journaled_live_projection(data_generation, fold_watermark)
            })
            .map_err(journal_failure)?;
        handle
            .with_schema_mut(SchemaStore::reset_journaled_live_projection)
            .map_err(journal_failure)?;
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalFoldProjection {
    StartupUnpositioned,
    OnlinePositioned,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct JournalHeadOrder {
    database_commit_sequence: DatabaseCommitSequence,
    journal_allocation: u8,
    journal_sequence: JournalSequence,
}

struct SelectedJournalHead {
    store_path: &'static str,
    handle: StoreHandle,
    watermark: FoldWatermark,
    batch: JournalBatch,
}

struct SelectedJournalControlHead {
    store_path: &'static str,
    handle: StoreHandle,
    watermark: FoldWatermark,
}

struct PreparedOnlineBatchRetirement {
    data_store: &'static LocalKey<RefCell<DataStore>>,
    data: PreparedDataPositionRetirement,
    index: Vec<(
        &'static LocalKey<RefCell<IndexStore>>,
        PreparedIndexPositionRetirement,
    )>,
    schema_store: &'static LocalKey<RefCell<SchemaStore>>,
    schema: PreparedSchemaPositionRetirement,
}

type GroupedOnlineIndexRetirementKeys = Vec<(
    &'static LocalKey<RefCell<IndexStore>>,
    Vec<crate::db::index::RawIndexStoreKey>,
)>;

struct OnlineRowRetirementTarget<'a> {
    record_ordinal: usize,
    entity_path: &'a str,
    primary_key: &'a RawDataStoreKey,
    after: Option<&'a RawRow>,
    schema_fingerprint: CommitSchemaFingerprint,
}

fn prepare_folded_row_transitions<C: CanisterKind>(
    db: &Db<C>,
    handle: StoreHandle,
    batch: &JournalBatch,
) -> Result<Vec<Option<PreparedRowCommitOp>>, InternalError> {
    let mut by_record = vec![None; batch.records().len()];
    if batch
        .records()
        .iter()
        .any(|record| matches!(record, JournalRecord::AcceptedSchemaPublish { .. }))
    {
        return Ok(by_record);
    }

    let mut record_ordinals = Vec::new();
    let mut row_ops = Vec::new();
    for (record_ordinal, record) in batch.records().iter().enumerate() {
        let (entity_path, primary_key, after, schema_fingerprint) = match record {
            JournalRecord::RowPut {
                entity_path,
                primary_key,
                row_bytes,
                schema_fingerprint,
            } => (
                entity_path,
                primary_key,
                Some(row_bytes.clone()),
                *schema_fingerprint,
            ),
            JournalRecord::RowDelete {
                entity_path,
                primary_key,
                schema_fingerprint,
            } => (entity_path, primary_key, None, *schema_fingerprint),
            _ => continue,
        };
        let before = handle.with_data(|store| {
            store
                .get_canonical(primary_key)
                .map(|row| row.as_bytes().to_vec())
        });
        row_ops.push(CommitRowOp::try_new_bytes(
            entity_path.as_str(),
            primary_key.as_bytes(),
            before,
            after,
            schema_fingerprint,
        )?);
        record_ordinals.push((record_ordinal, primary_key));
    }
    let prepared = db.prepare_row_commit_batch_for_replay(&row_ops)?;
    for ((record_ordinal, primary_key), prepared) in record_ordinals.into_iter().zip(prepared) {
        if !std::ptr::eq(prepared.data_store, handle.data_store())
            || prepared.data_key != *primary_key
        {
            return Err(InternalError::store_corruption());
        }
        prepared.preflight_fold_recovered()?;
        by_record[record_ordinal] = Some(prepared);
    }
    Ok(by_record)
}

fn collect_online_index_retirement_key(
    grouped: &mut GroupedOnlineIndexRetirementKeys,
    store: &'static LocalKey<RefCell<IndexStore>>,
    key: crate::db::index::RawIndexStoreKey,
) {
    if let Some((_, keys)) = grouped
        .iter_mut()
        .find(|(candidate, _)| std::ptr::eq(*candidate, store))
    {
        keys.push(key);
    } else {
        grouped.push((store, vec![key]));
    }
}

fn collect_online_row_retirement_keys<C: CanisterKind>(
    db: &Db<C>,
    handle: StoreHandle,
    target: OnlineRowRetirementTarget<'_>,
    prepared_rows: &[Option<PreparedRowCommitOp>],
    data_keys: &mut Vec<RawDataStoreKey>,
    grouped_index_keys: &mut GroupedOnlineIndexRetirementKeys,
) -> Result<(), InternalError> {
    let fallback = if prepared_rows
        .get(target.record_ordinal)
        .and_then(Option::as_ref)
        .is_none()
    {
        let prepared = prepare_recovered_row_transition(
            db,
            handle,
            target.entity_path,
            target.primary_key,
            target.after,
            target.schema_fingerprint,
        )?;
        prepared.preflight_fold_recovered()?;
        Some(prepared)
    } else {
        None
    };
    let prepared = prepared_rows
        .get(target.record_ordinal)
        .and_then(Option::as_ref)
        .or(fallback.as_ref())
        .ok_or_else(InternalError::store_corruption)?;
    data_keys.push(prepared.data_key.clone());
    for index_op in &prepared.index_ops {
        let _decision = classify_derived_index_overlay(index_op.value.as_ref());
        collect_online_index_retirement_key(
            grouped_index_keys,
            index_op.index_store,
            index_op.key.clone(),
        );
    }
    Ok(())
}

fn fold_oldest_journal_batch<C: CanisterKind>(
    db: &Db<C>,
    projection: JournalFoldProjection,
) -> Result<bool, StartupRecoveryFailure> {
    let Some(selected) = select_oldest_journal_head(db)? else {
        return Ok(true);
    };
    fold_selected_journal_head(db, selected, projection)?;
    journaled_tails_are_empty(db)
}

fn select_oldest_journal_head<C: CanisterKind>(
    db: &Db<C>,
) -> Result<Option<SelectedJournalHead>, StartupRecoveryFailure> {
    let mut selected: Option<(JournalHeadOrder, SelectedJournalControlHead)> = None;
    for (store_path, handle) in sorted_journaled_store_handles(db) {
        let journal_failure = |error| StartupRecoveryFailure::journal_store(store_path, error);
        let journal_store = handle
            .journal_tail_store()
            .ok_or_else(InternalError::store_corruption)
            .map_err(journal_failure)?;
        let control = journal_store
            .with_borrow(JournalTailStore::validate_current_tail_authority)
            .map_err(journal_failure)?;
        if control.is_empty() {
            continue;
        }
        let watermark = journal_store
            .with_borrow(JournalTailStore::fold_watermark)
            .map_err(journal_failure)?;
        let journal_sequence = watermark
            .highest_folded_journal_sequence()
            .next()
            .ok_or_else(InternalError::store_corruption)
            .map_err(journal_failure)?;
        let database_commit_sequence = control
            .head_database_commit_sequence()
            .ok_or_else(InternalError::store_corruption)
            .map_err(journal_failure)?;
        let allocation = handle
            .journal_allocation()
            .ok_or_else(InternalError::store_corruption)
            .map_err(journal_failure)?;
        let order = JournalHeadOrder {
            database_commit_sequence,
            journal_allocation: allocation.memory_id(),
            journal_sequence,
        };
        if selected
            .as_ref()
            .is_none_or(|(current, _)| order < *current)
        {
            selected = Some((
                order,
                SelectedJournalControlHead {
                    store_path,
                    handle,
                    watermark,
                },
            ));
        }
    }
    let Some((order, selected)) = selected else {
        return Ok(None);
    };
    let SelectedJournalControlHead {
        store_path,
        handle,
        watermark,
    } = selected;
    let journal_failure = |error| StartupRecoveryFailure::journal_store(store_path, error);
    let journal_store = handle
        .journal_tail_store()
        .ok_or_else(InternalError::store_corruption)
        .map_err(journal_failure)?;
    let batch = journal_store
        .with_borrow(|store| store.next_batch_after(watermark.highest_folded_journal_sequence()))
        .map_err(journal_failure)?
        .ok_or_else(InternalError::store_corruption)
        .map_err(journal_failure)?;
    if batch.database_commit_sequence() != order.database_commit_sequence
        || batch.journal_sequence() != order.journal_sequence
    {
        return Err(journal_failure(InternalError::store_corruption()));
    }
    Ok(Some(SelectedJournalHead {
        store_path,
        handle,
        watermark,
        batch,
    }))
}

fn fold_selected_journal_head<C: CanisterKind>(
    db: &Db<C>,
    selected: SelectedJournalHead,
    projection: JournalFoldProjection,
) -> Result<(), StartupRecoveryFailure> {
    let SelectedJournalHead {
        store_path,
        handle,
        watermark,
        batch,
    } = selected;
    let journal_failure = |error| StartupRecoveryFailure::journal_store(store_path, error);
    let journal_store = handle
        .journal_tail_store()
        .ok_or_else(InternalError::store_corruption)
        .map_err(journal_failure)?;
    let next_watermark =
        prepare_folded_journal_batch_completion(&batch, watermark).map_err(journal_failure)?;
    let mut prepared_rows =
        prepare_folded_row_transitions(db, handle, &batch).map_err(journal_failure)?;
    let prepared_row_count = prepared_rows.iter().filter(|row| row.is_some()).count();
    let batch_row_count = batch
        .records()
        .iter()
        .filter(|record| {
            matches!(
                record,
                JournalRecord::RowPut { .. } | JournalRecord::RowDelete { .. }
            )
        })
        .count();
    let row_fold_preflight = match (prepared_row_count, batch_row_count) {
        (0, _) => JournalRowFoldPreflight::Required,
        (prepared, rows) if prepared == rows => JournalRowFoldPreflight::PreparedBatch,
        _ => return Err(journal_failure(InternalError::store_corruption())),
    };
    let overlay_retirement = match projection {
        JournalFoldProjection::StartupUnpositioned => {
            let _candidate = validate_journal_batch_records(
                db,
                store_path,
                handle,
                &batch,
                JournalRecordApplyMode::Fold,
                row_fold_preflight,
            )
            .map_err(journal_failure)?;
            None
        }
        JournalFoldProjection::OnlinePositioned => {
            // Row retirement, complete validation, and Apply share the same
            // canonical-predecessor transition so accepted authority and
            // derived indexes are prepared only once for this batch.
            let retirement =
                prepare_online_batch_retirement(db, handle, &batch, prepared_rows.as_slice())
                    .map_err(journal_failure)?;
            let _candidate = validate_journal_batch_records(
                db,
                store_path,
                handle,
                &batch,
                JournalRecordApplyMode::Fold,
                row_fold_preflight,
            )
            .map_err(journal_failure)?;
            Some(retirement)
        }
    };
    let tail_retirement = journal_store
        .with_borrow(|store| store.prepare_batch_retirement(&batch, next_watermark))
        .map_err(journal_failure)?;
    let cardinality_maintenance = prepare_folded_cardinality_maintenance(
        handle,
        watermark,
        next_watermark,
        &batch,
        prepared_rows.as_slice(),
    )
    .map_err(journal_failure)?;
    handle
        .with_index(|store| store.preflight_prefix_cardinality_delta_watermark(watermark))
        .map_err(journal_failure)?;
    apply_preflighted_fold(
        db,
        store_path,
        handle,
        &batch,
        prepared_rows.as_mut_slice(),
        cardinality_maintenance,
    );
    if let Some(retirement) = overlay_retirement {
        apply_online_batch_retirement(retirement);
    }
    journal_store.with_borrow_mut(|store| {
        store.apply_prepared_batch_retirement(tail_retirement);
    });
    handle.with_index_mut(|store| {
        store.apply_prefix_cardinality_delta_watermark(watermark, next_watermark);
    });
    Ok(())
}

fn apply_preflighted_fold<C: CanisterKind>(
    db: &Db<C>,
    store_path: &'static str,
    handle: StoreHandle,
    batch: &JournalBatch,
    prepared_rows: &mut [Option<PreparedRowCommitOp>],
    cardinality_maintenance: Option<PreparedCardinalityMaintenance>,
) {
    for (record_ordinal, record) in batch.records().iter().enumerate() {
        let prepared_row = prepared_rows.get_mut(record_ordinal).and_then(Option::take);
        let result = prepared_row.map_or_else(
            || {
                apply_journal_record(
                    db,
                    store_path,
                    handle,
                    batch,
                    record_ordinal,
                    record,
                    JournalRecordApplyMode::Fold,
                )
            },
            PreparedRowCommitOp::fold_recovered,
        );
        if let Err(error) = result {
            trap_validated_journal_apply_contradiction(error);
        }
    }
    if let Some(maintenance) = cardinality_maintenance {
        let result = handle
            .with_schema_mut(|store| store.apply_prepared_cardinality_maintenance(maintenance));
        if let Err(error) = result {
            trap_validated_journal_apply_contradiction(error);
        }
    }
}

fn prepare_folded_cardinality_maintenance(
    handle: StoreHandle,
    watermark: FoldWatermark,
    next_watermark: FoldWatermark,
    batch: &JournalBatch,
    prepared_rows: &[Option<PreparedRowCommitOp>],
) -> Result<Option<PreparedCardinalityMaintenance>, InternalError> {
    if batch
        .records()
        .iter()
        .any(cardinality_batch_invalidates_source)
    {
        return Ok(None);
    }
    let incarnation = database_incarnation_id()?;
    let authority = handle.with_schema(|schema| {
        CardinalityBuildAuthority::derive(
            schema,
            incarnation,
            handle.allocation_identities(),
            watermark,
        )
    })?;
    let Some(header) = handle.with_schema(SchemaStore::cardinality_generation_header)? else {
        return Ok(None);
    };
    if header.state() != CardinalityGenerationState::Ready
        || header.validate_source(authority.source()).is_err()
    {
        return Ok(None);
    }
    let next_authority = handle.with_schema(|schema| {
        CardinalityBuildAuthority::derive(
            schema,
            incarnation,
            handle.allocation_identities(),
            next_watermark,
        )
    })?;
    let changes = collect_folded_cardinality_changes(handle, prepared_rows, &authority)?;
    handle
        .with_schema(|schema| {
            schema.prepare_cardinality_maintenance(
                header,
                authority.source(),
                next_authority.source(),
                changes.as_slice(),
            )
        })
        .map(Some)
}

const fn cardinality_batch_invalidates_source(record: &JournalRecord) -> bool {
    match record {
        JournalRecord::AcceptedSchemaPublish { .. }
        | JournalRecord::AcceptedSchemaIndexDelete { .. }
        | JournalRecord::AcceptedSchemaIndexPut { .. } => true,
        #[cfg(any(test, feature = "migration"))]
        JournalRecord::SchemaMigrationRowPut { .. }
        | JournalRecord::SchemaMigrationIndexPut { .. } => true,
        JournalRecord::RowPut { .. }
        | JournalRecord::RowDelete { .. }
        | JournalRecord::SchemaPut { .. }
        | JournalRecord::ConstraintValidationJobPut { .. }
        | JournalRecord::ConstraintValidationJobDelete { .. }
        | JournalRecord::ConstraintValidationIndexPut { .. }
        | JournalRecord::IdentityRangeAdvance { .. } => false,
    }
}

fn collect_folded_cardinality_changes(
    handle: StoreHandle,
    prepared_rows: &[Option<PreparedRowCommitOp>],
    authority: &CardinalityBuildAuthority,
) -> Result<Vec<(CardinalityCountDigest, i64)>, InternalError> {
    let mut final_rows = BTreeMap::new();
    let mut final_indexes = BTreeMap::new();
    for prepared in prepared_rows.iter().flatten() {
        if !std::ptr::eq(prepared.data_store, handle.data_store()) {
            return Err(InternalError::store_corruption());
        }
        final_rows.insert(prepared.data_key.clone(), prepared.data_value.is_some());
        for mutation in &prepared.index_ops {
            if std::ptr::eq(mutation.index_store, handle.index_store()) {
                final_indexes.insert(mutation.key.clone(), mutation.value.clone());
            }
        }
    }

    let mut changes = BTreeMap::new();
    for (key, next_present) in final_rows {
        let decoded = DecodedDataStoreKey::try_from_raw(&key)
            .map_err(|_| InternalError::store_corruption())?;
        if !authority.accepts_entity(decoded.entity_tag()) {
            return Err(InternalError::store_corruption());
        }
        let previous_present = handle.with_data(|store| store.get_canonical(&key).is_some());
        let delta = match (previous_present, next_present) {
            (false, true) => 1,
            (true, false) => -1,
            _ => 0,
        };
        if delta != 0 {
            let digest = CardinalityCountDigest::for_entity(decoded.entity_tag());
            add_cardinality_change(&mut changes, digest, delta)?;
        }
    }
    for (key, next) in final_indexes {
        let previous = handle.with_index(|store| store.get_canonical(&key));
        for digest in accepted_prefix_digests(&key, previous.as_ref(), authority)? {
            add_cardinality_change(&mut changes, digest, -1)?;
        }
        for digest in accepted_prefix_digests(&key, next.as_ref(), authority)? {
            add_cardinality_change(&mut changes, digest, 1)?;
        }
    }
    Ok(changes
        .into_iter()
        .filter(|(_, delta)| *delta != 0)
        .collect())
}

fn accepted_prefix_digests(
    raw_key: &crate::db::index::RawIndexStoreKey,
    value: Option<&IndexEntryValue>,
    authority: &CardinalityBuildAuthority,
) -> Result<Vec<CardinalityCountDigest>, InternalError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let key = IndexKey::try_from_raw(raw_key).map_err(|_| InternalError::store_corruption())?;
    if key.key_kind() != IndexKeyKind::User {
        return Ok(Vec::new());
    }
    let Some(component_count) = authority.accepted_index_component_count(*key.index_id()) else {
        return Ok(Vec::new());
    };
    if key.component_count() != component_count {
        return Err(InternalError::store_corruption());
    }
    let witness = value
        .decode_row_witness_from_index_key(&key)
        .map_err(|_| InternalError::store_corruption())?;
    if witness.existence_witness() != IndexEntryExistenceWitness::Present {
        return Ok(Vec::new());
    }
    let mut digests = Vec::new();
    digests
        .try_reserve_exact(component_count)
        .map_err(|_| InternalError::store_unsupported())?;
    let mut components = Vec::new();
    components
        .try_reserve_exact(component_count)
        .map_err(|_| InternalError::store_unsupported())?;
    for component_index in 0..component_count {
        let component = key
            .component(component_index)
            .ok_or_else(InternalError::store_corruption)?;
        components.push(component.to_vec());
        digests.push(CardinalityCountDigest::for_user_index_prefix(
            *key.index_id(),
            components.as_slice(),
        )?);
    }
    Ok(digests)
}

fn add_cardinality_change(
    changes: &mut BTreeMap<CardinalityCountDigest, i64>,
    digest: CardinalityCountDigest,
    delta: i64,
) -> Result<(), InternalError> {
    let current = changes.entry(digest).or_insert(0);
    *current = current
        .checked_add(delta)
        .ok_or_else(InternalError::store_unsupported)?;
    Ok(())
}

fn prepare_online_batch_retirement<C: CanisterKind>(
    db: &Db<C>,
    handle: StoreHandle,
    batch: &JournalBatch,
    prepared_rows: &[Option<PreparedRowCommitOp>],
) -> Result<PreparedOnlineBatchRetirement, InternalError> {
    let allocation = handle
        .journal_allocation()
        .ok_or_else(InternalError::store_corruption)?;
    let position = JournalOverlayPosition::new(allocation, batch.journal_sequence());
    let mut data_keys = Vec::new();
    let mut grouped_index_keys = Vec::new();
    for (record_ordinal, record) in batch.records().iter().enumerate() {
        let _decision = classify_journal_overlay(record);
        match record {
            JournalRecord::RowPut {
                entity_path,
                primary_key,
                row_bytes,
                schema_fingerprint,
            } => {
                let row =
                    RawRow::from_untrusted_bytes(row_bytes.clone()).map_err(InternalError::from)?;
                collect_online_row_retirement_keys(
                    db,
                    handle,
                    OnlineRowRetirementTarget {
                        record_ordinal,
                        entity_path,
                        primary_key,
                        after: Some(&row),
                        schema_fingerprint: *schema_fingerprint,
                    },
                    prepared_rows,
                    &mut data_keys,
                    &mut grouped_index_keys,
                )?;
            }
            JournalRecord::RowDelete {
                entity_path,
                primary_key,
                schema_fingerprint,
            } => {
                collect_online_row_retirement_keys(
                    db,
                    handle,
                    OnlineRowRetirementTarget {
                        record_ordinal,
                        entity_path,
                        primary_key,
                        after: None,
                        schema_fingerprint: *schema_fingerprint,
                    },
                    prepared_rows,
                    &mut data_keys,
                    &mut grouped_index_keys,
                )?;
            }
            JournalRecord::AcceptedSchemaIndexDelete { keys, .. }
            | JournalRecord::AcceptedSchemaIndexPut { keys, .. } => {
                for key in keys {
                    collect_online_index_retirement_key(
                        &mut grouped_index_keys,
                        handle.index_store(),
                        key.clone(),
                    );
                }
            }
            JournalRecord::ConstraintValidationIndexPut { key, .. } => {
                collect_online_index_retirement_key(
                    &mut grouped_index_keys,
                    handle.index_store(),
                    key.clone(),
                );
            }
            #[cfg(any(test, feature = "migration"))]
            JournalRecord::SchemaMigrationRowPut { primary_key, .. } => {
                data_keys.push(primary_key.clone());
            }
            #[cfg(any(test, feature = "migration"))]
            JournalRecord::SchemaMigrationIndexPut { key, .. } => {
                collect_online_index_retirement_key(
                    &mut grouped_index_keys,
                    handle.index_store(),
                    key.clone(),
                );
            }
            JournalRecord::SchemaPut { .. }
            | JournalRecord::AcceptedSchemaPublish { .. }
            | JournalRecord::ConstraintValidationJobPut { .. }
            | JournalRecord::ConstraintValidationJobDelete { .. }
            | JournalRecord::IdentityRangeAdvance { .. } => {}
        }
    }
    finalize_online_batch_retirement(handle, batch, position, data_keys, grouped_index_keys)
}

fn finalize_online_batch_retirement(
    handle: StoreHandle,
    batch: &JournalBatch,
    position: JournalOverlayPosition,
    data_keys: Vec<RawDataStoreKey>,
    grouped_index_keys: GroupedOnlineIndexRetirementKeys,
) -> Result<PreparedOnlineBatchRetirement, InternalError> {
    let data = handle.with_data(|store| store.prepare_position_retirement(data_keys, position))?;
    let index = grouped_index_keys
        .into_iter()
        .map(|(store, keys)| {
            store
                .with_borrow(|store| store.prepare_position_retirement(keys, position))
                .map(|retirement| (store, retirement))
        })
        .collect::<Result<Vec<_>, InternalError>>()?;
    let incarnation = database_incarnation_id()?;
    let schema = handle.with_schema(|store| {
        store.prepare_positioned_journal_batch_retirement(incarnation, batch, position)
    })?;
    Ok(PreparedOnlineBatchRetirement {
        data_store: handle.data_store(),
        data,
        index,
        schema_store: handle.schema_store(),
        schema,
    })
}

fn apply_online_batch_retirement(prepared: PreparedOnlineBatchRetirement) {
    prepared
        .data_store
        .with_borrow_mut(|store| store.apply_prepared_position_retirement(prepared.data));
    for (store, retirement) in prepared.index {
        store.with_borrow_mut(|store| store.apply_prepared_position_retirement(retirement));
    }
    prepared.schema_store.with_borrow_mut(|store| {
        store.apply_prepared_journal_batch_retirement(prepared.schema);
    });
}

fn prepare_folded_journal_batch_completion(
    batch: &JournalBatch,
    watermark: FoldWatermark,
) -> Result<FoldWatermark, InternalError> {
    if watermark.highest_folded_journal_sequence().next() != Some(batch.journal_sequence()) {
        return Err(InternalError::store_corruption());
    }
    let next_epoch = watermark
        .fold_epoch()
        .checked_add(1)
        .ok_or_else(InternalError::store_corruption)?;
    Ok(FoldWatermark::new(batch.journal_sequence(), next_epoch))
}

#[cfg(target_arch = "wasm32")]
fn trap_validated_journal_apply_contradiction(_error: InternalError) -> ! {
    ic_cdk::trap("validated journal application contradicted its complete preflight")
}

#[cfg(not(target_arch = "wasm32"))]
fn trap_validated_journal_apply_contradiction(_error: InternalError) -> ! {
    std::process::abort()
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalRowFoldPreflight {
    Required,
    PreparedBatch,
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

fn validate_replayed_journal_batch<C: CanisterKind>(
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
        JournalRowFoldPreflight::Required,
    )?;

    Ok(())
}

fn apply_validated_replayed_journal_batch<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
) {
    for (record_ordinal, record) in batch.records().iter().enumerate() {
        if let Err(error) = apply_journal_record(
            db,
            expected_store_path,
            expected_handle,
            batch,
            record_ordinal,
            record,
            JournalRecordApplyMode::Replay,
        ) {
            trap_validated_journal_apply_contradiction(error);
        }
    }
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
            let runtime_entity = match mode {
                JournalRecordApplyMode::Replay => {
                    db.accepted_runtime_entity_for_path(snapshot.entity_path())?
                }
                JournalRecordApplyMode::Fold => {
                    crate::db::runtime_entity_catalog::canonical_runtime_entity_for_path(
                        db,
                        snapshot.entity_path(),
                    )?
                }
            };
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
        JournalRecord::AcceptedSchemaIndexDelete { keys, .. } => {
            apply_recovered_accepted_schema_index_chunk(expected_handle, keys, false, mode)
        }
        JournalRecord::AcceptedSchemaIndexPut { keys, .. } => {
            apply_recovered_accepted_schema_index_chunk(expected_handle, keys, true, mode)
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
    let before = expected_handle.with_data(|store| {
        store
            .get_canonical(primary_key)
            .map(|row| row.as_bytes().to_vec())
    });
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
    prepared.fold_recovered()
}

fn apply_recovered_accepted_schema_index_chunk(
    handle: StoreHandle,
    keys: &[crate::db::index::RawIndexStoreKey],
    insert: bool,
    mode: JournalRecordApplyMode,
) -> Result<(), InternalError> {
    if mode != JournalRecordApplyMode::Fold
        || handle.storage_capabilities().recovery()
            != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_corruption());
    }
    handle.with_index_mut(|store| {
        for key in keys {
            store.fold_recovered_journal_entry(
                key.clone(),
                insert.then(IndexEntryValue::presence),
            )?;
        }
        Ok::<(), InternalError>(())
    })
}

fn validate_journal_batch_records<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
    mode: JournalRecordApplyMode,
    row_fold_preflight: JournalRowFoldPreflight,
) -> Result<Option<CandidateSchemaRevision>, InternalError> {
    if mode == JournalRecordApplyMode::Fold {
        expected_handle.with_data(DataStore::preflight_fold_recovered_journal)?;
        expected_handle.with_index(IndexStore::preflight_fold_recovered_journal)?;
        expected_handle.with_schema(SchemaStore::preflight_fold_recovered_journal)?;
    }
    let candidate =
        validate_journal_batch_envelope(db, expected_store_path, expected_handle, batch, mode)?;

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
            row_fold_preflight,
        )?;
    }

    Ok(candidate)
}

fn validate_journal_batch_envelope<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
    mode: JournalRecordApplyMode,
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
        mode,
    )?;

    Ok(candidate)
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
    row_fold_preflight: JournalRowFoldPreflight,
) -> Result<(), InternalError> {
    if mode == JournalRecordApplyMode::Fold
        && row_fold_preflight == JournalRowFoldPreflight::PreparedBatch
        && matches!(
            record,
            JournalRecord::RowPut { .. } | JournalRecord::RowDelete { .. }
        )
    {
        return Ok(());
    }
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
            let runtime_entity = match mode {
                JournalRecordApplyMode::Replay => {
                    db.accepted_runtime_entity_for_path(snapshot.entity_path())?
                }
                JournalRecordApplyMode::Fold => {
                    crate::db::runtime_entity_catalog::canonical_runtime_entity_for_path(
                        db,
                        snapshot.entity_path(),
                    )?
                }
            };
            if runtime_entity.store_path() != expected_store_path {
                return Err(InternalError::store_corruption());
            }
            if mode == JournalRecordApplyMode::Fold {
                expected_handle
                    .with_schema(|store| store.preflight_fold_persisted_snapshot(&snapshot))?;
            }
        }
        JournalRecord::AcceptedSchemaPublish {
            expected_revision, ..
        } => {
            let candidate = candidate.ok_or_else(InternalError::store_corruption)?;
            if mode == JournalRecordApplyMode::Fold {
                let incarnation = database_incarnation_id()?;
                expected_handle.with_schema(|store| {
                    store.preflight_fold_journaled_accepted_schema_candidate(
                        incarnation,
                        *expected_revision,
                        candidate,
                    )
                })?;
            }
        }
        JournalRecord::ConstraintValidationJobPut { job_bytes, .. } => {
            if mode == JournalRecordApplyMode::Fold {
                let job = decode_constraint_validation_job(job_bytes)?;
                expected_handle
                    .with_schema(|store| store.preflight_fold_constraint_validation_job(&job))?;
            }
        }
        JournalRecord::ConstraintValidationJobDelete { .. } => {
            if mode == JournalRecordApplyMode::Fold {
                expected_handle.with_schema(|store| {
                    store.preflight_fold_constraint_validation_job_removal()
                })?;
            }
        }
        JournalRecord::AcceptedSchemaIndexDelete {
            store_path,
            entity_tag,
            accepted_after_fingerprint,
            keys,
        } => validate_accepted_schema_index_chunk(
            expected_store_path,
            expected_handle,
            candidate,
            store_path,
            *entity_tag,
            *accepted_after_fingerprint,
            keys,
            false,
            mode,
        )?,
        JournalRecord::AcceptedSchemaIndexPut {
            store_path,
            entity_tag,
            accepted_after_fingerprint,
            keys,
        } => validate_accepted_schema_index_chunk(
            expected_store_path,
            expected_handle,
            candidate,
            store_path,
            *entity_tag,
            *accepted_after_fingerprint,
            keys,
            true,
            mode,
        )?,
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
                .with_schema(|store| match mode {
                    JournalRecordApplyMode::Replay => store.current_accepted_schema_bundle(),
                    JournalRecordApplyMode::Fold => {
                        store.current_canonical_accepted_schema_bundle()
                    }
                })?
                .ok_or_else(InternalError::store_corruption)?;
            super::schema_publication::validate_candidate_index_entries(
                &bundle,
                &job,
                std::slice::from_ref(key),
            )?;
            if mode == JournalRecordApplyMode::Fold {
                expected_handle.with_index(IndexStore::preflight_fold_recovered_journal)?;
            }
        }
        JournalRecord::IdentityRangeAdvance { range } => {
            let runtime_entity = match mode {
                JournalRecordApplyMode::Replay => {
                    db.accepted_runtime_entity_for_tag(range.owner().entity_tag())
                }
                JournalRecordApplyMode::Fold => {
                    crate::db::runtime_entity_catalog::canonical_runtime_entity_for_tag(
                        db,
                        range.owner().entity_tag(),
                    )
                }
            }
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
            if mode == JournalRecordApplyMode::Fold {
                expected_handle.with_schema(|store| {
                    store.preflight_fold_identity_range_advance(*range, advance_id)
                })?;
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
            if mode == JournalRecordApplyMode::Fold {
                expected_handle.with_data(DataStore::preflight_fold_recovered_journal)?;
            }
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
            if mode == JournalRecordApplyMode::Fold {
                expected_handle.with_index(IndexStore::preflight_fold_recovered_journal)?;
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "the recovery boundary validates every persisted index-chunk identity before apply"
)]
fn validate_accepted_schema_index_chunk(
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    candidate: Option<&CandidateSchemaRevision>,
    store_path: &str,
    entity_tag: EntityTag,
    accepted_after_fingerprint: CommitSchemaFingerprint,
    keys: &[crate::db::index::RawIndexStoreKey],
    insertion: bool,
    mode: JournalRecordApplyMode,
) -> Result<(), InternalError> {
    if store_path != expected_store_path
        || mode != JournalRecordApplyMode::Fold
        || expected_handle.storage_capabilities().recovery()
            != StoreRecoveryCapability::StableBasePlusJournalReplay
    {
        return Err(InternalError::store_corruption());
    }
    let candidate = candidate.ok_or_else(InternalError::store_corruption)?;
    let accepted_after = candidate
        .bundle()
        .entity_snapshots()
        .get(&entity_tag)
        .ok_or_else(InternalError::store_corruption)?;
    if accepted_schema_cache_fingerprint_for_persisted_snapshot(accepted_after)?
        != accepted_after_fingerprint
    {
        return Err(InternalError::store_corruption());
    }
    if insertion {
        for key in keys {
            let decoded =
                IndexKey::try_from_raw(key).map_err(|_| InternalError::store_corruption())?;
            if decoded.key_kind() != IndexKeyKind::User
                || !accepted_after.indexes().iter().any(|index| {
                    *decoded.index_id()
                        == crate::db::index::IndexId::new_with_generation(
                            entity_tag,
                            index.ordinal(),
                            index.physical_generation(),
                        )
                })
            {
                return Err(InternalError::store_corruption());
            }
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
        JournalRecordApplyMode::Fold => {
            canonical_journal_row_selection(
                db,
                expected_store_path,
                expected_handle,
                entity_path,
                primary_key,
                schema_fingerprint,
            )?;
            preflight_fold_recovered_row_transition(
                db,
                expected_handle,
                entity_path,
                primary_key,
                None,
                schema_fingerprint,
            )?;
            Ok(())
        }
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
            | JournalRecord::AcceptedSchemaIndexDelete { .. }
            | JournalRecord::AcceptedSchemaIndexPut { .. }
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
    mode: JournalRecordApplyMode,
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
                    mode,
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
                    mode,
                )?;
                removal = Some((*entity_tag, *constraint_id));
            }
            _ => {}
        }
    }

    let candidate_bundle = candidate.map(CandidateSchemaRevision::bundle);
    expected_handle.with_schema(|schema_store| {
        if let Some(bundle) = candidate_bundle {
            match mode {
                JournalRecordApplyMode::Replay => {
                    schema_store.validate_live_activation_transition(bundle)?;
                }
                JournalRecordApplyMode::Fold => {
                    schema_store.validate_canonical_activation_transition(bundle)?;
                }
            }
        }
        if replacement.is_none() && removal.is_none() {
            if let Some(bundle) = candidate_bundle {
                match mode {
                    JournalRecordApplyMode::Replay => {
                        schema_store.validate_constraint_validation_job_closure(bundle)?;
                    }
                    JournalRecordApplyMode::Fold => schema_store
                        .validate_canonical_constraint_validation_job_closure_with_change(
                            bundle, None, None,
                        )?,
                }
            }
            return Ok(());
        }
        let bundle = match candidate_bundle {
            Some(bundle) => bundle.clone(),
            None => match mode {
                JournalRecordApplyMode::Replay => schema_store.current_accepted_schema_bundle(),
                JournalRecordApplyMode::Fold => {
                    schema_store.current_canonical_accepted_schema_bundle()
                }
            }?
            .ok_or_else(InternalError::store_corruption)?,
        };
        match mode {
            JournalRecordApplyMode::Replay => schema_store
                .validate_constraint_validation_job_closure_with_change(
                    &bundle,
                    replacement.as_ref(),
                    removal,
                ),
            JournalRecordApplyMode::Fold => schema_store
                .validate_canonical_constraint_validation_job_closure_with_change(
                    &bundle,
                    replacement.as_ref(),
                    removal,
                ),
        }
    })
}

fn validate_constraint_validation_job_record_identity<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    record_store_path: &str,
    entity_tag: crate::types::EntityTag,
    _constraint_id: crate::db::schema::ConstraintId,
    mode: JournalRecordApplyMode,
) -> Result<(), InternalError> {
    if record_store_path != expected_store_path {
        return Err(InternalError::store_corruption());
    }
    let runtime_entity = match mode {
        JournalRecordApplyMode::Replay => db.accepted_runtime_entity_for_tag(entity_tag),
        JournalRecordApplyMode::Fold => {
            crate::db::runtime_entity_catalog::canonical_runtime_entity_for_tag(db, entity_tag)
        }
    }
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

fn preflight_fold_recovered_row_transition<C: CanisterKind>(
    db: &Db<C>,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    after: Option<&RawRow>,
    schema_fingerprint: CommitSchemaFingerprint,
) -> Result<(), InternalError> {
    let prepared = prepare_recovered_row_transition(
        db,
        expected_handle,
        entity_path,
        primary_key,
        after,
        schema_fingerprint,
    )?;
    prepared.preflight_fold_recovered()
}

fn prepare_recovered_row_transition<C: CanisterKind>(
    db: &Db<C>,
    expected_handle: StoreHandle,
    entity_path: &str,
    primary_key: &RawDataStoreKey,
    after: Option<&RawRow>,
    schema_fingerprint: CommitSchemaFingerprint,
) -> Result<crate::db::commit::PreparedRowCommitOp, InternalError> {
    let before = expected_handle.with_data(|store| {
        store
            .get_canonical(primary_key)
            .map(|row| row.as_bytes().to_vec())
    });
    let op = CommitRowOp::try_new_bytes(
        entity_path,
        primary_key.as_bytes(),
        before,
        after.map(|row| row.as_bytes().to_vec()),
        schema_fingerprint,
    )?;
    let prepared = db.prepare_row_commit_op_for_replay(&op)?;
    if !std::ptr::eq(prepared.data_store, expected_handle.data_store())
        || prepared.data_key != *primary_key
    {
        return Err(InternalError::store_corruption());
    }
    Ok(prepared)
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
    reader.validate_primary_key(&decoded_key)?;
    preflight_fold_recovered_row_transition(
        db,
        expected_handle,
        entity_path,
        primary_key,
        Some(&row),
        schema_fingerprint,
    )
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
    let runtime_entity =
        crate::db::runtime_entity_catalog::canonical_runtime_entity_for_path(db, entity_path)?;
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
        | JournalRecord::AcceptedSchemaIndexDelete { store_path, .. }
        | JournalRecord::AcceptedSchemaIndexPut { store_path, .. }
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
    stores: &'static LocalKey<crate::db::registry::StoreRegistry>,
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
    stores: &'static LocalKey<crate::db::registry::StoreRegistry>,
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
        JournalRecord::AcceptedSchemaIndexDelete { keys, .. } => {
            for key in keys {
                verify_recovered_index_delete(db, key, verified)?;
            }
        }
        JournalRecord::AcceptedSchemaIndexPut {
            store_path, keys, ..
        } => {
            for key in keys {
                verify_recovered_index_put(db, store_path, key, verified)?;
            }
        }
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

fn verify_recovered_index_delete<C: CanisterKind>(
    db: &Db<C>,
    key: &crate::db::index::RawIndexStoreKey,
    verified: &mut BTreeSet<RecoveredEffectIdentity>,
) -> Result<(), InternalError> {
    let decoded = IndexKey::try_from_raw(key)
        .map_err(|_| InternalError::recovery_effect_verification_failed())?;
    let runtime_entity = db
        .accepted_runtime_entity_for_tag(decoded.index_id().entity_tag())
        .map_err(|_| InternalError::recovery_effect_verification_failed())?;
    let identity = RecoveredEffectIdentity::Index {
        store_path: runtime_entity.store_path().to_string(),
        raw_key: key.as_bytes().to_vec(),
    };
    if verified.insert(identity) {
        let (_, handle) = registry_store_handle_for_path(db, runtime_entity.store_path())?;
        if handle.with_index(|store| store.get(key)).is_some() {
            return Err(InternalError::recovery_effect_verification_failed());
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn online_selector_order_is_database_then_allocation_then_tail_sequence() {
        let key = |database, allocation, sequence| JournalHeadOrder {
            database_commit_sequence: DatabaseCommitSequence::new(database),
            journal_allocation: allocation,
            journal_sequence: JournalSequence::new(sequence),
        };

        assert!(key(4, 250, 90) < key(5, 1, 1));
        assert!(key(5, 1, 90) < key(5, 2, 1));
        assert!(key(5, 2, 1) < key(5, 2, 2));
    }
}
