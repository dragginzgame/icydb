//! Module: db::commit::recovery
//! Responsibility: run system-level marker replay/rebuild recovery gates before operations.
//! Does not own: marker storage encoding, mutation planning, or query semantics.
//! Boundary: db entrypoints -> commit::recovery -> commit::{replay,rebuild,store} (one-way).
//!
//! This module implements a **system recovery step** that restores global
//! database invariants by completing or rolling back a previously started
//! commit before any new operation proceeds.
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
            CommitMarker, CommitRowOp,
            memory::configure_commit_memory_id,
            rebuild::rebuild_secondary_indexes_from_rows,
            replay::replay_commit_marker_row_ops,
            store::{
                commit_marker_may_be_present, commit_marker_present_fast,
                mark_commit_marker_verified_absent, with_commit_store,
            },
        },
        data::{DataStore, DecodedDataStoreKey, RawDataStoreKey, RawRow},
        diagnostics::integrity_report_after_recovery,
        index::IndexStore,
        journal::{
            FoldWatermark, JournalBatch, JournalRecord, JournalSequence, JournalTailStore,
            JournalTailVisit,
        },
        registry::{StoreHandle, StoreRecoveryCapability},
        schema::{
            AcceptedSchemaSnapshot, SchemaStore, accepted_commit_schema_fingerprint,
            decode_persisted_schema_snapshot, ensure_accepted_schema_snapshot,
            reconcile_runtime_schemas,
        },
    },
    error::{ErrorOrigin, InternalError},
    traits::CanisterKind,
    types::EntityTag,
};
use std::{cell::RefCell, sync::OnceLock};

#[cfg(test)]
use crate::db::commit::failpoint::{CommitFailpoint, hit_commit_failpoint};

static RECOVERED: OnceLock<()> = OnceLock::new();

thread_local! {
    static SCHEMA_RECONCILED_KEYS: RefCell<Vec<SchemaReconciliationKey>> =
        const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SchemaReconciliationKey {
    store_registry: usize,
    runtime_hooks: usize,
}

/// Ensure global database invariants are restored before proceeding.
///
/// This function performs a **system recovery step**:
/// - It completes or rolls back any previously started commit.
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

    if RECOVERED.get().is_none() {
        // Schema compatibility must be checked before row replay/rebuild can
        // decode stored rows with the generated runtime layout.
        ensure_schema_reconciled(db)?;
        perform_recovery(db)?;
        mark_schema_reconciliation_dirty(db);

        return ensure_schema_reconciled(db);
    }

    if !commit_marker_may_be_present() {
        return ensure_schema_reconciled(db);
    }

    if commit_marker_present_fast().map_err(|err| err.with_origin(ErrorOrigin::Recovery))? {
        // A marker-triggered recovery may rebuild indexes from existing rows,
        // so fail schema drift before any row decode path runs.
        ensure_schema_reconciled(db)?;
        perform_recovery(db)?;
        mark_schema_reconciliation_dirty(db);

        return ensure_schema_reconciled(db);
    }
    mark_commit_marker_verified_absent();

    ensure_schema_reconciled(db)
}

fn perform_recovery<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    let marker = with_commit_store(|store| store.load())
        .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    let had_marker = marker.is_some();
    if let Some(marker) = marker {
        publish_marker_bound_journal_batches(db, &marker)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
        // Phase 1: replay persisted row operations while marker authority is active.
        replay_commit_marker_row_ops(db, &marker.row_ops)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    }

    // Phase 2: fold committed journal-tail records into the canonical stable
    // base, then use the fold watermark as the replay boundary.
    if let Err(err) = fold_journaled_tails(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 3: rebuild journaled live projections from durable base + any
    // committed tail that remains above the fold watermark.
    if let Err(err) = rebuild_journaled_live_projections(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 4: rebuild secondary indexes from authoritative data rows.
    if let Err(err) = rebuild_secondary_indexes_from_rows(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 5: fold rebuilt journaled index materializations into canonical
    // index storage. Indexes are derived state, not independent journal truth.
    if let Err(err) = fold_journaled_index_materialized_views(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 6: enforce post-recovery integrity before clearing marker authority.
    if let Err(err) = validate_recovery_integrity(db) {
        return Err(err.with_origin(ErrorOrigin::Recovery));
    }

    // Phase 7: clear marker only after replay + rebuild + integrity validation succeed.
    if had_marker {
        with_commit_store(super::store::CommitStore::clear_verified)
            .map_err(|err| err.with_origin(ErrorOrigin::Recovery))?;
    }

    // Phase 8: authoritative rebuild succeeded, so every registered index is
    // query-visible again.
    db.mark_all_registered_index_stores_ready();
    mark_commit_marker_verified_absent();

    let _ = RECOVERED.set(());

    Ok(())
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
                Ok(JournalTailVisit::Continue)
            })
        })?;
    }

    Ok(())
}

fn fold_journaled_tails<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    for (store_path, handle) in sorted_journaled_store_handles(db) {
        let journal_store = handle
            .journal_tail_store()
            .ok_or_else(InternalError::store_corruption)?;
        let watermark = journal_store.with_borrow(JournalTailStore::fold_watermark)?;
        let mut highest_folded = watermark.highest_folded_journal_sequence();

        journal_store.with_borrow(|store| {
            store.visit_batches_after(watermark.highest_folded_journal_sequence(), |batch| {
                fold_journal_batch(db, store_path, handle, batch)?;
                highest_folded = batch.journal_sequence();
                Ok(JournalTailVisit::Continue)
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
    validate_journal_batch_records(db, expected_store_path, expected_handle, batch)?;

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
    validate_journal_batch_records(db, expected_store_path, expected_handle, batch)?;

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
    match record {
        JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            schema_fingerprint,
        } => {
            validate_journal_row_record(
                db,
                expected_store_path,
                expected_handle,
                entity_path,
                primary_key,
                schema_fingerprint,
            )?;
            let row =
                RawRow::from_untrusted_bytes(row_bytes.clone()).map_err(InternalError::from)?;
            expected_handle.with_data_mut(|store| {
                store
                    .apply_recovered_journal_put(primary_key.clone(), row)
                    .map(|_| ())
            })
        }
        JournalRecord::RowDelete {
            entity_path,
            primary_key,
            schema_fingerprint,
        } => {
            validate_journal_row_record(
                db,
                expected_store_path,
                expected_handle,
                entity_path,
                primary_key,
                schema_fingerprint,
            )?;
            expected_handle.with_data_mut(|store| {
                store
                    .apply_recovered_journal_delete(primary_key)
                    .map(|_| ())
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
            expected_handle.with_schema_mut(|schema_store| {
                schema_store.insert_persisted_snapshot(hooks.entity_tag, &snapshot)
            })
        }
    }
}

fn fold_journal_record<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    record: &JournalRecord,
) -> Result<(), InternalError> {
    match record {
        JournalRecord::RowPut {
            entity_path,
            primary_key,
            row_bytes,
            schema_fingerprint,
        } => {
            validate_journal_row_record(
                db,
                expected_store_path,
                expected_handle,
                entity_path,
                primary_key,
                schema_fingerprint,
            )?;
            let row =
                RawRow::from_untrusted_bytes(row_bytes.clone()).map_err(InternalError::from)?;
            expected_handle.with_data_mut(|store| {
                store
                    .fold_recovered_journal_put(primary_key.clone(), row)
                    .map(|_| ())
            })
        }
        JournalRecord::RowDelete {
            entity_path,
            primary_key,
            schema_fingerprint,
        } => {
            validate_journal_row_record(
                db,
                expected_store_path,
                expected_handle,
                entity_path,
                primary_key,
                schema_fingerprint,
            )?;
            expected_handle
                .with_data_mut(|store| store.fold_recovered_journal_delete(primary_key).map(|_| ()))
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
            expected_handle.with_schema_mut(|schema_store| {
                schema_store.fold_persisted_snapshot(hooks.entity_tag, &snapshot)
            })
        }
    }
}

fn validate_journal_batch_records<C: CanisterKind>(
    db: &Db<C>,
    expected_store_path: &'static str,
    expected_handle: StoreHandle,
    batch: &JournalBatch,
) -> Result<(), InternalError> {
    for record in batch.records() {
        match record {
            JournalRecord::RowPut {
                entity_path,
                primary_key,
                row_bytes,
                schema_fingerprint,
            } => {
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
                )?;
            }
            JournalRecord::RowDelete {
                entity_path,
                primary_key,
                schema_fingerprint,
            } => {
                validate_journal_row_record(
                    db,
                    expected_store_path,
                    expected_handle,
                    entity_path,
                    primary_key,
                    schema_fingerprint,
                )?;
                validate_journal_row_delete_preflight_if_needed(
                    db,
                    expected_handle,
                    entity_path,
                    primary_key,
                    *schema_fingerprint,
                )?;
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
            }
        }
    }

    Ok(())
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
    let accepted = match recovery_runtime_hook_for_entity_path(db, entity_path) {
        Ok(hooks) => {
            if hooks.store_path != expected_store_path
                || decoded_key.entity_tag() != hooks.entity_tag
            {
                return Err(InternalError::store_corruption());
            }
            expected_handle.with_schema_mut(|schema_store| {
                ensure_accepted_schema_snapshot(
                    schema_store,
                    hooks.entity_tag,
                    hooks.entity_path,
                    hooks.model,
                )
            })?
        }
        Err(err) => {
            if db.has_runtime_hooks() {
                return Err(err);
            }
            accepted_snapshot_for_no_hook_journal_row(
                expected_handle,
                decoded_key.entity_tag(),
                entity_path,
            )?
        }
    };
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
    if !db.has_runtime_hooks()
        || expected_handle.with_data(|store| {
            store
                .get(primary_key)
                .is_some_and(|row| row.as_bytes() == row_bytes)
        })
    {
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
    if !db.has_runtime_hooks() || !expected_handle.with_data(|store| store.contains(primary_key)) {
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
        JournalRecord::SchemaPut { store_path, .. } => {
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

// Typed fallback DBs do not carry generated runtime hooks. Resolve their journal
// row store identity from accepted schema snapshots already owned by the
// registered stores.
fn journal_row_record_store_handle<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &str,
    record: &JournalRecord,
) -> Result<(&'static str, StoreHandle), InternalError> {
    if let Ok(hooks) = recovery_runtime_hook_for_entity_path(db, entity_path) {
        return registry_store_handle_for_path(db, hooks.store_path);
    }
    if db.has_runtime_hooks() {
        return Err(InternalError::store_corruption());
    }

    let primary_key = match record {
        JournalRecord::RowPut { primary_key, .. }
        | JournalRecord::RowDelete { primary_key, .. } => primary_key,
        JournalRecord::SchemaPut { .. } => return Err(InternalError::store_corruption()),
    };
    let decoded_key = DecodedDataStoreKey::try_from_raw(primary_key)
        .map_err(|_| InternalError::store_corruption())?;

    resolve_no_hook_journaled_store_for_entity(db, decoded_key.entity_tag(), entity_path)
}

fn resolve_no_hook_journaled_store_for_entity<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
    entity_path: &str,
) -> Result<(&'static str, StoreHandle), InternalError> {
    let mut resolved = None::<(&'static str, StoreHandle)>;
    db.with_store_registry(|registry| {
        for (path, handle) in registry.iter() {
            if handle.storage_capabilities().recovery()
                != StoreRecoveryCapability::StableBasePlusJournalReplay
            {
                continue;
            }
            let Some(snapshot) = handle.with_schema_mut(|schema_store| {
                schema_store.latest_persisted_snapshot(entity_tag)
            })?
            else {
                continue;
            };
            if snapshot.entity_path() != entity_path {
                return Err(InternalError::store_corruption());
            }
            if resolved.replace((path, handle)).is_some() {
                return Err(InternalError::store_corruption());
            }
        }

        Ok::<(), InternalError>(())
    })?;

    resolved.ok_or_else(InternalError::store_corruption)
}

fn accepted_snapshot_for_no_hook_journal_row(
    expected_handle: StoreHandle,
    entity_tag: EntityTag,
    entity_path: &str,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let snapshot = expected_handle
        .with_schema_mut(|schema_store| schema_store.latest_persisted_snapshot(entity_tag))?
        .ok_or_else(InternalError::store_corruption)?;
    if snapshot.entity_path() != entity_path {
        return Err(InternalError::store_corruption());
    }

    AcceptedSchemaSnapshot::try_new(snapshot)
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
    if !db.has_runtime_hooks() {
        return Ok(());
    }

    let key = schema_reconciliation_key(db);
    if schema_reconciliation_clean(key) {
        return Ok(());
    }

    reconcile_runtime_schemas(db, db.entity_runtime_hooks)
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

fn mark_schema_reconciliation_dirty<C: CanisterKind>(db: &Db<C>) {
    let key = schema_reconciliation_key(db);
    SCHEMA_RECONCILED_KEYS.with(|keys| {
        keys.borrow_mut().retain(|existing| *existing != key);
    });
}
// Fail closed if recovery leaves any index/data divergence findings.
fn validate_recovery_integrity<C: CanisterKind>(db: &Db<C>) -> Result<(), InternalError> {
    if !db.has_runtime_hooks() {
        return Ok(());
    }

    let report = integrity_report_after_recovery(db)?;
    let totals = report.totals();
    if totals.missing_index_entries() > 0
        || totals.divergent_index_entries() > 0
        || totals.orphan_index_references() > 0
    {
        return Err(InternalError::recovery_integrity_validation_failed(
            totals.missing_index_entries(),
            totals.divergent_index_entries(),
            totals.orphan_index_references(),
        ));
    }

    Ok(())
}
