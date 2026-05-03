//! Module: diagnostics::integrity
//! Responsibility: read-only data/index integrity scan reports.
//! Does not own: commit replay, index rebuild mutation, or storage report DTO shape.
//! Boundary: recovery and diagnostics entrypoints consume this scan to verify invariants.

use crate::{
    db::{
        Db,
        commit::CommitRowOp,
        data::{DataKey, StorageKey, decode_structural_row_payload},
        diagnostics::{IntegrityReport, IntegrityStoreSnapshot, IntegrityTotals},
        index::IndexKey,
        registry::StoreHandle,
        schema::commit_schema_fingerprint_for_model,
    },
    error::{ErrorClass, InternalError},
    traits::CanisterKind,
    types::EntityTag,
};
use std::collections::{BTreeMap, BTreeSet};

#[cfg_attr(
    doc,
    doc = "Build one deterministic integrity scan over all registered stores.\n\nThis scan is read-only and classifies findings as:\n- corruption: malformed persisted bytes, incompatible persisted formats, or inconsistent structural links\n- misuse: unsupported runtime wiring (for example missing entity hooks)"
)]
pub(crate) fn integrity_report<C: CanisterKind>(
    db: &Db<C>,
) -> Result<IntegrityReport, InternalError> {
    db.ensure_recovered_state()?;

    integrity_report_after_recovery(db)
}

#[cfg_attr(
    doc,
    doc = "Build one deterministic integrity scan after recovery has already completed.\n\nCallers running inside recovery flow should use this variant to avoid recursive recovery gating."
)]
pub(in crate::db) fn integrity_report_after_recovery<C: CanisterKind>(
    db: &Db<C>,
) -> Result<IntegrityReport, InternalError> {
    build_integrity_report(db)
}

fn build_integrity_report<C: CanisterKind>(db: &Db<C>) -> Result<IntegrityReport, InternalError> {
    let mut stores = Vec::new();
    let mut totals = IntegrityTotals::default();
    let global_live_keys_by_entity = collect_global_live_keys_by_entity(db)?;

    db.with_store_registry(|reg| {
        // Keep deterministic output order across registry traversal implementations.
        let mut store_entries = reg.iter().collect::<Vec<_>>();
        store_entries.sort_by_key(|(path, _)| *path);

        for (path, store_handle) in store_entries {
            let mut snapshot = IntegrityStoreSnapshot::new(path.to_string());
            scan_store_forward_integrity(db, store_handle, &mut snapshot)?;
            scan_store_reverse_integrity(store_handle, &global_live_keys_by_entity, &mut snapshot);

            totals.add_store_snapshot(&snapshot);
            stores.push(snapshot);
        }

        Ok::<(), InternalError>(())
    })?;

    Ok(IntegrityReport::new(stores, totals))
}

// Build one global map of live data keys grouped by entity across all stores.
fn collect_global_live_keys_by_entity<C: CanisterKind>(
    db: &Db<C>,
) -> Result<BTreeMap<EntityTag, BTreeSet<StorageKey>>, InternalError> {
    let mut keys = BTreeMap::<EntityTag, BTreeSet<StorageKey>>::new();

    db.with_store_registry(|reg| {
        for (_, store_handle) in reg.iter() {
            store_handle.with_data(|data_store| {
                for entry in data_store.entries() {
                    if let Ok(data_key) = DataKey::try_from_raw(entry.key()) {
                        keys.entry(data_key.entity_tag())
                            .or_default()
                            .insert(data_key.storage_key());
                    }
                }
            });
        }

        Ok::<(), InternalError>(())
    })?;

    Ok(keys)
}

// Run forward (data -> index) integrity checks for one store.
fn scan_store_forward_integrity<C: CanisterKind>(
    db: &Db<C>,
    store_handle: StoreHandle,
    snapshot: &mut IntegrityStoreSnapshot,
) -> Result<(), InternalError> {
    store_handle.with_data(|data_store| {
        for entry in data_store.entries() {
            snapshot.data_rows_scanned = snapshot.data_rows_scanned.saturating_add(1);

            let raw_key = *entry.key();

            let Ok(data_key) = DataKey::try_from_raw(&raw_key) else {
                snapshot.corrupted_data_keys = snapshot.corrupted_data_keys.saturating_add(1);
                continue;
            };

            let hooks = match db.runtime_hook_for_entity_tag(data_key.entity_tag()) {
                Ok(hooks) => hooks,
                Err(err) => {
                    classify_scan_error(err, snapshot)?;
                    continue;
                }
            };

            let marker_row = CommitRowOp::new(
                hooks.entity_path,
                raw_key,
                None,
                Some(entry.value().as_bytes().to_vec()),
                commit_schema_fingerprint_for_model(hooks.entity_path, hooks.model),
            );

            // Validate the outer row envelope before typed preparation so
            // hard-cut persisted-format mismatches count as corruption.
            if let Err(err) = decode_structural_row_payload(&entry.value()) {
                classify_scan_error(err, snapshot)?;
                continue;
            }

            let prepared = match db.prepare_row_commit_op(&marker_row) {
                Ok(prepared) => prepared,
                Err(err) => {
                    classify_scan_error(err, snapshot)?;
                    continue;
                }
            };

            for index_op in prepared.index_ops {
                let Some(expected_value) = index_op.value else {
                    continue;
                };

                let actual = index_op
                    .index_store
                    .with_borrow(|index_store| index_store.get(&index_op.key));
                match actual {
                    Some(actual_value) if actual_value == expected_value => {}
                    Some(_) => {
                        snapshot.divergent_index_entries =
                            snapshot.divergent_index_entries.saturating_add(1);
                    }
                    None => {
                        snapshot.missing_index_entries =
                            snapshot.missing_index_entries.saturating_add(1);
                    }
                }
            }
        }

        Ok::<(), InternalError>(())
    })
}

// Run reverse (index -> data) integrity checks for one store.
fn scan_store_reverse_integrity(
    store_handle: StoreHandle,
    live_keys_by_entity: &BTreeMap<EntityTag, BTreeSet<StorageKey>>,
    snapshot: &mut IntegrityStoreSnapshot,
) {
    store_handle.with_index(|index_store| {
        for (raw_index_key, raw_index_entry) in index_store.entries() {
            snapshot.index_entries_scanned = snapshot.index_entries_scanned.saturating_add(1);

            let Ok(decoded_index_key) = IndexKey::try_from_raw(&raw_index_key) else {
                snapshot.corrupted_index_keys = snapshot.corrupted_index_keys.saturating_add(1);
                continue;
            };

            let index_entity_tag = data_entity_tag_for_index_key(&decoded_index_key);

            let Ok(indexed_primary_keys) = raw_index_entry.decode_keys() else {
                snapshot.corrupted_index_entries =
                    snapshot.corrupted_index_entries.saturating_add(1);
                continue;
            };

            for primary_key in indexed_primary_keys {
                let exists = live_keys_by_entity
                    .get(&index_entity_tag)
                    .is_some_and(|entity_keys| entity_keys.contains(&primary_key));
                if !exists {
                    snapshot.orphan_index_references =
                        snapshot.orphan_index_references.saturating_add(1);
                }
            }
        }
    });
}

// Map scan-time errors into explicit integrity classification buckets.
fn classify_scan_error(
    err: InternalError,
    snapshot: &mut IntegrityStoreSnapshot,
) -> Result<(), InternalError> {
    match err.class() {
        ErrorClass::Corruption | ErrorClass::IncompatiblePersistedFormat => {
            snapshot.corrupted_data_rows = snapshot.corrupted_data_rows.saturating_add(1);
            Ok(())
        }
        ErrorClass::Unsupported | ErrorClass::NotFound | ErrorClass::Conflict => {
            snapshot.misuse_findings = snapshot.misuse_findings.saturating_add(1);
            Ok(())
        }
        ErrorClass::Internal | ErrorClass::InvariantViolation => Err(err),
    }
}

// Parse the data-entity identity from one decoded index key.
const fn data_entity_tag_for_index_key(index_key: &IndexKey) -> EntityTag {
    index_key.index_id().entity_tag()
}
