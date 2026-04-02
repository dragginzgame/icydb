//! Module: diagnostics
//! Responsibility: read-only storage footprint and integrity snapshots.
//! Does not own: recovery, write-path mutation, or query planning semantics.
//! Boundary: consumes `Db`/store read APIs and returns DTO snapshots.

mod execution_trace;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        commit::CommitRowOp,
        data::{DataKey, StorageKey, decode_structural_row_cbor},
        index::IndexKey,
        registry::StoreHandle,
    },
    error::{ErrorClass, InternalError},
    traits::CanisterKind,
    types::EntityTag,
};
use candid::CandidType;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

pub use execution_trace::{
    ExecutionAccessPathVariant, ExecutionMetrics, ExecutionOptimization, ExecutionTrace,
};

#[cfg_attr(doc, doc = "StorageReport\n\nLive storage snapshot payload.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct StorageReport {
    pub(crate) storage_data: Vec<DataStoreSnapshot>,
    pub(crate) storage_index: Vec<IndexStoreSnapshot>,
    pub(crate) entity_storage: Vec<EntitySnapshot>,
    pub(crate) corrupted_keys: u64,
    pub(crate) corrupted_entries: u64,
}

#[cfg_attr(
    doc,
    doc = "IntegrityTotals\n\nAggregated integrity-scan counters across all stores."
)]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct IntegrityTotals {
    pub(crate) data_rows_scanned: u64,
    pub(crate) index_entries_scanned: u64,
    pub(crate) corrupted_data_keys: u64,
    pub(crate) corrupted_data_rows: u64,
    pub(crate) corrupted_index_keys: u64,
    pub(crate) corrupted_index_entries: u64,
    pub(crate) missing_index_entries: u64,
    pub(crate) divergent_index_entries: u64,
    pub(crate) orphan_index_references: u64,
    pub(crate) compatibility_findings: u64,
    pub(crate) misuse_findings: u64,
}

impl IntegrityTotals {
    const fn add_store_snapshot(&mut self, store: &IntegrityStoreSnapshot) {
        self.data_rows_scanned = self
            .data_rows_scanned
            .saturating_add(store.data_rows_scanned);
        self.index_entries_scanned = self
            .index_entries_scanned
            .saturating_add(store.index_entries_scanned);
        self.corrupted_data_keys = self
            .corrupted_data_keys
            .saturating_add(store.corrupted_data_keys);
        self.corrupted_data_rows = self
            .corrupted_data_rows
            .saturating_add(store.corrupted_data_rows);
        self.corrupted_index_keys = self
            .corrupted_index_keys
            .saturating_add(store.corrupted_index_keys);
        self.corrupted_index_entries = self
            .corrupted_index_entries
            .saturating_add(store.corrupted_index_entries);
        self.missing_index_entries = self
            .missing_index_entries
            .saturating_add(store.missing_index_entries);
        self.divergent_index_entries = self
            .divergent_index_entries
            .saturating_add(store.divergent_index_entries);
        self.orphan_index_references = self
            .orphan_index_references
            .saturating_add(store.orphan_index_references);
        self.compatibility_findings = self
            .compatibility_findings
            .saturating_add(store.compatibility_findings);
        self.misuse_findings = self.misuse_findings.saturating_add(store.misuse_findings);
    }

    /// Return total number of data rows scanned.
    #[must_use]
    pub const fn data_rows_scanned(&self) -> u64 {
        self.data_rows_scanned
    }

    /// Return total number of index entries scanned.
    #[must_use]
    pub const fn index_entries_scanned(&self) -> u64 {
        self.index_entries_scanned
    }

    /// Return total number of corrupted data-key findings.
    #[must_use]
    pub const fn corrupted_data_keys(&self) -> u64 {
        self.corrupted_data_keys
    }

    /// Return total number of corrupted data-row findings.
    #[must_use]
    pub const fn corrupted_data_rows(&self) -> u64 {
        self.corrupted_data_rows
    }

    /// Return total number of corrupted index-key findings.
    #[must_use]
    pub const fn corrupted_index_keys(&self) -> u64 {
        self.corrupted_index_keys
    }

    /// Return total number of corrupted index-entry findings.
    #[must_use]
    pub const fn corrupted_index_entries(&self) -> u64 {
        self.corrupted_index_entries
    }

    /// Return total number of missing index-entry findings.
    #[must_use]
    pub const fn missing_index_entries(&self) -> u64 {
        self.missing_index_entries
    }

    /// Return total number of divergent index-entry findings.
    #[must_use]
    pub const fn divergent_index_entries(&self) -> u64 {
        self.divergent_index_entries
    }

    /// Return total number of orphan index-reference findings.
    #[must_use]
    pub const fn orphan_index_references(&self) -> u64 {
        self.orphan_index_references
    }

    /// Return total number of compatibility findings.
    #[must_use]
    pub const fn compatibility_findings(&self) -> u64 {
        self.compatibility_findings
    }

    /// Return total number of misuse findings.
    #[must_use]
    pub const fn misuse_findings(&self) -> u64 {
        self.misuse_findings
    }
}

#[cfg_attr(
    doc,
    doc = "IntegrityStoreSnapshot\n\nPer-store integrity findings and scan counters."
)]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct IntegrityStoreSnapshot {
    pub(crate) path: String,
    pub(crate) data_rows_scanned: u64,
    pub(crate) index_entries_scanned: u64,
    pub(crate) corrupted_data_keys: u64,
    pub(crate) corrupted_data_rows: u64,
    pub(crate) corrupted_index_keys: u64,
    pub(crate) corrupted_index_entries: u64,
    pub(crate) missing_index_entries: u64,
    pub(crate) divergent_index_entries: u64,
    pub(crate) orphan_index_references: u64,
    pub(crate) compatibility_findings: u64,
    pub(crate) misuse_findings: u64,
}

impl IntegrityStoreSnapshot {
    /// Construct one empty store-level integrity snapshot.
    #[must_use]
    pub fn new(path: String) -> Self {
        Self {
            path,
            ..Self::default()
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return number of scanned data rows.
    #[must_use]
    pub const fn data_rows_scanned(&self) -> u64 {
        self.data_rows_scanned
    }

    /// Return number of scanned index entries.
    #[must_use]
    pub const fn index_entries_scanned(&self) -> u64 {
        self.index_entries_scanned
    }

    /// Return number of corrupted data-key findings.
    #[must_use]
    pub const fn corrupted_data_keys(&self) -> u64 {
        self.corrupted_data_keys
    }

    /// Return number of corrupted data-row findings.
    #[must_use]
    pub const fn corrupted_data_rows(&self) -> u64 {
        self.corrupted_data_rows
    }

    /// Return number of corrupted index-key findings.
    #[must_use]
    pub const fn corrupted_index_keys(&self) -> u64 {
        self.corrupted_index_keys
    }

    /// Return number of corrupted index-entry findings.
    #[must_use]
    pub const fn corrupted_index_entries(&self) -> u64 {
        self.corrupted_index_entries
    }

    /// Return number of missing index-entry findings.
    #[must_use]
    pub const fn missing_index_entries(&self) -> u64 {
        self.missing_index_entries
    }

    /// Return number of divergent index-entry findings.
    #[must_use]
    pub const fn divergent_index_entries(&self) -> u64 {
        self.divergent_index_entries
    }

    /// Return number of orphan index-reference findings.
    #[must_use]
    pub const fn orphan_index_references(&self) -> u64 {
        self.orphan_index_references
    }

    /// Return number of compatibility findings.
    #[must_use]
    pub const fn compatibility_findings(&self) -> u64 {
        self.compatibility_findings
    }

    /// Return number of misuse findings.
    #[must_use]
    pub const fn misuse_findings(&self) -> u64 {
        self.misuse_findings
    }
}

#[cfg_attr(
    doc,
    doc = "IntegrityReport\n\nFull integrity-scan output across all registered stores."
)]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct IntegrityReport {
    pub(crate) stores: Vec<IntegrityStoreSnapshot>,
    pub(crate) totals: IntegrityTotals,
}

impl IntegrityReport {
    /// Construct one integrity report payload.
    #[must_use]
    pub const fn new(stores: Vec<IntegrityStoreSnapshot>, totals: IntegrityTotals) -> Self {
        Self { stores, totals }
    }

    /// Borrow per-store integrity snapshots.
    #[must_use]
    pub const fn stores(&self) -> &[IntegrityStoreSnapshot] {
        self.stores.as_slice()
    }

    /// Borrow aggregated integrity totals.
    #[must_use]
    pub const fn totals(&self) -> &IntegrityTotals {
        &self.totals
    }
}

impl StorageReport {
    /// Construct one storage report payload.
    #[must_use]
    pub const fn new(
        storage_data: Vec<DataStoreSnapshot>,
        storage_index: Vec<IndexStoreSnapshot>,
        entity_storage: Vec<EntitySnapshot>,
        corrupted_keys: u64,
        corrupted_entries: u64,
    ) -> Self {
        Self {
            storage_data,
            storage_index,
            entity_storage,
            corrupted_keys,
            corrupted_entries,
        }
    }

    /// Borrow data-store snapshots.
    #[must_use]
    pub const fn storage_data(&self) -> &[DataStoreSnapshot] {
        self.storage_data.as_slice()
    }

    /// Borrow index-store snapshots.
    #[must_use]
    pub const fn storage_index(&self) -> &[IndexStoreSnapshot] {
        self.storage_index.as_slice()
    }

    /// Borrow entity-level storage snapshots.
    #[must_use]
    pub const fn entity_storage(&self) -> &[EntitySnapshot] {
        self.entity_storage.as_slice()
    }

    /// Return count of corrupted decoded data keys.
    #[must_use]
    pub const fn corrupted_keys(&self) -> u64 {
        self.corrupted_keys
    }

    /// Return count of corrupted index entries.
    #[must_use]
    pub const fn corrupted_entries(&self) -> u64 {
        self.corrupted_entries
    }
}

#[cfg_attr(doc, doc = "DataStoreSnapshot\n\nData-store snapshot row.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct DataStoreSnapshot {
    pub(crate) path: String,
    pub(crate) entries: u64,
    pub(crate) memory_bytes: u64,
}

impl DataStoreSnapshot {
    /// Construct one data-store snapshot row.
    #[must_use]
    pub const fn new(path: String, entries: u64, memory_bytes: u64) -> Self {
        Self {
            path,
            entries,
            memory_bytes,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return row count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
}

#[cfg_attr(doc, doc = "IndexStoreSnapshot\n\nIndex-store snapshot row.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct IndexStoreSnapshot {
    pub(crate) path: String,
    pub(crate) entries: u64,
    pub(crate) user_entries: u64,
    pub(crate) system_entries: u64,
    pub(crate) memory_bytes: u64,
}

impl IndexStoreSnapshot {
    /// Construct one index-store snapshot row.
    #[must_use]
    pub const fn new(
        path: String,
        entries: u64,
        user_entries: u64,
        system_entries: u64,
        memory_bytes: u64,
    ) -> Self {
        Self {
            path,
            entries,
            user_entries,
            system_entries,
            memory_bytes,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return total entry count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return user-namespace entry count.
    #[must_use]
    pub const fn user_entries(&self) -> u64 {
        self.user_entries
    }

    /// Return system-namespace entry count.
    #[must_use]
    pub const fn system_entries(&self) -> u64 {
        self.system_entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
}

#[cfg_attr(doc, doc = "EntitySnapshot\n\nPer-entity storage snapshot row.")]
#[derive(CandidType, Clone, Debug, Default, Deserialize)]
pub struct EntitySnapshot {
    pub(crate) store: String,

    pub(crate) path: String,

    pub(crate) entries: u64,

    pub(crate) memory_bytes: u64,
}

impl EntitySnapshot {
    /// Construct one entity-storage snapshot row.
    #[must_use]
    pub const fn new(store: String, path: String, entries: u64, memory_bytes: u64) -> Self {
        Self {
            store,
            path,
            entries,
            memory_bytes,
        }
    }

    /// Borrow store path.
    #[must_use]
    pub const fn store(&self) -> &str {
        self.store.as_str()
    }

    /// Borrow entity path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Return row count.
    #[must_use]
    pub const fn entries(&self) -> u64 {
        self.entries
    }

    /// Return memory usage in bytes.
    #[must_use]
    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
}

#[cfg_attr(
    doc,
    doc = "EntityStats\n\nInternal struct for building per-entity stats before snapshotting."
)]
#[derive(Default)]
struct EntityStats {
    entries: u64,
    memory_bytes: u64,
}

impl EntityStats {
    // Accumulate per-entity entry count and byte footprint for snapshot output.
    const fn update(&mut self, value_len: u64) {
        self.entries = self.entries.saturating_add(1);
        self.memory_bytes = self
            .memory_bytes
            .saturating_add(DataKey::entry_size_bytes(value_len));
    }
}

fn storage_report_name_for_hook<'a, C: CanisterKind>(
    name_map: &BTreeMap<&'static str, &'a str>,
    hooks: &EntityRuntimeHooks<C>,
) -> &'a str {
    name_map
        .get(hooks.entity_path)
        .copied()
        .or_else(|| name_map.get(hooks.model.name()).copied())
        .unwrap_or(hooks.entity_path)
}

#[cfg_attr(
    doc,
    doc = "Build one deterministic storage snapshot with per-entity rollups.\n\nThis path is read-only and fail-closed on decode/validation errors by counting corrupted keys/entries instead of panicking."
)]
pub(crate) fn storage_report<C: CanisterKind>(
    db: &Db<C>,
    name_to_path: &[(&'static str, &'static str)],
) -> Result<StorageReport, InternalError> {
    db.ensure_recovered_state()?;
    // Build one optional alias map once, then resolve report names from the
    // runtime hook table so entity tags keep distinct path identity even when
    // multiple hooks intentionally share the same model name.
    let name_map: BTreeMap<&'static str, &str> = name_to_path.iter().copied().collect();
    let mut tag_name_map = BTreeMap::<EntityTag, &str>::new();
    for hooks in db.entity_runtime_hooks {
        tag_name_map
            .entry(hooks.entity_tag)
            .or_insert_with(|| storage_report_name_for_hook(&name_map, hooks));
    }
    let mut data = Vec::new();
    let mut index = Vec::new();
    let mut entity_storage: Vec<EntitySnapshot> = Vec::new();
    let mut corrupted_keys = 0u64;
    let mut corrupted_entries = 0u64;

    db.with_store_registry(|reg| {
        // Keep diagnostics snapshots deterministic by traversing stores in path order.
        let mut stores = reg.iter().collect::<Vec<_>>();
        stores.sort_by_key(|(path, _)| *path);

        for (path, store_handle) in stores {
            // Phase 1: collect data-store snapshots and per-entity stats.
            store_handle.with_data(|store| {
                data.push(DataStoreSnapshot::new(
                    path.to_string(),
                    store.len(),
                    store.memory_bytes(),
                ));

                // Track per-entity counts and byte footprint for snapshot output.
                let mut by_entity: BTreeMap<EntityTag, EntityStats> = BTreeMap::new();

                for entry in store.iter() {
                    let Ok(dk) = DataKey::try_from_raw(entry.key()) else {
                        corrupted_keys = corrupted_keys.saturating_add(1);
                        continue;
                    };

                    let value_len = entry.value().len() as u64;

                    by_entity
                        .entry(dk.entity_tag())
                        .or_default()
                        .update(value_len);
                }

                for (entity_tag, stats) in by_entity {
                    let path_name = tag_name_map
                        .get(&entity_tag)
                        .copied()
                        .map(str::to_string)
                        .or_else(|| {
                            db.runtime_hook_for_entity_tag(entity_tag)
                                .ok()
                                .map(|hooks| {
                                    storage_report_name_for_hook(&name_map, hooks).to_string()
                                })
                        })
                        .unwrap_or_else(|| format!("#{}", entity_tag.value()));
                    entity_storage.push(EntitySnapshot::new(
                        path.to_string(),
                        path_name,
                        stats.entries,
                        stats.memory_bytes,
                    ));
                }
            });

            // Phase 2: collect index-store snapshots and integrity counters.
            store_handle.with_index(|store| {
                let mut user_entries = 0u64;
                let mut system_entries = 0u64;

                for (key, value) in store.entries() {
                    let Ok(decoded_key) = IndexKey::try_from_raw(&key) else {
                        corrupted_entries = corrupted_entries.saturating_add(1);
                        continue;
                    };

                    if decoded_key.uses_system_namespace() {
                        system_entries = system_entries.saturating_add(1);
                    } else {
                        user_entries = user_entries.saturating_add(1);
                    }

                    if value.validate().is_err() {
                        corrupted_entries = corrupted_entries.saturating_add(1);
                    }
                }

                index.push(IndexStoreSnapshot::new(
                    path.to_string(),
                    store.len(),
                    user_entries,
                    system_entries,
                    store.memory_bytes(),
                ));
            });
        }
    });

    // Phase 3: enforce deterministic entity snapshot emission order.
    // This remains stable even if outer store traversal internals change.
    entity_storage
        .sort_by(|left, right| (left.store(), left.path()).cmp(&(right.store(), right.path())));

    Ok(StorageReport::new(
        data,
        index,
        entity_storage,
        corrupted_keys,
        corrupted_entries,
    ))
}

#[cfg_attr(
    doc,
    doc = "Build one deterministic integrity scan over all registered stores.\n\nThis scan is read-only and classifies findings as:\n- corruption: malformed persisted bytes or inconsistent structural links\n- compatibility: persisted payloads outside decode compatibility windows\n- misuse: unsupported runtime wiring (for example missing entity hooks)"
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
                for entry in data_store.iter() {
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
        for entry in data_store.iter() {
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
                crate::db::schema::commit_schema_fingerprint_for_model(
                    hooks.entity_path,
                    hooks.model,
                ),
            );

            // Validate envelope compatibility before typed preparation so
            // incompatible persisted formats remain compatibility-classified.
            if let Err(err) = decode_structural_row_cbor(&entry.value()) {
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
                    .store
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
        ErrorClass::Corruption => {
            snapshot.corrupted_data_rows = snapshot.corrupted_data_rows.saturating_add(1);
            Ok(())
        }
        ErrorClass::IncompatiblePersistedFormat => {
            snapshot.compatibility_findings = snapshot.compatibility_findings.saturating_add(1);
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
    index_key.index_id().entity_tag
}
