//! Module: diagnostics::storage_report
//! Responsibility: read-only storage footprint snapshot collection.
//! Does not own: integrity recovery validation or diagnostics DTO shape.
//! Boundary: consumes recovered `Db` store registries and emits `StorageReport`.

use crate::{
    db::{
        Db, EntityRuntimeHooks,
        data::DataKey,
        diagnostics::{DataStoreSnapshot, EntitySnapshot, IndexStoreSnapshot, StorageReport},
        index::IndexKey,
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};
use std::collections::BTreeMap;

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

// Update one small per-store entity-stat accumulator without pulling ordered
// map machinery into the default snapshot path. Final output ordering is still
// enforced later on the emitted snapshot rows.
fn update_default_entity_stats(
    entity_stats: &mut Vec<(EntityTag, EntityStats)>,
    entity_tag: EntityTag,
    value_len: u64,
) {
    if let Some((_, stats)) = entity_stats
        .iter_mut()
        .find(|(existing_tag, _)| *existing_tag == entity_tag)
    {
        stats.update(value_len);
        return;
    }

    let mut stats = EntityStats::default();
    stats.update(value_len);
    entity_stats.push((entity_tag, stats));
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

// Resolve one default entity path label for storage snapshots without pulling
// alias/path remapping support into the caller.
fn storage_report_default_name_for_entity_tag<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
) -> String {
    db.runtime_hook_for_entity_tag(entity_tag).ok().map_or_else(
        || format!("#{}", entity_tag.value()),
        |hooks| hooks.entity_path.to_string(),
    )
}

///
/// StorageReportMode
///
/// Internal selection for the two storage-report labeling contracts.
/// The mode keeps default and explicit report entrypoints on one traversal
/// while preserving their historical per-store entity-stat accumulation order.
///

enum StorageReportMode<'a> {
    Default,
    Explicit {
        name_map: BTreeMap<&'static str, &'a str>,
        tag_name_map: BTreeMap<EntityTag, &'a str>,
    },
}

impl StorageReportMode<'_> {
    // Resolve the outward entity label for one tag under the active report mode.
    fn entity_label<C: CanisterKind>(&self, db: &Db<C>, entity_tag: EntityTag) -> String {
        match self {
            Self::Default => storage_report_default_name_for_entity_tag(db, entity_tag),
            Self::Explicit {
                name_map,
                tag_name_map,
            } => tag_name_map
                .get(&entity_tag)
                .copied()
                .map(str::to_string)
                .or_else(|| {
                    db.runtime_hook_for_entity_tag(entity_tag)
                        .ok()
                        .map(|hooks| storage_report_name_for_hook(name_map, hooks).to_string())
                })
                .unwrap_or_else(|| format!("#{}", entity_tag.value())),
        }
    }
}

///
/// EntityStatsByMode
///
/// Per-store entity-stat accumulator that preserves the previous collection
/// shape for each public storage-report entrypoint. Default reports keep a
/// small insertion-ordered vector; explicit reports keep the historical
/// `EntityTag`-ordered map before the final public snapshot sort.
///

enum EntityStatsByMode {
    Default(Vec<(EntityTag, EntityStats)>),
    Explicit(BTreeMap<EntityTag, EntityStats>),
}

impl EntityStatsByMode {
    fn new(mode: &StorageReportMode<'_>) -> Self {
        match mode {
            StorageReportMode::Default => Self::Default(Vec::new()),
            StorageReportMode::Explicit { .. } => Self::Explicit(BTreeMap::new()),
        }
    }

    // Accumulate one data-row contribution using the mode-specific backing
    // collection retained from the previous separate implementations.
    fn update(&mut self, entity_tag: EntityTag, value_len: u64) {
        match self {
            Self::Default(entity_stats) => {
                update_default_entity_stats(entity_stats, entity_tag, value_len);
            }
            Self::Explicit(entity_stats) => {
                entity_stats
                    .entry(entity_tag)
                    .or_default()
                    .update(value_len);
            }
        }
    }

    // Emit per-entity snapshots into the shared report output vector.
    fn push_snapshots<C: CanisterKind>(
        self,
        store_path: &str,
        db: &Db<C>,
        mode: &StorageReportMode<'_>,
        entity_storage: &mut Vec<EntitySnapshot>,
    ) {
        match self {
            Self::Default(entity_stats) => {
                for (entity_tag, stats) in entity_stats {
                    push_entity_snapshot(
                        entity_storage,
                        store_path.to_string(),
                        mode.entity_label(db, entity_tag),
                        stats.entries,
                        stats.memory_bytes,
                    );
                }
            }
            Self::Explicit(entity_stats) => {
                for (entity_tag, stats) in entity_stats {
                    push_entity_snapshot(
                        entity_storage,
                        store_path.to_string(),
                        mode.entity_label(db, entity_tag),
                        stats.entries,
                        stats.memory_bytes,
                    );
                }
            }
        }
    }
}

// Append one per-entity snapshot row after the caller has chosen its
// mode-specific iteration order and outward label.
fn push_entity_snapshot(
    entity_storage: &mut Vec<EntitySnapshot>,
    store: String,
    path: String,
    entries: u64,
    memory_bytes: u64,
) {
    entity_storage.push(EntitySnapshot::new(store, path, entries, memory_bytes));
}

#[cfg_attr(
    doc,
    doc = "Build one deterministic storage snapshot with default entity-path names.\n\nThis variant is used by generated snapshot endpoints that never pass alias remapping, so it keeps the snapshot root independent from optional alias-resolution machinery."
)]
pub(crate) fn storage_report_default<C: CanisterKind>(
    db: &Db<C>,
) -> Result<StorageReport, InternalError> {
    db.ensure_recovered_state()?;

    Ok(build_storage_report(db, &StorageReportMode::Default))
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

    Ok(build_storage_report(
        db,
        &StorageReportMode::Explicit {
            name_map,
            tag_name_map,
        },
    ))
}

fn build_storage_report<C: CanisterKind>(
    db: &Db<C>,
    mode: &StorageReportMode<'_>,
) -> StorageReport {
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
                let mut by_entity = EntityStatsByMode::new(mode);

                for entry in store.entries() {
                    let Ok(dk) = DataKey::try_from_raw(entry.key()) else {
                        corrupted_keys = corrupted_keys.saturating_add(1);
                        continue;
                    };

                    let value_len = entry.value().len() as u64;

                    by_entity.update(dk.entity_tag(), value_len);
                }

                by_entity.push_snapshots(path, db, mode, &mut entity_storage);
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
                    store.state(),
                ));
            });
        }
    });

    // Phase 3: enforce deterministic entity snapshot emission order.
    // This remains stable even if outer store traversal internals change.
    entity_storage
        .sort_by(|left, right| (left.store(), left.path()).cmp(&(right.store(), right.path())));

    StorageReport::new(
        data,
        index,
        entity_storage,
        corrupted_keys,
        corrupted_entries,
    )
}
