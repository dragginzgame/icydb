//! Module: db::commit::rebuild
//! Responsibility: rebuild secondary indexes from authoritative persisted rows.
//! Does not own: commit-marker replay, commit-marker persistence, or query planning.
//! Boundary: commit::recovery -> commit::rebuild -> commit::{prepare,apply} (one-way).

use crate::{
    db::{
        Db,
        commit::CommitRowOp,
        data::{DataStore, DecodedDataStoreKey, StoreVisit},
        index::{IndexEntryValue, IndexState, IndexStore, IndexStoreVisit, RawIndexStoreKey},
        registry::{StoreHandle, StoreRecoveryCapability},
        schema::{accepted_commit_schema_fingerprint, ensure_accepted_schema_snapshot},
    },
    error::InternalError,
    traits::CanisterKind,
};
use std::convert::Infallible;

/// Rebuild all secondary indexes from authoritative data rows.
///
/// Invariant: row stores are the source of truth; index stores are fully
/// derived and can be recreated exactly from persisted rows.
pub(in crate::db) fn rebuild_secondary_indexes_from_rows(
    db: &Db<impl CanisterKind>,
) -> Result<(), InternalError> {
    if !db.has_runtime_hooks() {
        return Ok(());
    }

    // Phase 1: capture deterministic store ordering and rollback snapshots.
    let stores = sorted_store_handles(db);
    let snapshots = stores
        .iter()
        .map(|(_, handle)| IndexStoreSnapshot::capture(*handle))
        .collect::<Vec<_>>();

    // Phase 2: clear and rebuild all index entries from authoritative rows.
    let rebuild_result = rebuild_secondary_indexes_in_place(db, &stores);
    if let Err(err) = rebuild_result {
        // Phase 3: fail closed by restoring the exact pre-rebuild snapshot.
        for snapshot in snapshots {
            snapshot.restore();
        }
        return Err(err);
    }

    Ok(())
}

///
/// IndexStoreSnapshot
///
/// Rollback snapshot for one index store captured before recovery rebuild.
/// This protects fail-closed recovery semantics if any rebuild step fails.
///

#[derive(Clone)]
struct IndexStoreSnapshot {
    handle: StoreHandle,
    entries: Vec<(RawIndexStoreKey, IndexEntryValue)>,
    state: IndexState,
}

impl IndexStoreSnapshot {
    // Capture one index store's exact pre-rebuild contents and readiness state.
    fn capture(handle: StoreHandle) -> Self {
        Self {
            handle,
            entries: handle.with_index(|index_store| {
                let mut entries = Vec::new();
                let _: Result<(), Infallible> = index_store.visit_entries(|raw_key, raw_entry| {
                    entries.push((raw_key.clone(), raw_entry.clone()));
                    Ok(IndexStoreVisit::Continue)
                });
                entries
            }),
            state: handle.index_state(),
        }
    }

    // Restore one index store to the exact pre-rebuild snapshot.
    fn restore(self) {
        let data_generation = self.handle.with_data(DataStore::generation);
        self.handle.with_index_mut(|index_store| {
            index_store.clear();
            for (raw_key, raw_entry) in self.entries {
                index_store.insert(raw_key, raw_entry);
            }
            index_store.mark_prefix_cardinality_data_generation(data_generation);

            match self.state {
                IndexState::Building => index_store.mark_building(),
                IndexState::Ready => index_store.mark_ready(),
                IndexState::Dropping => index_store.mark_dropping(),
            }
        });
    }
}

/// Collect store handles in deterministic path order for stable rebuild behavior.
fn sorted_store_handles(db: &Db<impl CanisterKind>) -> Vec<(&'static str, StoreHandle)> {
    let mut stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    stores.retain(|(_, handle)| {
        matches!(
            handle.storage_capabilities().recovery(),
            StoreRecoveryCapability::StableBasePlusJournalReplay
        )
    });
    // StoreRegistry iteration is HashMap-backed and intentionally unordered.
    // Recovery semantics must remain deterministic, so sort explicitly by path.
    stores.sort_by_key(|(path, _)| *path);
    debug_assert!(
        stores.windows(2).all(|pair| pair[0].0 <= pair[1].0),
        "store registry iteration order must not affect semantic rebuild ordering",
    );

    stores
}

fn rebuild_secondary_indexes_in_place(
    db: &Db<impl CanisterKind>,
    stores: &[(&'static str, StoreHandle)],
) -> Result<(), InternalError> {
    // Phase 1: fail closed during rebuild so no query path can treat one
    // partially rebuilt secondary index as authoritative.
    for (_, handle) in stores {
        handle.mark_index_building();
    }

    // Phase 2: clear all index stores before deterministic full rebuild.
    for (_, handle) in stores {
        handle.with_index_mut(IndexStore::clear);
    }

    // Phase 3: rebuild index entries from authoritative row stores.
    for (_, handle) in stores {
        let rows = handle.with_data(|data_store| {
            let mut rows = Vec::new();
            let _: Result<(), InternalError> = data_store.visit_entries(|raw_key, raw_row| {
                rows.push((raw_key.clone(), raw_row.clone()));
                Ok(StoreVisit::Continue)
            });
            rows
        });

        for (raw_key, raw_row) in rows {
            let data_key = DecodedDataStoreKey::try_from_raw(&raw_key)
                .map_err(|_| InternalError::startup_index_rebuild_invalid_data_key())?;
            let hooks = db.runtime_hook_for_entity_tag(data_key.entity_tag())?;
            let accepted_schema = handle.with_schema_mut(|schema_store| {
                ensure_accepted_schema_snapshot(
                    schema_store,
                    hooks.entity_tag,
                    hooks.entity_path,
                    hooks.model,
                )
            })?;
            let schema_fingerprint = accepted_commit_schema_fingerprint(&accepted_schema)?;
            let row_op = CommitRowOp::new(
                hooks.entity_path,
                raw_key,
                None,
                Some(raw_row.as_bytes().to_vec()),
                schema_fingerprint,
            );
            let prepared = db.prepare_row_commit_op(&row_op)?;

            for index_op in prepared.index_ops {
                index_op.apply();
            }
        }

        let data_generation = handle.with_data(DataStore::generation);
        handle.with_index_mut(|index_store| {
            index_store.mark_prefix_cardinality_data_generation(data_generation);
        });
    }

    Ok(())
}
