//! Module: db::commit::rebuild
//! Responsibility: rebuild secondary indexes from authoritative persisted rows.
//! Does not own: marker-bound journal publication, commit-marker persistence, or query planning.
//! Boundary: commit::recovery -> commit::rebuild -> commit::{prepare,apply} (one-way).

#[cfg(test)]
use crate::db::commit::failpoint::{CommitFailpoint, hit_commit_failpoint};
use crate::{
    db::{
        Db,
        commit::CommitRowOp,
        data::{DataStore, DecodedDataStoreKey, StoreVisit},
        index::IndexStore,
        registry::{StoreHandle, StoreRecoveryCapability},
        schema::{accepted_commit_schema_fingerprint, ensure_accepted_schema_snapshot},
    },
    error::InternalError,
    traits::CanisterKind,
};

/// Rebuild all secondary indexes from authoritative data rows.
///
/// Invariant: row stores are the source of truth; index stores are fully
/// derived and can be recreated exactly from persisted rows.
pub(in crate::db) fn rebuild_secondary_indexes_from_rows(
    db: &Db<impl CanisterKind>,
) -> Result<(), InternalError> {
    // Derived indexes have one recovery direction: clear and rebuild from the
    // accepted schema plus authoritative rows. Failure leaves the store
    // non-Ready so guarded retry starts forward from another complete clear.
    let stores = sorted_store_handles(db);
    match rebuild_secondary_indexes_in_place(db, &stores) {
        Ok(()) => Ok(()),
        Err(error) => {
            // Discard any prefix derived before rejection. This is not
            // rollback to a before-image: the stores remain Building and the
            // next guarded recovery attempt starts forward from empty state.
            for (_, handle) in &stores {
                handle.with_index_mut(IndexStore::clear);
            }
            Err(error)
        }
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
    #[cfg(test)]
    hit_commit_failpoint(CommitFailpoint::AfterSecondaryIndexRebuildClear)?;

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
                    hooks.store_path,
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
