//! Commit store memory allocation helpers.

use crate::{db::commit::marker::COMMIT_LABEL, error::InternalError};
use canic_memory::{
    registry::{MemoryRange, MemoryRangeSnapshot, MemoryRegistry, MemoryRegistryEntry},
    runtime::registry::MemoryRegistryRuntime,
};
use std::{collections::BTreeSet, sync::OnceLock};

static COMMIT_STORE_ID: OnceLock<u8> = OnceLock::new();
pub(in crate::db::commit) const REGISTRY_DATA_STORE_LABEL: &str = "::icydb::__macro::DataStore";
pub(in crate::db::commit) const REGISTRY_INDEX_STORE_LABEL: &str = "::icydb::__macro::IndexStore";

/// Resolve or allocate the memory id used for commit marker storage.
pub(super) fn commit_memory_id() -> Result<u8, InternalError> {
    if let Some(id) = COMMIT_STORE_ID.get() {
        return Ok(*id);
    }

    MemoryRegistryRuntime::init(None).map_err(|err| {
        InternalError::store_internal(format!("memory registry init failed: {err}"))
    })?;

    // Reuse an existing commit-marker slot when present so the commit store
    // location stays stable across process restarts and upgrades.
    let snapshots = MemoryRegistryRuntime::snapshot_ids_by_range();
    if let Some(id) = find_existing_commit_id(&snapshots)? {
        let _ = COMMIT_STORE_ID.set(id);
        return Ok(id);
    }

    let (owner, range, used_ids) = select_commit_range(&snapshots)?;
    let id = allocate_commit_id(range, &used_ids)?;
    MemoryRegistry::register(id, &owner, COMMIT_LABEL).map_err(|err| {
        InternalError::store_internal(format!("commit memory id registration failed: {err}"))
    })?;

    let _ = COMMIT_STORE_ID.set(id);
    Ok(id)
}

// Resolve an already-registered commit-marker memory id.
fn find_existing_commit_id(snapshots: &[MemoryRangeSnapshot]) -> Result<Option<u8>, InternalError> {
    let mut commit_ids = snapshots
        .iter()
        .flat_map(|snapshot| snapshot.entries.iter())
        .filter_map(|(id, entry)| (entry.label == COMMIT_LABEL).then_some(*id))
        .collect::<Vec<_>>();
    commit_ids.sort_unstable();
    commit_ids.dedup();

    match commit_ids.as_slice() {
        [] => Ok(None),
        [id] => Ok(Some(*id)),
        _ => Err(InternalError::store_corruption(format!(
            "multiple commit marker memory ids registered: {commit_ids:?}"
        ))),
    }
}

// Locate the registry range reserved for data and index stores.
fn select_commit_range(
    snapshots: &[MemoryRangeSnapshot],
) -> Result<(String, MemoryRange, BTreeSet<u8>), InternalError> {
    for snapshot in snapshots {
        if snapshot
            .entries
            .iter()
            .any(|(_, entry)| is_db_store_entry(entry))
        {
            let used_ids = snapshot
                .entries
                .iter()
                .map(|(id, _)| *id)
                .collect::<BTreeSet<_>>();
            return Ok((snapshot.owner.clone(), snapshot.range, used_ids));
        }
    }

    Err(InternalError::store_internal(
        "unable to locate reserved memory range for commit markers",
    ))
}

// Allocate a free memory id for the commit marker store.
fn allocate_commit_id(range: MemoryRange, used: &BTreeSet<u8>) -> Result<u8, InternalError> {
    for id in (range.start..=range.end).rev() {
        if !used.contains(&id) {
            return Ok(id);
        }
    }

    Err(InternalError::store_unsupported(format!(
        "no free memory ids available for commit markers in range {}-{}",
        range.start, range.end
    )))
}

// Identify registry entries that anchor the DB store memory range.
pub(in crate::db::commit) fn is_db_store_entry(entry: &MemoryRegistryEntry) -> bool {
    matches!(
        entry.label.as_str(),
        REGISTRY_DATA_STORE_LABEL | REGISTRY_INDEX_STORE_LABEL
    )
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use canic_memory::registry::MemoryRange;

    fn range_snapshot(
        owner: &str,
        start: u8,
        end: u8,
        entries: &[(u8, &str)],
    ) -> MemoryRangeSnapshot {
        let entries = entries
            .iter()
            .map(|(id, label)| {
                (
                    *id,
                    MemoryRegistryEntry {
                        crate_name: owner.to_string(),
                        label: (*label).to_string(),
                    },
                )
            })
            .collect::<Vec<_>>();

        MemoryRangeSnapshot {
            owner: owner.to_string(),
            range: MemoryRange { start, end },
            entries,
        }
    }

    #[test]
    fn find_existing_commit_id_returns_none_when_absent() {
        let snapshots = vec![range_snapshot("icydb", 1, 10, &[(2, "UserDataStore")])];
        let id = find_existing_commit_id(&snapshots).expect("scan should succeed");
        assert_eq!(id, None);
    }

    #[test]
    fn find_existing_commit_id_returns_single_registered_id() {
        let snapshots = vec![range_snapshot("icydb", 1, 10, &[(9, COMMIT_LABEL)])];
        let id = find_existing_commit_id(&snapshots).expect("scan should succeed");
        assert_eq!(id, Some(9));
    }

    #[test]
    fn find_existing_commit_id_rejects_duplicate_commit_label_entries() {
        let snapshots = vec![
            range_snapshot("icydb", 1, 10, &[(8, COMMIT_LABEL)]),
            range_snapshot("icydb", 11, 20, &[(19, COMMIT_LABEL)]),
        ];
        let err =
            find_existing_commit_id(&snapshots).expect_err("duplicate commit labels must fail");
        assert_eq!(err.class, crate::error::ErrorClass::Corruption);
        assert_eq!(err.origin, crate::error::ErrorOrigin::Store);
    }

    #[test]
    fn is_db_store_entry_requires_internal_anchor_labels() {
        let data = MemoryRegistryEntry {
            crate_name: "icydb_test".to_string(),
            label: REGISTRY_DATA_STORE_LABEL.to_string(),
        };
        let index = MemoryRegistryEntry {
            crate_name: "icydb_test".to_string(),
            label: REGISTRY_INDEX_STORE_LABEL.to_string(),
        };
        let user_named = MemoryRegistryEntry {
            crate_name: "icydb_test".to_string(),
            label: "my_app::UserDataStore".to_string(),
        };

        assert!(is_db_store_entry(&data));
        assert!(is_db_store_entry(&index));
        assert!(!is_db_store_entry(&user_named));
    }
}
