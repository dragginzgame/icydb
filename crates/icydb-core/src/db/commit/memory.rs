//! Commit store memory allocation helpers.

use crate::{db::commit::marker::COMMIT_LABEL, error::InternalError};
use canic_memory::{
    registry::{MemoryRange, MemoryRegistry, MemoryRegistryEntry},
    runtime::registry::MemoryRegistryRuntime,
};
use std::{collections::BTreeSet, sync::OnceLock};

static COMMIT_STORE_ID: OnceLock<u8> = OnceLock::new();

/// Resolve or allocate the memory id used for commit marker storage.
pub(super) fn commit_memory_id() -> Result<u8, InternalError> {
    if let Some(id) = COMMIT_STORE_ID.get() {
        return Ok(*id);
    }

    MemoryRegistryRuntime::init(None).map_err(|err| {
        InternalError::store_internal(format!("memory registry init failed: {err}"))
    })?;

    let (owner, range, used_ids) = select_commit_range()?;
    let id = allocate_commit_id(range, &used_ids)?;
    MemoryRegistry::register(id, &owner, COMMIT_LABEL).map_err(|err| {
        InternalError::store_internal(format!("commit memory id registration failed: {err}"))
    })?;

    let _ = COMMIT_STORE_ID.set(id);
    Ok(id)
}

// Locate the registry range reserved for data and index stores.
fn select_commit_range() -> Result<(String, MemoryRange, BTreeSet<u8>), InternalError> {
    let snapshots = MemoryRegistryRuntime::snapshot_ids_by_range();
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
            return Ok((snapshot.owner, snapshot.range, used_ids));
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

// Identify registry entries that belong to DB stores.
fn is_db_store_entry(entry: &MemoryRegistryEntry) -> bool {
    entry.label.ends_with("DataStore") || entry.label.ends_with("IndexStore")
}
