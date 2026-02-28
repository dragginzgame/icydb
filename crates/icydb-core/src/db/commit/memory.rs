//! Module: commit::memory
//! Responsibility: resolve and validate the commit-marker stable-memory slot.
//! Does not own: marker encoding, marker persistence, or recovery orchestration.
//! Boundary: commit::{recovery,store} -> commit::memory (one-way).

use crate::{db::commit::marker::COMMIT_LABEL, error::InternalError};
use canic_memory::{
    registry::{MemoryRangeEntry, MemoryRegistry},
    runtime::registry::MemoryRegistryRuntime,
};
use std::sync::OnceLock;

static COMMIT_STORE_ID: OnceLock<u8> = OnceLock::new();

/// Resolve the configured memory id used for commit marker storage.
pub(super) fn commit_memory_id() -> Result<u8, InternalError> {
    COMMIT_STORE_ID.get().copied().ok_or_else(|| {
        InternalError::store_internal(
            "commit memory id is not configured; initialize recovery before commit store access",
        )
    })
}

/// Configure and register the commit marker memory id.
pub(in crate::db::commit) fn configure_commit_memory_id(
    memory_id: u8,
) -> Result<u8, InternalError> {
    // Phase 1: enforce one immutable runtime slot id per process.
    if let Some(cached_id) = COMMIT_STORE_ID.get() {
        if *cached_id != memory_id {
            return Err(InternalError::store_internal(format!(
                "commit memory id mismatch: cached={cached_id}, configured={memory_id}"
            )));
        }

        return Ok(*cached_id);
    }

    // Phase 2: initialize registry runtime and validate slot ownership.
    MemoryRegistryRuntime::init(None).map_err(|err| {
        InternalError::store_internal(format!("memory registry init failed: {err}"))
    })?;

    validate_commit_slot_registration(memory_id)?;

    // Phase 3: cache the validated slot id for all future accesses.
    let _ = COMMIT_STORE_ID.set(memory_id);
    Ok(memory_id)
}

/// Validate that exactly one canonical commit-marker slot exists.
fn validate_commit_slot_registration(memory_id: u8) -> Result<(), InternalError> {
    let mut commit_ids = MemoryRegistryRuntime::snapshot_entries()
        .into_iter()
        .filter_map(|(id, entry)| (entry.label == COMMIT_LABEL).then_some(id))
        .collect::<Vec<_>>();
    commit_ids.sort_unstable();
    commit_ids.dedup();

    match commit_ids.as_slice() {
        [] => register_commit_slot(memory_id),
        [registered_id] if *registered_id == memory_id => Ok(()),
        [registered_id] => Err(InternalError::store_unsupported(format!(
            "configured commit memory id {memory_id} does not match existing commit marker id {registered_id}"
        ))),
        _ => Err(InternalError::store_corruption(format!(
            "multiple commit marker memory ids registered: {commit_ids:?}"
        ))),
    }
}

/// Register the configured commit-marker slot when no prior slot exists.
fn register_commit_slot(memory_id: u8) -> Result<(), InternalError> {
    if let Some(entry) = MemoryRegistryRuntime::get(memory_id) {
        return Err(InternalError::store_unsupported(format!(
            "configured commit memory id {memory_id} is already registered as '{}'",
            entry.label
        )));
    }

    let owner = owner_for_memory_id(memory_id)?;
    MemoryRegistry::register(memory_id, &owner, COMMIT_LABEL).map_err(|err| {
        InternalError::store_internal(format!("commit memory id registration failed: {err}"))
    })?;

    Ok(())
}

/// Resolve the canonical owner label for one configured memory id.
fn owner_for_memory_id(memory_id: u8) -> Result<String, InternalError> {
    owner_for_memory_id_in_ranges(memory_id, &MemoryRegistryRuntime::snapshot_range_entries())
}

/// Resolve owner label from registry range entries.
fn owner_for_memory_id_in_ranges(
    memory_id: u8,
    ranges: &[MemoryRangeEntry],
) -> Result<String, InternalError> {
    // Memory-range ownership is non-overlapping by registry contract, so the
    // first matching owner is the canonical owner for this memory id.
    let owner = ranges
        .iter()
        .find(|range_entry| range_entry.range.contains(memory_id))
        .map(|range_entry| range_entry.owner.clone());

    owner.ok_or_else(|| {
        InternalError::store_unsupported(format!(
            "configured commit memory id {memory_id} is outside reserved ranges",
        ))
    })
}

#[cfg(test)]
fn range_entry(owner: &str, start: u8, end: u8) -> MemoryRangeEntry {
    MemoryRangeEntry {
        owner: owner.to_string(),
        range: canic_memory::registry::MemoryRange { start, end },
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorClass, ErrorOrigin};

    #[test]
    fn owner_for_memory_id_returns_matching_owner() {
        let ranges = vec![range_entry("a", 1, 10), range_entry("b", 11, 20)];
        let owner = owner_for_memory_id_in_ranges(12, &ranges).expect("owner should resolve");
        assert_eq!(owner, "b");
    }

    #[test]
    fn owner_for_memory_id_rejects_out_of_range_id() {
        let ranges = vec![range_entry("a", 1, 10), range_entry("b", 11, 20)];
        let err =
            owner_for_memory_id_in_ranges(30, &ranges).expect_err("id outside ranges must fail");
        assert_eq!(err.class, ErrorClass::Unsupported);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }

    #[test]
    fn owner_for_memory_id_prefers_first_matching_range_owner() {
        let ranges = vec![range_entry("a", 1, 10), range_entry("b", 10, 20)];
        let owner =
            owner_for_memory_id_in_ranges(10, &ranges).expect("first matching owner should win");
        assert_eq!(owner, "a");
    }
}
