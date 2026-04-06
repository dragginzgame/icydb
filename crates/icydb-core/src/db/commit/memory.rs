//! Module: commit::memory
//! Responsibility: resolve and validate the commit-marker stable-memory slot.
//! Does not own: marker encoding, marker persistence, or recovery orchestration.
//! Boundary: commit::{recovery,store} -> commit::memory (one-way).

use crate::{db::commit::marker::COMMIT_LABEL, error::InternalError};
use canic_cdk::structures::{DefaultMemoryImpl, memory::VirtualMemory};
use canic_memory::api::{MemoryApi, MemoryInspection};
use std::sync::OnceLock;

static COMMIT_STORE_ID: OnceLock<u8> = OnceLock::new();

/// Resolve the configured memory id used for commit marker storage.
pub(super) fn commit_memory_id() -> Result<u8, InternalError> {
    COMMIT_STORE_ID
        .get()
        .copied()
        .ok_or_else(InternalError::commit_memory_id_unconfigured)
}

/// Configure and register the commit marker memory id.
pub(in crate::db::commit) fn configure_commit_memory_id(
    memory_id: u8,
) -> Result<u8, InternalError> {
    // Phase 1: enforce one immutable runtime slot id per process.
    if let Some(cached_id) = COMMIT_STORE_ID.get() {
        if *cached_id != memory_id {
            return Err(InternalError::commit_memory_id_mismatch(
                *cached_id, memory_id,
            ));
        }

        return Ok(*cached_id);
    }

    // Phase 2: flush deferred registry state and validate slot ownership.
    MemoryApi::bootstrap_pending().map_err(InternalError::commit_memory_registry_init_failed)?;

    validate_commit_slot_registration(memory_id)?;

    // Phase 3: cache the validated slot id for all future accesses.
    let _ = COMMIT_STORE_ID.set(memory_id);
    Ok(memory_id)
}

/// Open the configured commit-marker memory slot through the shared memory API.
pub(super) fn commit_memory_handle() -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    let memory_id = commit_memory_id()?;
    let owner = owner_for_memory_id(memory_id)?;

    MemoryApi::register(memory_id, &owner, COMMIT_LABEL)
        .map_err(InternalError::commit_memory_id_registration_failed)
}

/// Validate that exactly one canonical commit-marker slot exists.
fn validate_commit_slot_registration(memory_id: u8) -> Result<(), InternalError> {
    let mut commit_ids = MemoryApi::registered()
        .into_iter()
        .filter_map(|entry| (entry.label == COMMIT_LABEL).then_some(entry.id))
        .collect::<Vec<_>>();
    commit_ids.sort_unstable();
    commit_ids.dedup();

    match commit_ids.as_slice() {
        [] => register_commit_slot(memory_id),
        [registered_id] if *registered_id == memory_id => Ok(()),
        [registered_id] => Err(InternalError::configured_commit_memory_id_mismatch(
            memory_id,
            *registered_id,
        )),
        _ => Err(InternalError::multiple_commit_memory_ids_registered(
            commit_ids,
        )),
    }
}

/// Register the configured commit-marker slot when no prior slot exists.
fn register_commit_slot(memory_id: u8) -> Result<(), InternalError> {
    let inspection = inspect_memory(memory_id)?;
    if let Some(label) = inspection.label.as_deref() {
        return Err(InternalError::commit_memory_id_already_registered(
            memory_id, label,
        ));
    }

    MemoryApi::register(memory_id, &inspection.owner, COMMIT_LABEL)
        .map_err(InternalError::commit_memory_id_registration_failed)?;

    Ok(())
}

/// Resolve the canonical owner label for one configured memory id.
fn owner_for_memory_id(memory_id: u8) -> Result<String, InternalError> {
    Ok(inspect_memory(memory_id)?.owner)
}

// Inspect one configured memory id through the public Canic memory API.
fn inspect_memory(memory_id: u8) -> Result<MemoryInspection, InternalError> {
    MemoryApi::inspect(memory_id)
        .ok_or_else(|| InternalError::commit_memory_id_outside_reserved_ranges(memory_id))
}

#[cfg(test)]
fn inspection(memory_id: u8, owner: &str, start: u8, end: u8) -> MemoryInspection {
    MemoryInspection {
        id: memory_id,
        owner: owner.to_string(),
        range: canic_memory::registry::MemoryRange { start, end },
        label: None,
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
    fn owner_for_memory_inspection_returns_matching_owner() {
        let inspection = inspection(12, "b", 11, 20);
        let owner = inspection.owner;
        assert_eq!(owner, "b");
    }

    #[test]
    fn inspect_memory_missing_id_maps_to_out_of_range_error() {
        let err = MemoryApi::inspect(30)
            .ok_or_else(|| InternalError::commit_memory_id_outside_reserved_ranges(30))
            .expect_err("id outside ranges must fail");
        assert_eq!(err.class, ErrorClass::Unsupported);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }
}
