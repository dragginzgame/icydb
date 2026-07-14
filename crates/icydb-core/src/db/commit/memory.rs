//! Module: db::commit::memory
//! Responsibility: resolve and validate the commit-marker stable-memory slot.
//! Does not own: marker encoding, marker persistence, or recovery orchestration.
//! Boundary: commit::{recovery,store} -> commit::memory (one-way).

use crate::error::InternalError;
#[cfg(not(test))]
use ic_memory::open_default_memory_manager_memory;
use ic_stable_structures::{DefaultMemoryImpl, memory_manager::VirtualMemory};
#[cfg(test)]
use std::cell::RefCell;
use std::{
    cell::Cell,
    sync::{Mutex, OnceLock},
};

static COMMIT_STORE_ALLOCATIONS: OnceLock<Mutex<Vec<CommitMemoryAllocation>>> = OnceLock::new();

thread_local! {
    static CURRENT_COMMIT_STORE_ALLOCATION: Cell<Option<CommitMemoryAllocation>> =
        const { Cell::new(None) };
    #[cfg(test)]
    static TEST_COMMIT_MEMORIES: RefCell<
        Vec<(CommitMemoryAllocation, VirtualMemory<DefaultMemoryImpl>)>
    > = const { RefCell::new(Vec::new()) };
}

/// Runtime allocation identity for the commit-marker control slot.
///
/// This is process-global commit storage wiring, not marker payload metadata.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CommitMemoryAllocation {
    pub(in crate::db) memory_id: u8,
    pub(in crate::db) stable_key: &'static str,
}

pub(in crate::db) fn current_commit_memory_allocation()
-> Result<CommitMemoryAllocation, InternalError> {
    CURRENT_COMMIT_STORE_ALLOCATION.with(|cell| {
        cell.get()
            .ok_or_else(InternalError::commit_memory_id_unconfigured)
    })
}

/// Configure and register the commit marker memory id.
pub(in crate::db::commit) fn configure_commit_memory_id(
    memory_id: u8,
    stable_key: &'static str,
) -> Result<u8, InternalError> {
    let allocation = CommitMemoryAllocation {
        memory_id,
        stable_key,
    };

    register_commit_memory_allocation(allocation)?;
    CURRENT_COMMIT_STORE_ALLOCATION.with(|cell| cell.set(Some(allocation)));

    Ok(memory_id)
}

/// Open the configured commit-marker memory slot through the shared memory API.
#[cfg(test)]
pub(in crate::db) fn commit_memory_handle(
    allocation: CommitMemoryAllocation,
) -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    TEST_COMMIT_MEMORIES.with(|memories| {
        let mut memories = memories.borrow_mut();
        if let Some((_, memory)) = memories
            .iter()
            .find(|(existing, _)| *existing == allocation)
        {
            return Ok(memory.clone());
        }

        let memory = crate::testing::test_memory(allocation.memory_id);
        memories.push((allocation, memory.clone()));
        Ok(memory)
    })
}

/// Open the configured commit-marker memory slot through the shared memory API.
#[cfg(not(test))]
pub(in crate::db) fn commit_memory_handle(
    allocation: CommitMemoryAllocation,
) -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    open_default_memory_manager_memory(allocation.stable_key, allocation.memory_id)
        .map_err(InternalError::commit_memory_id_registration_failed)
}

fn commit_memory_allocations() -> &'static Mutex<Vec<CommitMemoryAllocation>> {
    COMMIT_STORE_ALLOCATIONS.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_commit_memory_allocation(
    allocation: CommitMemoryAllocation,
) -> Result<(), InternalError> {
    {
        let mut allocations = commit_memory_allocations()
            .lock()
            .map_err(|_| InternalError::store_invariant())?;
        if validate_commit_memory_allocation_compat(&allocations, allocation)?.is_none() {
            allocations.push(allocation);
        }
    }

    Ok(())
}

fn validate_commit_memory_allocation_compat(
    cached: &[CommitMemoryAllocation],
    allocation: CommitMemoryAllocation,
) -> Result<Option<CommitMemoryAllocation>, InternalError> {
    for cached in cached {
        if *cached == allocation {
            return Ok(Some(*cached));
        }
        if cached.memory_id == allocation.memory_id {
            return Err(InternalError::commit_memory_stable_key_mismatch(
                cached.stable_key,
                allocation.stable_key,
            ));
        }
        if cached.stable_key == allocation.stable_key {
            return Err(InternalError::commit_memory_id_mismatch(
                cached.memory_id,
                allocation.memory_id,
            ));
        }
    }

    Ok(None)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorClass, ErrorOrigin};

    #[test]
    fn cached_commit_memory_allocation_reuses_matching_slot() {
        let cached = CommitMemoryAllocation {
            memory_id: 12,
            stable_key: "icydb.test.commit.control.v1",
        };
        let allocation = CommitMemoryAllocation {
            memory_id: 12,
            stable_key: "icydb.test.commit.control.v1",
        };

        assert_eq!(
            validate_commit_memory_allocation_compat(&[cached], allocation)
                .expect("matching cache should pass"),
            Some(cached),
        );
    }

    #[test]
    fn cached_commit_memory_allocation_rejects_mismatched_slot() {
        let cached = CommitMemoryAllocation {
            memory_id: 12,
            stable_key: "icydb.test.commit.control.v1",
        };
        let allocation = CommitMemoryAllocation {
            memory_id: 30,
            stable_key: "icydb.test.commit.control.v1",
        };

        let err = validate_commit_memory_allocation_compat(&[cached], allocation)
            .expect_err("mismatched cache should fail");
        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }

    #[test]
    fn cached_commit_memory_allocation_accepts_independent_slot() {
        let cached = CommitMemoryAllocation {
            memory_id: 12,
            stable_key: "icydb.test.commit.control.v1",
        };
        let allocation = CommitMemoryAllocation {
            memory_id: 30,
            stable_key: "icydb.test.commit.peer-control.v1",
        };

        assert_eq!(
            validate_commit_memory_allocation_compat(&[cached], allocation)
                .expect("independent cache should pass"),
            None,
        );
    }
}
