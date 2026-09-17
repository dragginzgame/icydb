//! Module: db::commit::memory
//! Responsibility: resolve and validate the commit-marker stable-memory slot.
//! Does not own: marker encoding, marker persistence, or recovery orchestration.
//! Boundary: commit::{recovery,store} -> commit::memory (one-way).

use crate::error::InternalError;
use ic_memory::RuntimeMemory;
use ic_memory::ic_stable_structures::DefaultMemoryImpl;
#[cfg(not(test))]
use ic_memory::open_default_memory_manager_memory;
use std::cell::Cell;
#[cfg(test)]
use std::cell::RefCell;

thread_local! {
    static CURRENT_COMMIT_STORE_ALLOCATION: Cell<Option<CommitMemoryAllocation>> =
        const { Cell::new(None) };
    #[cfg(test)]
    static TEST_COMMIT_MEMORIES: RefCell<
        Vec<(CommitMemoryAllocation, RuntimeMemory<DefaultMemoryImpl>)>
    > = const { RefCell::new(Vec::new()) };
}

/// Runtime allocation identity for the commit-marker control slot.
///
/// This selects a database within the thread's committed memory runtime; it is
/// not an allocation registry or marker payload metadata.

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

#[cfg(test)]
pub(in crate::db) fn current_commit_memory_allocation_if_configured()
-> Option<CommitMemoryAllocation> {
    CURRENT_COMMIT_STORE_ALLOCATION.with(Cell::get)
}

/// Select an already-resolved commit allocation for this database operation.
/// Production callers resolve the ID from committed authority first; actual
/// opens also verify the key/ID pair through that same authority. Selection
/// neither allocates memory nor maintains a second collision registry.
pub(in crate::db) fn select_commit_memory_allocation(memory_id: u8, stable_key: &'static str) {
    let allocation = CommitMemoryAllocation {
        memory_id,
        stable_key,
    };

    CURRENT_COMMIT_STORE_ALLOCATION.with(|cell| cell.set(Some(allocation)));
}

/// Open the configured commit-marker memory slot through the shared memory API.
#[cfg(test)]
pub(in crate::db) fn commit_memory_handle(
    allocation: CommitMemoryAllocation,
) -> Result<RuntimeMemory<DefaultMemoryImpl>, InternalError> {
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
) -> Result<RuntimeMemory<DefaultMemoryImpl>, InternalError> {
    open_default_memory_manager_memory(allocation.stable_key, allocation.memory_id)
        .map_err(InternalError::commit_memory_id_registration_failed)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_commit_selection_can_switch_databases_and_return() {
        let first = CommitMemoryAllocation {
            memory_id: 12,
            stable_key: "icydb.test.commit.control.v1",
        };
        let second = CommitMemoryAllocation {
            memory_id: 30,
            stable_key: "icydb.test.commit.peer-control.v1",
        };

        for allocation in [first, first, second, first] {
            select_commit_memory_allocation(allocation.memory_id, allocation.stable_key);
            assert_eq!(current_commit_memory_allocation().unwrap(), allocation);
        }
    }
}
