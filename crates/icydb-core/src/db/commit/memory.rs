//! Module: db::commit::memory
//! Responsibility: resolve and validate the commit-marker stable-memory slot.
//! Does not own: marker encoding, marker persistence, or recovery orchestration.
//! Boundary: commit::{recovery,store} -> commit::memory (one-way).

#[cfg(not(test))]
use crate::db::registry::open_committed_memory;
use crate::error::InternalError;
use ic_memory::RuntimeMemory;
use ic_stable_structures::DefaultMemoryImpl;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

thread_local! {
    static CURRENT_COMMIT_STORE_ALLOCATION: Cell<Option<CommitMemoryAllocation>> =
        const { Cell::new(None) };
    static COMMIT_MEMORIES: RefCell<
        Vec<(CommitMemoryAllocation, Rc<RuntimeMemory<DefaultMemoryImpl>>)>
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

/// Share the sole physical lease for this database's raw control regions.
///
/// Control-frame IO has no cached tree metadata. Schema namespace views reload
/// their tree on each operation; no independent long-lived tree may use this lease.
pub(in crate::db) fn commit_memory_handle(
    allocation: CommitMemoryAllocation,
) -> Result<Rc<RuntimeMemory<DefaultMemoryImpl>>, InternalError> {
    COMMIT_MEMORIES.with(|memories| {
        let mut memories = memories
            .try_borrow_mut()
            .map_err(|_| InternalError::store_invariant())?;
        if let Some((_, memory)) = memories
            .iter()
            .find(|(existing, _)| *existing == allocation)
        {
            return Ok(Rc::clone(memory));
        }
        #[cfg(test)]
        let memory = crate::testing::test_memory(allocation.memory_id);
        #[cfg(not(test))]
        let memory = open_committed_memory(allocation.stable_key, allocation.memory_id)?;
        let memory = Rc::new(memory);
        memories.push((allocation, Rc::clone(&memory)));
        Ok(memory)
    })
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
