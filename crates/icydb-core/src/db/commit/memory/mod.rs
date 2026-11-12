//! Module: db::commit::memory
//! Responsibility: resolve and validate the commit-marker stable-memory slot.
//! Does not own: marker encoding, marker persistence, or recovery orchestration.
//! Boundary: commit::{recovery,store} -> commit::memory (one-way).

// The test memory selection stays inside memory acquisition so tests follow the same admission path.

#[cfg(not(test))]
use crate::db::registry::open_committed_memory;
use crate::{error::InternalError, traits::CanisterKind};
use ic_storage::{DefaultMemoryImpl, RuntimeMemory};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

thread_local! {
    static CURRENT_COMMIT_STORE: Cell<Option<CommitMemorySelection>> =
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
    pub(in crate::db) memory_id: u64,
    pub(in crate::db) stable_key: &'static str,
}

// Readiness is selected with the database, but is not part of physical lease
// identity: changing generated source must keep the same sole memory handle.
#[derive(Clone, Copy)]
struct CommitMemorySelection {
    allocation: CommitMemoryAllocation,
    readiness_record_key: fn() -> Result<Option<[u8; 32]>, InternalError>,
}

pub(in crate::db) fn current_commit_memory_allocation()
-> Result<CommitMemoryAllocation, InternalError> {
    CURRENT_COMMIT_STORE.with(|cell| {
        cell.get()
            .map(|selection| selection.allocation)
            .ok_or_else(InternalError::commit_memory_id_unconfigured)
    })
}

pub(in crate::db) fn current_commit_memory_allocation_if_configured()
-> Option<CommitMemoryAllocation> {
    CURRENT_COMMIT_STORE.with(|cell| cell.get().map(|selection| selection.allocation))
}

/// Select an already-resolved commit allocation for this database operation.
/// Production callers resolve the ID from committed authority first; actual
/// opens also verify the key/ID pair through that same authority. Selection
/// neither allocates memory nor maintains a second collision registry.
pub(in crate::db) fn select_commit_memory_allocation_for<C: CanisterKind>(
    memory_id: u64,
    stable_key: &'static str,
) {
    let allocation = CommitMemoryAllocation {
        memory_id,
        stable_key,
    };

    CURRENT_COMMIT_STORE.with(|cell| {
        cell.set(Some(CommitMemorySelection {
            allocation,
            readiness_record_key: C::generated_schema_application_record_key,
        }));
    });
}

/// Resolve only when retention needs a victim; ordinary database entry does not
/// compute a schema identity or open an application-record view.
pub(in crate::db) fn current_schema_application_readiness_key()
-> Result<Option<[u8; 32]>, InternalError> {
    CURRENT_COMMIT_STORE
        .with(Cell::get)
        .map_or(Ok(None), |selection| (selection.readiness_record_key)())
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

/// Borrow the selected database's existing control lease for internal frame IO.
///
/// The key must match the current checked commit selection. The synchronous
/// borrow cannot escape; callers must not create independent cached trees.
///
/// # Errors
/// Returns a store error for an absent or mismatched selection, or a failed lease.
///
/// # Panics
/// Propagates panics raised by the caller's callback.
#[doc(hidden)]
pub fn with_selected_commit_memory<R>(
    stable_key: &str,
    access: impl FnOnce(&RuntimeMemory<DefaultMemoryImpl>) -> R,
) -> Result<R, InternalError> {
    let allocation = current_commit_memory_allocation()?;
    if allocation.stable_key != stable_key {
        return Err(InternalError::store_internal());
    }
    let memory = commit_memory_handle(allocation)?;
    Ok(access(&memory))
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::select_commit_memory_allocation;
