//! Module: db::commit::memory
//! Responsibility: resolve and validate the commit-marker stable-memory slot.
//! Does not own: marker encoding, marker persistence, or recovery orchestration.
//! Boundary: commit::{recovery,store} -> commit::memory (one-way).

#[cfg(not(test))]
use crate::db::registry::open_committed_memory;
use crate::{error::InternalError, traits::CanisterKind};
use ic_memory::{DefaultMemoryImpl, RuntimeMemory};
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
    pub(in crate::db) memory_id: u8,
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
    memory_id: u8,
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

/// Select a raw fixture allocation without a generated readiness owner.
#[cfg(test)]
pub(in crate::db) fn select_commit_memory_allocation(memory_id: u8, stable_key: &'static str) {
    CURRENT_COMMIT_STORE.with(|cell| {
        cell.set(Some(CommitMemorySelection {
            allocation: CommitMemoryAllocation {
                memory_id,
                stable_key,
            },
            readiness_record_key: || Ok(None),
        }));
    });
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
mod tests {
    use super::*;
    use ic_memory::Memory as _;

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

    #[test]
    fn readiness_owner_changes_do_not_duplicate_the_control_lease() {
        struct Generated<const SOURCE: u8>;
        impl<const SOURCE: u8> crate::traits::Path for Generated<SOURCE> {
            const PATH: &'static str = "test::Generated";
        }
        impl<const SOURCE: u8> CanisterKind for Generated<SOURCE> {
            const COMMIT_STABLE_KEY: &'static str = "icydb.readiness.commit.v1";
            const STARTUP_STABLE_KEY: &'static str = "icydb.readiness.startup.v1";
            const INTEGRITY_PROGRESS_STABLE_KEY: &'static str = "icydb.readiness.integrity.v1";

            fn generated_schema_application_record_key() -> Result<Option<[u8; 32]>, InternalError>
            {
                Ok(Some([SOURCE; 32]))
            }
        }

        let allocation = CommitMemoryAllocation {
            memory_id: 94,
            stable_key: "icydb.readiness.commit.v1",
        };
        select_commit_memory_allocation_for::<Generated<1>>(94, allocation.stable_key);
        let lease = commit_memory_handle(allocation).unwrap();
        assert_eq!(
            current_schema_application_readiness_key().unwrap(),
            Some([1; 32])
        );
        select_commit_memory_allocation_for::<Generated<2>>(94, allocation.stable_key);
        assert_eq!(
            current_schema_application_readiness_key().unwrap(),
            Some([2; 32])
        );
        assert!(Rc::ptr_eq(
            &lease,
            &commit_memory_handle(allocation).unwrap()
        ));

        select_commit_memory_allocation(95, "icydb.readiness.other-control.v1");
        assert_eq!(current_schema_application_readiness_key().unwrap(), None);
        select_commit_memory_allocation_for::<Generated<1>>(94, allocation.stable_key);
        assert_eq!(
            current_schema_application_readiness_key().unwrap(),
            Some([1; 32])
        );
        assert!(Rc::ptr_eq(
            &lease,
            &commit_memory_handle(allocation).unwrap()
        ));
    }

    #[test]
    fn selected_control_borrow_keeps_the_live_lease_and_refuses_another_key() {
        const KEY: &str = "icydb.test.borrowed.commit.control.v1";
        select_commit_memory_allocation(93, KEY);
        let owner = commit_memory_handle(current_commit_memory_allocation().unwrap()).unwrap();
        owner.as_ref().grow(1).unwrap();
        owner.write(0, b"retained");
        let bytes = with_selected_commit_memory(KEY, |memory| {
            let mut bytes = [0; 8];
            memory.read(0, &mut bytes);
            bytes
        })
        .unwrap();
        assert_eq!(&bytes, b"retained");

        let called = Cell::new(false);
        assert!(
            with_selected_commit_memory("icydb.test.other.commit.control.v1", |_| {
                called.set(true);
            })
            .is_err()
        );
        assert!(!called.get());
        let mut retained = [0; 8];
        owner.read(0, &mut retained);
        assert_eq!(&retained, b"retained");
    }
}
