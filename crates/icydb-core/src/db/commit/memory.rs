//! Module: commit::memory
//! Responsibility: resolve and validate the commit-marker stable-memory slot.
//! Does not own: marker encoding, marker persistence, or recovery orchestration.
//! Boundary: commit::{recovery,store} -> commit::memory (one-way).

use crate::error::InternalError;
use canic_cdk::structures::{DefaultMemoryImpl, memory::VirtualMemory};
#[cfg(not(test))]
use ic_memory::runtime;
use std::sync::OnceLock;

static COMMIT_STORE_ALLOCATION: OnceLock<CommitMemoryAllocation> = OnceLock::new();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommitMemoryAllocation {
    memory_id: u8,
    stable_key: &'static str,
}

fn commit_memory_allocation() -> Result<CommitMemoryAllocation, InternalError> {
    COMMIT_STORE_ALLOCATION
        .get()
        .copied()
        .ok_or_else(InternalError::commit_memory_id_unconfigured)
}

/// Configure and register the commit marker memory id.
pub(in crate::db::commit) fn configure_commit_memory_id(
    memory_id: u8,
    stable_key: &'static str,
) -> Result<u8, InternalError> {
    // Phase 1: enforce one immutable runtime slot id per process.
    if let Some(cached_id) = validate_cached_commit_memory_allocation(memory_id, stable_key)? {
        return Ok(cached_id);
    }

    #[cfg(test)]
    {
        let _ = COMMIT_STORE_ALLOCATION.set(CommitMemoryAllocation {
            memory_id,
            stable_key,
        });
        Ok(memory_id)
    }

    #[cfg(not(test))]
    {
        let _ = COMMIT_STORE_ALLOCATION.set(CommitMemoryAllocation {
            memory_id,
            stable_key,
        });
        Ok(memory_id)
    }
}

/// Open the configured commit-marker memory slot through the shared memory API.
pub(super) fn commit_memory_handle() -> Result<VirtualMemory<DefaultMemoryImpl>, InternalError> {
    let allocation = commit_memory_allocation()?;

    #[cfg(test)]
    {
        Ok(crate::testing::test_memory(allocation.memory_id))
    }

    #[cfg(not(test))]
    {
        runtime::open_default_memory_manager_memory(allocation.stable_key, allocation.memory_id)
            .map_err(InternalError::commit_memory_id_registration_failed)
    }
}

fn validate_cached_commit_memory_allocation(
    memory_id: u8,
    stable_key: &'static str,
) -> Result<Option<u8>, InternalError> {
    validate_commit_memory_allocation_compat(
        COMMIT_STORE_ALLOCATION.get().copied(),
        memory_id,
        stable_key,
    )
}

fn validate_commit_memory_allocation_compat(
    cached: Option<CommitMemoryAllocation>,
    memory_id: u8,
    stable_key: &'static str,
) -> Result<Option<u8>, InternalError> {
    let Some(cached) = cached else {
        return Ok(None);
    };
    if cached.memory_id != memory_id {
        return Err(InternalError::commit_memory_id_mismatch(
            cached.memory_id,
            memory_id,
        ));
    }
    if cached.stable_key != stable_key {
        return Err(InternalError::commit_memory_stable_key_mismatch(
            cached.stable_key,
            stable_key,
        ));
    }

    Ok(Some(cached.memory_id))
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

        assert_eq!(
            validate_commit_memory_allocation_compat(
                Some(cached),
                12,
                "icydb.test.commit.control.v1"
            )
            .expect("matching cache should pass"),
            Some(12),
        );
    }

    #[test]
    fn cached_commit_memory_allocation_rejects_mismatched_slot() {
        let cached = CommitMemoryAllocation {
            memory_id: 12,
            stable_key: "icydb.test.commit.control.v1",
        };

        let err = validate_commit_memory_allocation_compat(
            Some(cached),
            30,
            "icydb.test.commit.control.v1",
        )
        .expect_err("mismatched cache should fail");
        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }
}
